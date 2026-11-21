#!/usr/bin/env bash
set -euo pipefail
# The postgres composite action calls this after the anonymous digest gate.
cd "${GITHUB_WORKSPACE:?}"
case "${PG_MODE:?}" in start|stop) ;; *) exit 1 ;; esac
case "${PG_EXAMPLE:?}" in true|false) ;; *) exit 1 ;; esac
engine=$(cat .github/postgres.ref)
export REPLICANT_POSTGRES_IMAGE="$engine"
if [ "$PG_EXAMPLE" = true ]; then
  if [ "$PG_MODE" = start ]; then
    echo "REPLICANT_POSTGRES_IMAGE=$engine" >> "${GITHUB_ENV:?}"
    docker compose -f examples/replication_pipeline/docker-compose.yml up -d --build --wait
  else
    log_status=0
    if ! docker compose -f examples/replication_pipeline/docker-compose.yml logs; then log_status=1; fi
    docker compose -f examples/replication_pipeline/docker-compose.yml down -v --remove-orphans --rmi local
    exit "$log_status"
  fi
  exit 0
fi

# Names and ports are job-local, independent of server major.
if [ "$PG_MODE" = stop ]; then
  cleanup_status=0
  for name in pg pg-source; do
    if docker container inspect "$name" > /dev/null 2>&1; then
      if ! docker logs "$name"; then cleanup_status=1; fi
      if ! docker rm -f -v "$name"; then cleanup_status=1; fi
    fi
    if docker image inspect "replicant-$name-plugins:ci" > /dev/null 2>&1; then
      if ! docker image rm "replicant-$name-plugins:ci"; then cleanup_status=1; fi
    fi
  done
  exit "$cleanup_status"
fi

start_server() {
  local lane=$1 name=$2 port=$3 kind role image setting socket ready got expected auth
  kind=$(jq -er '.kind' <<< "$lane")
  role=$(jq -er '.role' <<< "$lane")
  expected=$(jq -er '.expected_major' <<< "$lane")
  local settings=(-c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10)
  if [ "$kind" = engine ]; then
    socket=/tmp
    docker run --detach --pull=never --name "$name" \
      --publish "127.0.0.1:$port:5432" \
      --tmpfs /var/lib/pg:uid=26,gid=26,mode=0750 --tmpfs /tmp \
      --env TEST_PG_USER --env TEST_PG_PASSWORD --env TEST_PG_DATABASE \
      --entrypoint baselabs-test-postgres "$engine" "${settings[@]}"
  else
    socket=/var/run/postgresql
    image=$(jq -er '.image' <<< "$lane")
    auth=$(jq -er '.auth' <<< "$lane")
    if [ "$role" = decoders ]; then
      docker build -f test/support/pg_old.dockerfile --build-arg "BASE=$image" \
        -t "replicant-$name-plugins:ci" .
      image="replicant-$name-plugins:ci"
      settings+=(-c shared_preload_libraries=pglogical)
    fi
    local declared_settings
    declared_settings=$(jq -r '.settings[]' <<< "$lane")
    while IFS= read -r setting; do
      if [ -n "$setting" ]; then settings+=(-c "$setting"); fi
    done <<< "$declared_settings"
    export POSTGRES_USER="$TEST_PG_USER" POSTGRES_PASSWORD="$TEST_PG_PASSWORD" POSTGRES_DB="$TEST_PG_DATABASE"
    export POSTGRES_HOST_AUTH_METHOD="$auth"
    export POSTGRES_INITDB_ARGS="--auth-host=$auth"
    docker run --detach --name "$name" --publish "127.0.0.1:$port:5432" \
      --env POSTGRES_USER --env POSTGRES_PASSWORD --env POSTGRES_DB --env POSTGRES_HOST_AUTH_METHOD \
      --env POSTGRES_INITDB_ARGS \
      "$image" "${settings[@]}"
  fi
  ready=false
  for ((attempt=0; attempt<60; attempt++)); do
    # Engine TCP answers only after init. Compatibility entrypoints restart:
    # require three accepts spanning at least two seconds.
    if docker exec "$name" pg_isready -h 127.0.0.1 -U "$TEST_PG_USER" -d "$TEST_PG_DATABASE" > /dev/null 2>&1; then
      if [ "$kind" = engine ] || {
        sleep 1 && docker exec "$name" pg_isready -h 127.0.0.1 -U "$TEST_PG_USER" -d "$TEST_PG_DATABASE" > /dev/null 2>&1 &&
        sleep 1 && docker exec "$name" pg_isready -h 127.0.0.1 -U "$TEST_PG_USER" -d "$TEST_PG_DATABASE" > /dev/null 2>&1;
      }; then
        ready=true
        break
      fi
    fi
    sleep 1
  done
  if [ "$ready" != true ]; then
    docker logs "$name"
    echo '::error::PostgreSQL did not become ready within 60 attempts'
    exit 1
  fi
  if [ "$role" = decoders ]; then
    docker exec --env "PG_AUTH=$auth" "$name" bash -euo pipefail -c \
      'printf "host replication all all %s\n" "$PG_AUTH" >> "$PGDATA/pg_hba.conf"'
    docker exec -i "$name" psql -h "$socket" -U "$TEST_PG_USER" -d "$TEST_PG_DATABASE" -v ON_ERROR_STOP=1 <<'SQL'
SELECT pg_reload_conf();
CREATE EXTENSION IF NOT EXISTS pglogical;
SELECT pglogical.create_node(node_name := 'ci_src', dsn := format('host=127.0.0.1 dbname=%I', current_database()));
SQL
  fi
  got=$(docker exec "$name" psql -h "$socket" -U "$TEST_PG_USER" -d "$TEST_PG_DATABASE" -tAc "SHOW server_version_num")
  echo "server_version_num=$got"
  test "$((got / 10000))" = "$expected"
}

start_server "${PG_LANE:?}" pg 15432
sidecar=$(jq -c '.sidecar // empty' <<< "$PG_LANE")
if [ -n "$sidecar" ]; then start_server "$sidecar" pg-source 15433; fi
