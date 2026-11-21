#!/usr/bin/env bash
set -euo pipefail
# This is the only reader of the compatibility declaration. Validate everything
# before publishing any matrix output; callers may supply paths for contract tests.
python3 - "${1:-.github/postgres-lanes.json}" "${2:-.github/postgres.ref}" >> "${GITHUB_OUTPUT:?}" <<'PY'
import json, re, sys
from pathlib import Path


def refuse(message):
    raise SystemExit('PostgreSQL lanes: ' + message)


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            refuse('duplicate object key')
        result[key] = value
    return result


reference = Path(sys.argv[2]).read_text()
match = re.fullmatch(r'ghcr\.io/baselabs/postgres:([0-9]+)\.[0-9]+-inputs-[0-9a-f]{64}@sha256:[0-9a-f]{64}\n', reference)
if not match:
    refuse('invalid one-line engine reference')
engine_major = match[1]
lanes = json.loads(Path(sys.argv[1]).read_text(), object_pairs_hook=unique_keys)
if not isinstance(lanes, list) or not lanes:
    refuse('expected a nonempty array')
seen = set()
vocabulary = {'pgoutput', 'decoders', 'legacy-source', 'parity-primary'}
for lane in lanes:
    if not isinstance(lane, dict) or set(lane) - {'major', 'image', 'roles', 'settings'}:
        refuse('unknown lane keys or invalid object')
    if not {'major', 'image', 'roles'} <= set(lane):
        refuse('missing required lane keys')
    major = lane['major']
    if not isinstance(major, str) or not re.fullmatch(r'9\.6|[1-9][0-9]+', major):
        refuse('invalid major')
    if major in seen or major == engine_major:
        refuse('duplicate major or engine declared as compatibility')
    seen.add(major)
    image = lane['image']
    if not isinstance(image, str) or not re.fullmatch(r'postgres:' + re.escape(major) + r'(?:[.-][a-zA-Z0-9_.-]+)?@sha256:[0-9a-f]{64}', image):
        refuse('image must match its major and pin a Docker Hub manifest digest')
    roles = lane['roles']
    if not isinstance(roles, list) or not roles or any(not isinstance(r, str) for r in roles):
        refuse('invalid roles')
    if len(roles) != len(set(roles)) or set(roles) - vocabulary:
        refuse('unknown or duplicate roles')
    if not set(roles) & {'pgoutput', 'decoders'}:
        refuse('lane has no test role')
    if ('legacy-source' in roles or 'parity-primary' in roles) and 'decoders' not in roles:
        refuse('decoder source roles require decoders')
    if 'legacy-source' in roles and int(major.split('.')[0]) >= 10:
        refuse('legacy source must exercise a pre-publication server')
    if ('pgoutput' in roles or 'parity-primary' in roles) and int(major.split('.')[0]) < 10:
        refuse('publication roles require a publication-capable server')
    if 'legacy-source' in roles and 'parity-primary' in roles:
        refuse('parity primary cannot be its legacy source')
    settings = lane.get('settings', [])
    if not isinstance(settings, list) or any(s != 'output_plugin_libraries=wal2json,pglogical_output,pgoutput' for s in settings):
        refuse('unknown or malformed decoder settings')
    if len(settings) > 1 or (settings and 'decoders' not in roles):
        refuse('duplicate settings or settings without decoders')
legacy = [lane for lane in lanes if 'legacy-source' in lane['roles']]
if len(legacy) != 1:
    refuse('exactly one legacy source is required')


def cell(lane, role):
    return {'kind': 'compatibility', 'major': lane['major'],
            'expected_major': lane['major'].split('.')[0], 'role': role,
            'image': lane['image'],
            'auth': 'md5' if int(lane['major'].split('.')[0]) < 14 else 'scram-sha-256',
            'settings': lane.get('settings', []) if role == 'decoders' else []}


matrix = [{'kind': 'engine', 'major': engine_major, 'expected_major': engine_major,
           'role': 'pgoutput', 'settings': []}]
for lane in lanes:
    for role in ('pgoutput', 'decoders'):
        if role in lane['roles']:
            row = cell(lane, role)
            row['legacy_self'] = 'legacy-source' in lane['roles']
            if role == 'decoders' and 'parity-primary' in lane['roles']:
                row['sidecar'] = cell(legacy[0], 'decoders')
            matrix.append(row)
print('matrix=' + json.dumps({'pg': matrix}, separators=(',', ':')))
print('engine-major=' + engine_major)
PY
