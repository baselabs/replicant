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


def validate_image(major, image):
    if not isinstance(image, str) or not re.fullmatch(r'postgres:' + re.escape(major) + r'(?:[.-][a-zA-Z0-9_.-]+)?@sha256:[0-9a-f]{64}', image):
        refuse('image must match its major and pin a Docker Hub manifest digest')


def upstream_cell(major, image, plugins):
    expected = major.split('.')[0]
    return {'kind': 'compatibility', 'major': major, 'expected_major': expected,
            'image': image, 'plugins': plugins,
            'auth': 'md5' if int(expected) < 14 else 'scram-sha-256'}


reference = Path(sys.argv[2]).read_text()
match = re.fullmatch(r'ghcr\.io/baselabs/postgres:([0-9]+)\.[0-9]+-inputs-[0-9a-f]{64}@sha256:[0-9a-f]{64}\n', reference)
if not match:
    refuse('invalid one-line engine reference')
engine_major = match[1]
declaration = json.loads(Path(sys.argv[1]).read_text(), object_pairs_hook=unique_keys)
if not isinstance(declaration, dict) or set(declaration) != {'engine_pglogical_image', 'compatibility'}:
    refuse('expected an engine pglogical image and compatibility lanes')

# The companion extends the engine job with a pglogical test run.
engine_pglogical_image = declaration['engine_pglogical_image']
validate_image(engine_major, engine_pglogical_image)
engine = {'kind': 'engine', 'major': engine_major, 'expected_major': engine_major,
          'plugins': [],
          'plugin_server': upstream_cell(engine_major, engine_pglogical_image, ['pglogical'])}

lanes = declaration['compatibility']
if not isinstance(lanes, list) or not lanes:
    refuse('expected a nonempty array')
seen = {engine_major}
flags = {'legacy-source', 'parity-primary'}
required = {'major', 'image', 'plugins'}
for lane in lanes:
    if not isinstance(lane, dict) or not required <= set(lane) <= required | flags:
        refuse('unknown or missing lane keys')
    major = lane['major']
    if not isinstance(major, str) or not re.fullmatch(r'9\.6|[1-9][0-9]+', major):
        refuse('invalid major')
    if major in seen:
        refuse('duplicate major or engine declared as compatibility')
    seen.add(major)
    validate_image(major, lane['image'])
    plugins = lane['plugins']
    if (not isinstance(plugins, list)
        or any(p not in ('pglogical', 'wal2json') for p in plugins)
        or len(plugins) != len(set(plugins))
        or ('wal2json' in plugins and 'pglogical' not in plugins)):
        refuse('invalid plugin list')
    roles = flags & lane.keys()
    if any(lane[role] is not True for role in roles):
        refuse('invalid decoder source role')
    if roles and set(plugins) != {'pglogical', 'wal2json'}:
        refuse('decoder source roles require both plugins')
    if roles == flags or ('legacy-source' in roles) != (major == '9.6'):
        refuse('legacy source must exercise a pre-publication server')
legacy = [lane for lane in lanes if lane.get('legacy-source')]
if len(legacy) != 1:
    refuse('exactly one legacy source is required')


def compatibility_cell(lane):
    return upstream_cell(lane['major'], lane['image'], lane['plugins'])


matrix = [engine]
for lane in lanes:
    row = compatibility_cell(lane)
    row['legacy_self'] = lane.get('legacy-source', False)
    if lane.get('parity-primary'):
        row['legacy_server'] = compatibility_cell(legacy[0])
    matrix.append(row)
print('matrix=' + json.dumps({'pg': matrix}, separators=(',', ':')))
print('engine-major=' + engine_major)
PY
