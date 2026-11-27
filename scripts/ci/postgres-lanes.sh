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
    return {'kind': 'compatibility', 'major': major, 'expected_major': major,
            'image': image, 'plugins': plugins}


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
supported = {str(major) for major in range(15, 20)}
if engine_major not in supported:
    refuse('engine major is outside the supported range')
required = {'major', 'image', 'plugins'}
for lane in lanes:
    if not isinstance(lane, dict) or set(lane) != required:
        refuse('unknown or missing lane keys')
    major = lane['major']
    if not isinstance(major, str) or major not in supported:
        refuse('invalid major')
    if major in seen:
        refuse('duplicate major or engine declared as compatibility')
    seen.add(major)
    validate_image(major, lane['image'])
    if lane['plugins'] != ['pglogical']:
        refuse('compatibility lanes must include pglogical')
if seen != supported:
    refuse('one lane is required for each supported major')

matrix = [engine] + [upstream_cell(lane['major'], lane['image'], lane['plugins']) for lane in lanes]
print('matrix=' + json.dumps({'pg': matrix}, separators=(',', ':')))
print('engine-major=' + engine_major)
PY
