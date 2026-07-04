import Config

# Plan 2's integration + crash-injection suites run only when REPLICANT_TEST_URL
# points at a live PG16 with wal_level=logical (see AGENTS.md → Testing). Plan 1's
# unit + fixture-conformance suites need no server — the conformance test replays
# committed real bytes captured once via test/support/fixture_capture.ex.
