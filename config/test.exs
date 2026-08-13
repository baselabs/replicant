import Config

# Plan 2's integration + crash-injection suites run only when REPLICANT_TEST_URL
# points at a live PG16 with wal_level=logical (see AGENTS.md → Testing). Plan 1's
# unit + fixture-conformance suites need no server — the conformance test
# (test/replicant/decoder/conformance_test.exs) replays real captured pgoutput
# byte literals inlined from walex 4.8.0 (MIT; credited in NOTICE).

# Silence the one-time `:info` advisory the :telemetry app emits when a test attaches an
# anonymous-fn handler ("local function... performance penalty"). replicant itself emits NO
# Logger calls (the value-free boundary), so nothing useful is hidden at :warning in the test
# env; this just suppresses the cosmetic attach-time advisory (P2 #11).
config :logger, level: :warning
