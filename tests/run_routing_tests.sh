#!/bin/sh
# Routing tests: connect to pgpooler and assert backend via backend_id table.
# Run: docker compose run --rm test
# Or from host: PGHOST=localhost PGPORT=6432 ./tests/run_routing_tests.sh

set -e
HOST="${PGHOST:-localhost}"
PORT="${PGPORT:-6432}"

run_test() {
  user="$1"
  database="$2"
  expected="$3"
  if [ "$user" = "reporting_reader" ]; then
    export PGPASSWORD="rreader"
  else
    export PGPASSWORD="postgres"
  fi
  result=$(psql -h "$HOST" -p "$PORT" -U "$user" -d "$database" -t -A -v ON_ERROR_STOP=1 -c "SELECT name FROM backend_id;" 2>&1) || true
  if [ "$result" = "$expected" ]; then
    echo "OK  $user / $database -> $expected"
    return 0
  else
    echo "FAIL $user / $database -> expected $expected, got: $result"
    return 1
  fi
}

echo "Routing tests (pgpooler at $HOST:$PORT)"
failed=0

run_test postgres main primary || failed=1
run_test postgres app primary || failed=1
run_test postgres postgres primary || failed=1
run_test postgres reporting replica || failed=1
run_test postgres analytics_test replica || failed=1
run_test postgres dwh_test replica || failed=1
run_test postgres default_test primary || failed=1
run_test reporting_reader reporting replica || failed=1

if [ $failed -eq 0 ]; then
  echo "All routing tests passed."
  exit 0
else
  echo "Some tests failed."
  exit 1
fi
