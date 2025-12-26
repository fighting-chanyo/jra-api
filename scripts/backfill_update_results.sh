#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-https://jra-ipat-scraper-212192951042.asia-northeast1.run.app}"
SLEEP_SEC="${SLEEP_SEC:-45}"
CONNECT_TIMEOUT="${CONNECT_TIMEOUT:-10}"
MAX_TIME="${MAX_TIME:-30}"

DATES=(
  2025-01-05
  2025-01-06
  2025-01-11
  2025-01-12
  2025-01-13
  2025-01-18
  2025-01-19
  2025-01-25
  2025-01-26
  2025-02-01
  2025-02-02
  2025-02-08
  2025-02-09
  2025-02-10
  2025-02-15
  2025-02-16
  2025-02-22
  2025-02-23
  2025-03-01
  2025-03-02
  2025-03-08
  2025-03-09
  2025-03-15
  2025-03-16
  2025-03-22
  2025-03-23
  2025-03-29
  2025-03-30
  2025-04-05
  2025-04-06
  2025-04-12
  2025-04-13
  2025-04-19
  2025-04-20
  2025-04-26
  2025-04-27
  2025-05-03
  2025-05-04
  2025-05-10
  2025-05-11
  2025-05-17
  2025-05-18
  2025-05-24
  2025-05-25
  2025-05-31
  2025-06-01
  2025-06-07
  2025-06-08
  2025-06-14
  2025-06-15
  2025-06-21
  2025-06-22
  2025-06-28
  2025-06-29
  2025-07-05
  2025-07-06
  2025-07-12
  2025-07-13
  2025-07-19
  2025-07-20
  2025-07-26
  2025-07-27
  2025-08-02
  2025-08-03
  2025-08-09
  2025-08-10
  2025-08-16
  2025-08-17
  2025-08-23
  2025-08-24
  2025-08-30
  2025-08-31
  2025-09-06
  2025-09-07
  2025-09-13
  2025-09-14
  2025-09-15
  2025-09-20
  2025-09-21
  2025-09-27
  2025-09-28
  2025-10-04
  2025-10-05
  2025-10-11
  2025-10-12
  2025-10-13
  2025-10-18
  2025-10-19
  2025-10-25
  2025-10-26
  2025-11-01
  2025-11-02
  2025-11-08
  2025-11-09
  2025-11-15
  2025-11-16
  2025-11-22
  2025-11-23
  2025-11-24
  2025-11-29
)

echo "BASE_URL=$BASE_URL"
echo "SLEEP_SEC=$SLEEP_SEC"
echo "DATES=${#DATES[@]}"

tmp_body_file=""
cleanup() {
  if [[ -n "$tmp_body_file" && -f "$tmp_body_file" ]]; then
    rm -f "$tmp_body_file"
  fi
}
trap cleanup EXIT

tmp_body_file="$(mktemp)"

for d in "${DATES[@]}"; do
  url="$BASE_URL/api/races/update-results?target_date=$d"
  echo
  echo "=== POST $url ==="

  http_code="$(
    curl -sS \
      --connect-timeout "$CONNECT_TIMEOUT" \
      --max-time "$MAX_TIME" \
      -o "$tmp_body_file" \
      -w "%{http_code}" \
      -X POST "$url" || true
  )"

  echo "HTTP $http_code"
  cat "$tmp_body_file" || true
  echo

  if [[ "$http_code" != "200" && "$http_code" != "202" ]]; then
    echo "WARN: non-2xx for date=$d (HTTP $http_code)" >&2
  fi

  sleep "$SLEEP_SEC"
done

echo "DONE"
