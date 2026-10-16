#!/usr/bin/env bash
# Remove test data that killed test runs left in the dev services.
#
#   scripts/clean-test-data.sh [--hours N] [--all] [--drop-bucket] [--dry-run]
#
# Tests name every scratch Postgres schema `t_<label>_<unix secs>_<random>` and
# every S3 prefix of the shared test bucket `<label>-t<unix secs>` (see
# `ant_store::testing`). Normally tests remove their own data (drop guards)
# and each test process sweeps stale leftovers when it starts; this script
# does the same by hand, with a shorter age if you like. It only touches
# names of that shape, never anything else.
#
#   --hours N      remove data older than N hours (default 6)
#   --all          same as --hours 0: everything, only when no test is running
#   --drop-bucket  also delete the whole S3 test bucket (ANT_TEST_S3_BUCKET,
#                  default ant-test): SeaweedFS returns disk space only for
#                  deleted buckets, never for deleted objects. The next test
#                  run recreates it. Only when no test is running.
#   --dry-run      list what would go
#
# Environment: ANT_TEST_PG_URL (default postgres://ant:ant@localhost:54329/ant),
# psql on PATH or in the dev services' PostgreSQL, weed from the dev services
# (scripts/dev-services.sh) for the S3 side.
set -euo pipefail

hours=6 drop_bucket=0 dry=0
while [ $# -gt 0 ]; do
    case "$1" in
        --hours) hours="${2:?--hours needs a number}"; shift ;;
        --all) hours=0 ;;
        --drop-bucket) drop_bucket=1 ;;
        --dry-run) dry=1 ;;
        -h | --help) sed -n '2,24p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

base="${ANT_SERVICES_DIR:-$HOME/.local/share/antimony-services}"
url="${ANT_TEST_PG_URL:-postgres://ant:ant@localhost:54329/ant}"
bucket="${ANT_TEST_S3_BUCKET:-ant-test}"
weed="$base/bin/weed"
psql_bin="$(command -v psql || true)"
[ -n "$psql_bin" ] || psql_bin="$(ls "$base"/postgresql-*/bin/psql 2>/dev/null | head -n1 || true)"
[ -n "$psql_bin" ] || { echo "psql not found" >&2; exit 1; }

now="$(date +%s)"
cutoff=$((now - ${hours%.*} * 3600))

# --- Postgres schemas -------------------------------------------------------
schemas="$("$psql_bin" "$url" -tAc "
  SELECT nspname FROM pg_namespace
  WHERE nspname ~ '^t_.+_[0-9]{10}_[0-9]+\$'
    AND substring(nspname from '_([0-9]{10})_[0-9]+\$')::bigint <= $cutoff
  ORDER BY nspname")"
n=0
for s in $schemas; do
    n=$((n + 1))
    if [ "$dry" = 1 ]; then echo "would drop schema $s"; else
        "$psql_bin" "$url" -qAc "DROP SCHEMA IF EXISTS \"$s\" CASCADE" >/dev/null
    fi
done
echo "postgres: $n test schema(s) $([ "$dry" = 1 ] && echo found || echo dropped)"

# --- S3 ----------------------------------------------------------------------
if [ ! -x "$weed" ]; then
    echo "s3: $weed not found, skipped"
    exit 0
fi
shell() { "$weed" shell -master="${ANT_WEED_MASTER:-127.0.0.1:19333}" 2>/dev/null; }
if [ "$drop_bucket" = 1 ]; then
    if [ "$dry" = 1 ]; then echo "would delete bucket $bucket"; else
        printf 's3.bucket.delete -name %s\n' "$bucket" | shell >/dev/null || true
        echo "s3: bucket $bucket deleted"
    fi
    exit 0
fi
# Objects are deleted by the tests' guards; what stays is empty directories
# (and the data of killed runs) under `<label>-t<secs>`.
prefixes="$(printf 'fs.ls /buckets/%s/\n' "$bucket" | shell \
    | tr -d '>' | tr ' ' '\n' | grep -E -- '-t[0-9]{10}$' || true)"
n=0
for p in $prefixes; do
    t="${p##*-t}"
    [ "$t" -le "$cutoff" ] || continue
    n=$((n + 1))
    if [ "$dry" = 1 ]; then echo "would remove s3 prefix $p"; else
        printf 'fs.rm -rf /buckets/%s/%s\n' "$bucket" "$p" | shell >/dev/null || true
    fi
done
echo "s3: $n test prefix(es) in $bucket $([ "$dry" = 1 ] && echo found || echo removed)"
