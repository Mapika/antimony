#!/bin/sh
# Local test services without Docker: PostgreSQL 17 and SeaweedFS (S3) as
# plain user processes, on the same ports as docker-compose.yml
# (Postgres 127.0.0.1:54329 ant/ant/ant, S3 127.0.0.1:9009 any credentials).
#
#   scripts/dev-services.sh install   # download binaries (once)
#   scripts/dev-services.sh start|stop|status
#   scripts/dev-services.sh wipe      # stop and delete all service data
#
# Everything lives in $ANT_SERVICES_DIR (default ~/.local/share/antimony-services).
set -eu

BASE="${ANT_SERVICES_DIR:-$HOME/.local/share/antimony-services}"
PG_VERSION=17.11.0
WEED_VERSION=3.97
PGBIN="$BASE/postgresql-$PG_VERSION-x86_64-unknown-linux-gnu/bin"
PGDATA="$BASE/pgdata"
S3DATA="$BASE/s3data"

pg_up() { "$PGBIN/pg_isready" -h 127.0.0.1 -p 54329 >/dev/null 2>&1; }
s3_up() { [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9009/ 2>/dev/null)" != "000" ]; }

install() {
    mkdir -p "$BASE/bin"
    tmp=$(mktemp -d)
    curl -sSfL -o "$tmp/weed.tgz" \
        "https://github.com/seaweedfs/seaweedfs/releases/download/$WEED_VERSION/linux_amd64.tar.gz"
    tar xzf "$tmp/weed.tgz" -C "$BASE/bin"
    curl -sSfL -o "$tmp/pg.tgz" \
        "https://github.com/theseus-rs/postgresql-binaries/releases/download/$PG_VERSION/postgresql-$PG_VERSION-x86_64-unknown-linux-gnu.tar.gz"
    tar xzf "$tmp/pg.tgz" -C "$BASE"
    rm -rf "$tmp"
}

start() {
    [ -x "$PGBIN/postgres" ] && [ -x "$BASE/bin/weed" ] || install
    if [ ! -d "$PGDATA" ]; then
        printf 'ant\n' > "$BASE/.pw"
        "$PGBIN/initdb" -D "$PGDATA" -U ant --pwfile="$BASE/.pw" -A scram-sha-256 >/dev/null
        rm -f "$BASE/.pw"
    fi
    if ! pg_up; then
        setsid nohup "$PGBIN/postgres" -D "$PGDATA" -p 54329 -k /tmp \
            -c listen_addresses=127.0.0.1 -c max_connections=500 -c shared_buffers=512MB \
            > "$BASE/postgres.log" 2>&1 < /dev/null &
        i=0; until pg_up; do i=$((i + 1)); [ $i -gt 60 ] && { echo "postgres did not start"; exit 1; }; sleep 0.5; done
        PGPASSWORD=ant "$PGBIN/psql" -h 127.0.0.1 -p 54329 -U ant -d postgres -tAc \
            "select 1 from pg_database where datname='ant'" | grep -q 1 \
            || PGPASSWORD=ant "$PGBIN/createdb" -h 127.0.0.1 -p 54329 -U ant ant
    fi
    if ! s3_up; then
        mkdir -p "$S3DATA"
        setsid nohup "$BASE/bin/weed" server -s3 -s3.port=9009 -dir="$S3DATA" \
            -ip=127.0.0.1 -ip.bind=127.0.0.1 -master.port=19333 -volume.port=18080 \
            -filer.port=18888 -volume.max=0 > "$BASE/weed.log" 2>&1 < /dev/null &
        i=0; until s3_up; do i=$((i + 1)); [ $i -gt 120 ] && { echo "seaweedfs did not start"; exit 1; }; sleep 0.5; done
    fi
    status
}

stop() {
    pg_up && "$PGBIN/pg_ctl" -D "$PGDATA" stop -m fast >/dev/null || true
    pkill -f "$BASE/bin/weed server" 2>/dev/null || true
    status
}

status() {
    pg_up && echo "postgres: up (127.0.0.1:54329)" || echo "postgres: down"
    s3_up && echo "s3:       up (127.0.0.1:9009)" || echo "s3:       down"
    du -sh "$PGDATA" "$S3DATA" 2>/dev/null || true
}

wipe() {
    stop
    rm -rf "$PGDATA" "$S3DATA"
    echo "service data deleted"
}

case "${1:-status}" in
    install) install ;;
    start) start ;;
    stop) stop ;;
    status) status ;;
    wipe) wipe ;;
    *) echo "usage: $0 install|start|stop|status|wipe" >&2; exit 2 ;;
esac
