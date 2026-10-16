#!/usr/bin/env bash
# Back up an antimony deployment: the Postgres schema and the S3 objects.
#
#   deploy/backup.sh            (cron, nightly; see README.md)
#
# Layout of $BACKUP_DIR (put it on a second disk, or a mount of remote storage):
#   pg/ant-<UTC stamp>.dump            pg_dump -Fc of the antimony schema, + .sha256
#   s3/                                mirror of the bucket (what the bucket holds now)
#   s3-deleted/<UTC stamp>/            objects that vanished from the bucket since the
#                                      previous run (packs removed by `ant admin gc`):
#                                      kept for the retention period, because an older
#                                      Postgres dump may still refer to them
#   config/                            policy.toml and .env (secrets: protect this dir)
#   last-success                       UTC stamp of the last complete run (monitor it)
#
# Order matters: Postgres first, S3 second. A restored database must never
# reference objects that are missing from the restored bucket, while extra
# objects in the bucket are harmless (docs/operator-guide.md section 7).
#
# Configuration (environment, or deploy/.env): BACKUP_DIR (required),
# BACKUP_RETENTION_DAYS (14), BACKUP_RCLONE_REMOTE (optional offsite copy of
# BACKUP_DIR), PG_SCHEMA (ant). Overrides for unusual setups and for testing
# against a host-installed Postgres/S3 instead of the compose stack:
#   PG_EXEC        prefix that runs a Postgres client tool
#                  (default: docker compose exec -T postgres; use "" for host tools)
#   PG_BIN         directory of pg_dump/pg_restore with trailing slash (default empty)
#   PG_USER/PG_DB  (ant / ant)
#   RCLONE_MODE    docker (default: rclone/rclone image on the compose network) | host
#   S3_ENDPOINT    endpoint as seen by rclone (default http://seaweedfs:8333)
set -euo pipefail
umask 077   # dumps and configuration copies are private to the backup user

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"
# shellcheck source=/dev/null
if [ -f .env ]; then set -a; . ./.env; set +a; fi

: "${BACKUP_DIR:?set BACKUP_DIR (see .env.example)}"
retention="${BACKUP_RETENTION_DAYS:-14}"
schema="${PG_SCHEMA:-ant}"
pg_user="${PG_USER:-ant}"
pg_db="${PG_DB:-ant}"
pg_exec="${PG_EXEC-docker compose exec -T postgres}"
pg_bin="${PG_BIN:-}"
s3_endpoint="${S3_ENDPOINT:-http://seaweedfs:8333}"
bucket="${S3_BUCKET:-antimony}"
rclone_mode="${RCLONE_MODE:-docker}"
rclone_image="${RCLONE_IMAGE:-rclone/rclone:1.68.2}"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
log() { printf '%s backup: %s\n' "$(date -u +%FT%TZ)" "$*"; }
die() { log "FAILED: $*"; exit 1; }

mkdir -p "$BACKUP_DIR"/{pg,s3,s3-deleted,config}
exec 9>"$BACKUP_DIR/.lock"
flock -n 9 || die "another backup is running"

# --- 1. Postgres ---------------------------------------------------------------
dump="$BACKUP_DIR/pg/ant-$stamp.dump"
log "pg_dump schema $schema -> $dump"
# shellcheck disable=SC2086 # PG_EXEC is a command prefix, split on purpose
if ! $pg_exec ${pg_bin}pg_dump -U "$pg_user" -d "$pg_db" --schema="$schema" -Fc --no-owner \
        > "$dump.partial"; then
    rm -f "$dump.partial"
    die "pg_dump"
fi
# The archive must at least be readable end to end.
# shellcheck disable=SC2086
$pg_exec ${pg_bin}pg_restore --list < "$dump.partial" > /dev/null || {
    rm -f "$dump.partial"; die "pg_restore --list rejected the dump"; }
mv "$dump.partial" "$dump"
(cd "$BACKUP_DIR/pg" && sha256sum "ant-$stamp.dump" > "ant-$stamp.dump.sha256")
log "dump $(du -h "$dump" | cut -f1)"

# --- 2. S3 objects (after Postgres) -------------------------------------------------
# `sync` mirrors the bucket; what it would delete locally (objects the server
# removed, e.g. garbage-collected packs) moves to s3-deleted/<stamp> instead.
log "s3 bucket $bucket -> $BACKUP_DIR/s3"
rclone_env=(
    RCLONE_CONFIG_SRC_TYPE=s3 RCLONE_CONFIG_SRC_PROVIDER=Other
    "RCLONE_CONFIG_SRC_ENDPOINT=$s3_endpoint"
    "RCLONE_CONFIG_SRC_ACCESS_KEY_ID=${S3_ACCESS_KEY:?S3_ACCESS_KEY}"
    "RCLONE_CONFIG_SRC_SECRET_ACCESS_KEY=${S3_SECRET_KEY:?S3_SECRET_KEY}"
    RCLONE_CONFIG_SRC_REGION=us-east-1 RCLONE_CONFIG_SRC_FORCE_PATH_STYLE=true
)
rclone_run() { # rclone arguments with the destination under /backup (docker) or $BACKUP_DIR (host)
    if [ "$rclone_mode" = host ]; then
        env "${rclone_env[@]}" rclone "$@"
    else
        local e=() v
        for v in "${rclone_env[@]}"; do e+=(-e "$v"); done
        docker run --rm --network "${COMPOSE_NETWORK:-antimony_default}" \
            -v "$BACKUP_DIR:/backup" "${e[@]}" "$rclone_image" "$@"
    fi
}
if [ "$rclone_mode" = host ]; then root="$BACKUP_DIR"; else root=/backup; fi
rclone_run sync "SRC:$bucket" "$root/s3" \
    --backup-dir "$root/s3-deleted/$stamp" \
    --transfers 16 --checkers 32 --fast-list --stats-one-line -q \
    || die "s3 sync"
# An empty "deleted" directory is noise.
rmdir "$BACKUP_DIR/s3-deleted/$stamp" 2>/dev/null || true

# --- 3. configuration ------------------------------------------------------------------
for f in policy.toml .env Caddyfile compose.yaml postgres.conf; do
    [ -f "$here/$f" ] && cp "$here/$f" "$BACKUP_DIR/config/$f"
done

# --- 4. retention -----------------------------------------------------------------------
# Dumps and the s3-deleted copies share one period on purpose: a dump is only
# restorable while the packs it refers to are still around.
find "$BACKUP_DIR/pg" -maxdepth 1 -name 'ant-*.dump*' -mtime "+$retention" -delete
find "$BACKUP_DIR/s3-deleted" -mindepth 1 -maxdepth 1 -type d -mtime "+$retention" \
    -exec rm -rf {} +

# --- 5. optional offsite copy ----------------------------------------------------------------
if [ -n "${BACKUP_RCLONE_REMOTE:-}" ]; then
    log "offsite: $BACKUP_DIR -> $BACKUP_RCLONE_REMOTE"
    rclone sync "$BACKUP_DIR" "$BACKUP_RCLONE_REMOTE" --exclude '.lock' --transfers 8 -q \
        || die "offsite sync"
fi

printf '%s\n' "$stamp" > "$BACKUP_DIR/last-success"
log "done $stamp"
