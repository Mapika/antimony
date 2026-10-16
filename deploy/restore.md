# Restoring antimony from `backup.sh` backups

What `backup.sh` leaves in `$BACKUP_DIR`:

| path | content |
|---|---|
| `pg/ant-<stamp>.dump` (+ `.sha256`) | `pg_dump -Fc` of the `ant` schema: refs, commit graph, changesets, small objects, history index |
| `s3/` | copy of the bucket as of the last run (large objects and packs) |
| `s3-deleted/<stamp>/` | objects removed from the bucket between the previous run and `<stamp>` (packs deleted by `ant admin gc`) |
| `config/` | `policy.toml`, `.env`, `Caddyfile`, `compose.yaml`, `postgres.conf` of the last run |

Two rules from [the operator guide](../docs/operator-guide.md#7-backups-and-upgrades)
shape everything below:

1. A restored Postgres must not reference objects that are missing from the
   restored bucket. Extra objects in the bucket are harmless. `backup.sh`
   dumps Postgres *before* it copies S3, so the newest `s3/` always covers the
   newest dump.
2. An *older* dump may refer to packs that `ant admin gc` deleted later. Those
   are in `s3-deleted/<stamp>/` for every run after the dump, which is why
   restoring an older dump copies those directories back too (step 4).

Restoring rewinds refs to the dump's time. Agents whose workspace base was
created after the dump will find it missing: tell them to start new
workspaces (`ant ws new`). The derived history index rebuilds itself.

Everything below assumes the compose stack in `deploy/` and `$BACKUP_DIR`
from `.env`. Run it from `deploy/`.

## 1. Stop the writers

```sh
set -a; . ./.env; set +a
docker compose stop caddy ant          # postgres and seaweedfs stay up
docker compose up -d postgres seaweedfs   # (on a fresh host: this is all you start)
```

On a new host first: install Docker, `git clone` the repository for
`deploy/`, copy `$BACKUP_DIR/config/.env` and `policy.toml` into `deploy/`
(or restore them from the offsite copy), mount or copy `$BACKUP_DIR`.

## 2. Pick the dump and verify it

```sh
ls -1 "$BACKUP_DIR"/pg/*.dump | tail -5           # newest last
DUMP="$BACKUP_DIR/pg/ant-20261016T031500Z.dump"   # the one you want
( cd "$(dirname "$DUMP")" && sha256sum -c "$(basename "$DUMP").sha256" )
STAMP=$(basename "$DUMP" .dump); STAMP=${STAMP#ant-}
```

## 3. Postgres

```sh
docker compose exec -T postgres psql -U ant -d ant -c 'DROP SCHEMA IF EXISTS ant CASCADE'
docker compose exec -T postgres pg_restore -U ant -d ant --no-owner --exit-on-error < "$DUMP"
docker compose exec -T postgres psql -U ant -d ant -c '\dt ant.*' | head -20
```

`--exit-on-error` makes a bad archive fail loudly instead of leaving a
half-restored schema; if it fails, run the `DROP SCHEMA` again before retrying.

## 4. Objects

Copy the mirror back, then every `s3-deleted` directory newer than the dump
(for the newest dump there are none, or only ones that nothing refers to).
`rclone copy` never deletes, so this only adds objects.

```sh
rc() {  # rclone against the compose bucket; paths under /backup are $BACKUP_DIR
  docker run --rm --network antimony_default -v "$BACKUP_DIR:/backup:ro" \
    -e RCLONE_CONFIG_SRC_TYPE=s3 -e RCLONE_CONFIG_SRC_PROVIDER=Other \
    -e RCLONE_CONFIG_SRC_ENDPOINT=http://seaweedfs:8333 \
    -e RCLONE_CONFIG_SRC_ACCESS_KEY_ID="$S3_ACCESS_KEY" \
    -e RCLONE_CONFIG_SRC_SECRET_ACCESS_KEY="$S3_SECRET_KEY" \
    -e RCLONE_CONFIG_SRC_FORCE_PATH_STYLE=true \
    rclone/rclone:1.68.2 "$@"
}
rc copy /backup/s3 "SRC:${S3_BUCKET:-antimony}" --transfers 16 -q
for d in "$BACKUP_DIR"/s3-deleted/*/; do
  s=$(basename "$d")
  if [ "$s" \> "$STAMP" ]; then rc copy "/backup/s3-deleted/$s" "SRC:${S3_BUCKET:-antimony}" -q; fi
done
```

(`rclone` creates the bucket if the fresh SeaweedFS does not have it yet.)
Restoring an offsite copy instead: fetch it into a directory first
(`rclone sync $BACKUP_RCLONE_REMOTE /path`) and point `BACKUP_DIR` there.

## 5. Start and verify

```sh
docker compose up -d
docker compose ps                       # all healthy
curl -s https://$ANT_DOMAIN/readyz      # {"status":"ok","postgres":"ok","s3":"ok"}
export ANT_SERVER=https://$ANT_DOMAIN ANT_TOKEN=<admin token>
ant ref list
ant log main -n 5
ant cat main:<a path with a large file> | sha256sum     # a blob that lives in S3
docker compose run --rm --no-deps ant admin schema      # version / pending migrations
docker compose run --rm --no-deps ant admin gc --dry-run   # reads every reachable tree: must not report errors
```

The last two read all reachable data from both stores; run them for a first
restore drill and whenever you want certainty. A restored dump from an older
release is migrated by `ant serve` when it starts (additive, see
[docs/operations.md](../docs/operations.md#upgrading)).

## What was tested

The procedure above was run end to end on the development machine's Postgres
17 and SeaweedFS 3.97, with the same `pg_dump`/`pg_restore` and `rclone`
invocations that `backup.sh` and this document use, but executed with host
tools instead of `docker compose exec`/`docker run` (no Docker daemon was
available):

1. imported this repository's history (311 commits) into a throw-away schema
   and bucket and ran an `ant serve` on it;
2. fingerprinted the store through the server: `ant ref list`, `ant log main
   -n 5` and SHA-256 of three files, one of them a multi-kilobyte blob that
   lives in S3;
3. `backup.sh` (host mode): dump, `pg_restore --list` check, checksum, bucket
   mirror, config copy, retention;
4. dropped the schema and deleted the bucket;
5. steps 2 to 4 of this document (verify checksum, `pg_restore
   --exit-on-error`, `rclone copy` of the mirror);
6. started a new `ant serve` on the restored stores: the fingerprint was
   byte-for-byte identical.

Also exercised: an object deleted from the bucket (as `gc` does) ends up under
`s3-deleted/<stamp>/` on the next run, and `BACKUP_RETENTION_DAYS` prunes old
dumps and old `s3-deleted` directories.

Not tested here: the `docker compose exec` / `docker run` wrappers themselves,
and a restore onto a different host. Do a drill on your own host soon after
the first backup (on a spare VM, or by restoring into a differently named
database), and repeat it now and then: an untested backup is a hope.

## Beyond a handful of agents

`pg_dump` takes a consistent snapshot without stopping the server, but it
re-reads the whole schema every night and restores are logical (slow for
hundreds of GB). Once the dump takes more than a few minutes, switch Postgres
to continuous archiving (pgBackRest or WAL-G with PITR) and keep `backup.sh`
only for the bucket and configuration; the S3 half of the procedure does not
change.
