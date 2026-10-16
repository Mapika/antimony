#!/usr/bin/env bash
# Upgrade test: a released version against the current checkout, on real
# Postgres + S3. Proves the documented upgrade paths (docs/operations.md,
# "Upgrading"; docs/compatibility.md) for the next release.
#
#   scripts/upgrade-test.sh [--from vX.Y.Z] [--old-bin PATH] [--new-bin PATH]
#                           [--corpus BARE-GIT-DIR [--corpus-ref REF]]
#                           [--only inplace|rolling] [--out DIR] [--work DIR]
#                           [--keep]
#
#   --from TAG       released version to upgrade from (default: the newest
#                    v* tag that is an ancestor of HEAD); built from a
#                    temporary worktree unless --old-bin is given
#   --old-bin PATH   use this `ant` as the old version instead of building
#   --new-bin PATH   use this `ant` as the new version (default: build the
#                    checkout, `target/release/ant`)
#   --corpus DIR     also import this bare git repository with the old
#                    version (e.g. a bare clone of git/git) and check that
#                    every read of it survives the upgrade
#   --corpus-ref R   git ref of the corpus to import (default refs/heads/master)
#   --only S         run one scenario: `inplace` (stop old, start new on the
#                    same schema) or `rolling` (old and new side by side)
#   --work DIR       parent of the work directory (default $TMPDIR or /tmp;
#                    holds the old version's build, a few GB)
#   --keep           keep the schemas, buckets and work directory
#
# Scenarios (each prints PASS/FAIL lines; exit 1 if any check failed):
#   inplace  old: import, lands, unlanded changeset, workspaces, caches,
#            blame (blame/v1 rows), log, diff, git clone; snapshot every
#            output. New client against the old server gives the same
#            output. Upgrade in place: every snapshot byte-identical through
#            the new server (old and new client), fresh clone fscks, old
#            client caches and workspaces keep working with the new client,
#            old cache-serve in front of the new server and new cache-serve
#            in front of the old one. New data (lands with pack frames, an
#            incremental import), checks again. `ant admin gc` (dry run and
#            real, after simulating a month), checks again; blame/v1 rows of
#            collected commits are gone. Downgrade: the old binary refuses the
#            migrated schema (serve, import, admin) and changes nothing.
#   rolling  two old servers; one new server with --pack-frames false
#            migrates the schema while they keep serving; mixed writes and
#            reads in both directions, no frames written; an old server
#            restart is refused; what an old server does with a frame (clean
#            error, never wrong bytes); old servers gone, frames on, checks.
#
# Requires the test services (scripts/dev-services.sh start, or docker
# compose), python3 and git. Not a benchmark: no timings, no bench lock.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
STAMP=$(date +%Y%m%d%H%M%S)
FROM=""
OLD=""
NEW=""
CORPUS=""
CORPUS_REF="refs/heads/master"
ONLY=""
KEEP=0
OUT=""
while [ $# -gt 0 ]; do
    case "$1" in
        --from) FROM="$2"; shift 2 ;;
        --old-bin) OLD="$2"; shift 2 ;;
        --new-bin) NEW="$2"; shift 2 ;;
        --corpus) CORPUS="$2"; shift 2 ;;
        --corpus-ref) CORPUS_REF="$2"; shift 2 ;;
        --only) ONLY="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        --work) WORK_PARENT="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
[ -n "$FROM" ] || FROM=$(git describe --tags --abbrev=0 --match 'v*' HEAD)
WORK="${WORK_PARENT:-${TMPDIR:-/tmp}}/ant-upgrade-$STAMP"
OUT="${OUT:-$WORK/out}"
mkdir -p "$WORK" "$OUT"
WORK=$(cd "$WORK" && pwd)
OUT=$(cd "$OUT" && pwd)
RESULTS="$OUT/results.txt"
: > "$RESULTS"
PG_URL="postgres://ant:ant@localhost:54329/ant"
SERVICES_DIR="${ANT_SERVICES_DIR:-$HOME/.local/share/antimony-services}"
PSQL=$(command -v psql || ls "$SERVICES_DIR"/postgresql-*/bin/psql 2>/dev/null | head -1 || true)
[ -n "$PSQL" ] || { echo "psql not found" >&2; exit 2; }
SCHEMAS=()
BUCKETS=()
PIDS=()

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }
sql() { PGPASSWORD=ant "$PSQL" -h 127.0.0.1 -p 54329 -U ant -d ant -qAtX -v ON_ERROR_STOP=1 -c "$1"; }
FAILS=0
pass() { echo "PASS $*" | tee -a "$RESULTS"; }
fail() { echo "FAIL $*" | tee -a "$RESULTS"; FAILS=$((FAILS + 1)); }
# check <name> <command...>: PASS when the command succeeds.
check() { local n="$1"; shift; if "$@" >> "$OUT/run.log" 2>&1; then pass "$n"; else fail "$n"; fi; }
# same <name> <a> <b>: files or directories with identical contents.
same() { if diff -r "$2" "$3" > "$OUT/diff-$1.txt" 2>&1; then pass "$1"; else fail "$1 (see $OUT/diff-$1.txt)"; fi; }

guard() {
    if [ -f "$SERVICES_DIR/DISK_LOW" ]; then log "DISK_LOW present: stopping"; exit 3; fi
    local free
    free=$(df -BG --output=avail / | tail -1 | tr -dc 0-9)
    [ "$free" -ge 20 ] || { log "only ${free} GB free on /"; exit 3; }
    if [ -d "${ANT_EXTRA_DISK:-/nonexistent}" ]; then
        free=$(df -BG --output=avail "${ANT_EXTRA_DISK:-/nonexistent}" | tail -1 | tr -dc 0-9)
        [ "$free" -ge 20 ] || { log "only ${free} GB free on $ANT_EXTRA_DISK"; exit 3; }
    fi
}

cleanup() {
    local p
    for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    if [ "$KEEP" = 0 ]; then
        for s in "${SCHEMAS[@]}"; do sql "DROP SCHEMA IF EXISTS $s CASCADE" >/dev/null 2>&1 || true; done
        local weed="$SERVICES_DIR/bin/weed"
        for b in "${BUCKETS[@]}"; do
            # SeaweedFS frees space only when a whole bucket is deleted.
            [ -x "$weed" ] && printf 's3.bucket.delete -name %s\n' "$b" |
                timeout 300 "$weed" shell -master=127.0.0.1:19333 >/dev/null 2>&1 || true
        done
        if [ -d "$WORK/old-src" ]; then git worktree remove --force "$WORK/old-src" || true; fi
        # Keep the results when they were asked for elsewhere.
        case "$OUT" in "$WORK"/*) cp -r "$OUT" "${WORK}-results" && echo "results kept in ${WORK}-results" ;; esac
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

free_ports() {
    python3 -c 'import socket,sys
s=[socket.socket() for _ in range(int(sys.argv[1]))]
[x.bind(("127.0.0.1",0)) for x in s]
print(*[x.getsockname()[1] for x in s])' "$1"
}

export ANT_AUTHOR_NAME="Upgrade Test" ANT_AUTHOR_EMAIL="upgrade@example.com"
unset ANT_SERVER ANT_HTTP ANT_CACHE ANT_TOKEN ANT_PACK_FRAMES ANT_NO_MIGRATE

guard
# ---------------------------------------------------------------- binaries
if [ -z "$NEW" ]; then
    log "building the checkout"
    cargo build -q --release -p ant-cli
    NEW="$ROOT/target/release/ant"
fi
if [ -z "$OLD" ]; then
    log "building $FROM"
    git worktree add -q --detach "$WORK/old-src" "$FROM"
    (cd "$WORK/old-src" && cargo build -q --release -p ant-cli)
    OLD="$WORK/old-src/target/release/ant"
fi
log "old: $("$OLD" --version)  new: $("$NEW" --version)"

# The repository itself as the first corpus: the old tag imported first,
# HEAD later as an incremental import.
git clone -q --bare --no-local "$ROOT" "$WORK/self.git"
git -C "$WORK/self.git" update-ref refs/heads/imp "$FROM^{commit}"
git -C "$WORK/self.git" fetch -q "$ROOT" "+HEAD:refs/heads/head"

# ---------------------------------------------------------------- helpers
STORE=()
use_store() { # <schema> <bucket>
    SCHEMAS+=("$1")
    BUCKETS+=("$2")
    STORE=(--pg-url "$PG_URL" --pg-schema "$1" --s3-endpoint http://localhost:9009
        --s3-bucket "$2" --s3-access-key x --s3-secret-key x)
    SCHEMA="$1"
}

# start_server <name> <bin> [flags...]: sets <name>_GRPC / <name>_HTTP
# (urls) and <name>_PID.
start_server() {
    local name="$1" bin="$2" g h pid i
    shift 2
    read -r g h < <(free_ports 2)
    "$bin" serve "${STORE[@]}" --grpc-listen "127.0.0.1:$g" --http-listen "127.0.0.1:$h" "$@" \
        >> "$OUT/server-$name.log" 2>&1 &
    pid=$!
    PIDS+=("$pid")
    for i in $(seq 1 240); do
        if "$bin" --server "http://127.0.0.1:$g" ref list > /dev/null 2>&1; then
            printf -v "${name}_GRPC" '%s' "http://127.0.0.1:$g"
            printf -v "${name}_HTTP" '%s' "http://127.0.0.1:$h"
            printf -v "${name}_PID" '%s' "$pid"
            return 0
        fi
        kill -0 "$pid" 2> /dev/null || { log "server $name exited"; return 1; }
        sleep 0.5
    done
    log "server $name did not come up"
    return 1
}
stop_server() { local pid="${1}_PID"; kill "${!pid}" 2>/dev/null || true; wait "${!pid}" 2>/dev/null || true; }

# ant <bin> <grpc> <http|-> <cache> args...
ant() {
    local bin="$1" g="$2" h="$3" c="$4"
    shift 4
    if [ "$h" = - ]; then
        "$bin" --server "$g" --cache "$c" "$@"
    else
        "$bin" --server "$g" --http "$h" --cache "$c" "$@"
    fi
}

# q <dir> <name> <command...>: stdout and exit code of one read.
q() {
    local d="$1" n="$2" rc=0
    shift 2
    "$@" > "$d/$n" 2> "$d/$n.stderr" || rc=$?
    echo "$rc" > "$d/$n.rc"
}

# snap <dir> <bin> <grpc> <http> <cache>: every read the upgrade must keep
# byte-identical, over the commits recorded before it (R_*). stderr is
# kept for reading but not compared (its wording may change).
snap() {
    local d="$1" b="$2" g="$3" h="$4" c="$5"
    mkdir -p "$d"
    a() { ant "$b" "$g" "$h" "$c" "$@"; }
    q "$d" ls-root a ls "$R_MAIN"
    q "$d" ls-sub a ls "$R_MAIN:crates/ant-server/src"
    q "$d" ls-blob a ls "$R_MAIN:README.md"
    q "$d" cat-readme a cat "$R_MAIN:README.md"
    q "$d" cat-history a cat "$R_SRC:crates/ant-server/src/ops/history.rs"
    q "$d" cat-landed a cat "$R_MAIN:upgrade/notes.txt"
    q "$d" cat-big a cat "$R_MAIN:upgrade/big/a.txt"
    q "$d" cat-missing a cat "$R_MAIN:no/such/file"
    q "$d" blame-history a blame "$R_SRC:crates/ant-server/src/ops/history.rs"
    q "$d" blame-readme a blame "$R_MAIN:README.md"
    q "$d" blame-notes a blame "$R_MAIN:upgrade/notes.txt"
    q "$d" blame-dead a blame "$R_DEAD:upgrade/notes.txt"
    q "$d" log a log "$R_MAIN" -n 40
    q "$d" log-all a log "$R_SRC" -n 100000
    q "$d" log-path a log "$R_MAIN" --path crates/ant-store/src/postgres.rs -n 20
    q "$d" log-path-landed a log "$R_MAIN" -- upgrade/notes.txt
    q "$d" log-range a log "$R_SRC..$R_MAIN"
    q "$d" diff a diff "$R_SRC" "$R_MAIN"
    q "$d" diff-patch a diff --patch "$R_SRC" "$R_MAIN"
    q "$d" diff-path a diff "$R_SRC" "$R_MAIN" -- upgrade/big
    q "$d" merge-base a merge-base "$R_SRC" "$R_DEAD"
    q "$d" landed-as a landed-as "$R_LANDED_SUBMIT" --ref main
    q "$d" ref-src a ref get src
    if [ -n "${R_CORPUS:-}" ]; then
        q "$d" corpus-ls a ls "$R_CORPUS"
        q "$d" corpus-cat a cat "$R_CORPUS:Makefile"
        q "$d" corpus-blame a blame "$R_CORPUS:builtin/commit.c"
        q "$d" corpus-log a log "$R_CORPUS" -n 200
        q "$d" corpus-log-path a log "$R_CORPUS" --path builtin/commit.c -n 30
        q "$d" corpus-ref a ref get corpus
    fi
}

# clone_check <name> <http> [<git ref src should match>]: mirror clone of
# the git export, `git fsck`, the commits and objects of the refs recorded
# before the upgrade (compared with the first clone, `a-old`), and the files
# of `src` (and `corpus`) against the source repositories.
clone_check() {
    local n="$1" h="$2" srcref="${3:-refs/heads/imp}" d="$WORK/clone-$1" o="$OUT/clone-$1"
    rm -rf "$d"
    mkdir -p "$o"
    if ! git clone -q --mirror "$h/repo.git" "$d" >> "$OUT/run.log" 2>&1; then
        fail "$n: git clone"
        return
    fi
    check "$n: git fsck" git -C "$d" fsck --no-progress --strict
    git -C "$d" rev-parse refs/heads/src > "$o/src"
    git -C "$d" rev-list --objects refs/heads/src | sort | sha256sum > "$o/src-objects"
    git -C "$d" rev-list --objects "$R_MAIN_GIT" | sort | sha256sum > "$o/main-objects"
    rm -rf "$WORK/tree-a" "$WORK/tree-b"
    mkdir -p "$WORK/tree-a" "$WORK/tree-b"
    git -C "$d" archive refs/heads/src | tar -x -C "$WORK/tree-a"
    git -C "$WORK/self.git" archive "$srcref" | tar -x -C "$WORK/tree-b"
    same "$n: export src tree = git ${srcref#refs/heads/} tree" "$WORK/tree-a" "$WORK/tree-b"
    if [ -n "${R_CORPUS:-}" ]; then
        git -C "$d" rev-parse refs/heads/corpus > "$o/corpus"
        git -C "$d" rev-list --objects refs/heads/corpus | sort | sha256sum > "$o/corpus-objects"
        rm -rf "$WORK/tree-a" "$WORK/tree-b"
        mkdir -p "$WORK/tree-a" "$WORK/tree-b"
        git -C "$d" archive refs/heads/corpus | tar -x -C "$WORK/tree-a"
        git -C "$CORPUS" archive "$CORPUS_REF" | tar -x -C "$WORK/tree-b"
        same "$n: export corpus tree = git tree" "$WORK/tree-a" "$WORK/tree-b"
    fi
    rm -rf "$WORK/tree-a" "$WORK/tree-b" "$d"
    local a="$OUT/clone-a-old"
    if [ "$n" != a-old ]; then
        [ "$srcref" = refs/heads/imp ] || rm -f "$o/src" "$o/src-objects"
        local f fs=()
        for f in "$o"/*; do fs+=("$(basename "$f")"); done
        if (cd "$o" && cat "${fs[@]}") | cmp -s - <(cd "$a" && cat "${fs[@]}"); then
            pass "$n: exported commit ids and objects as before the upgrade (${fs[*]})"
        else
            fail "$n: exported commit ids or objects changed (${fs[*]})"
        fi
    fi
}

# Files of a workspace (without .ant), as one digest per file.
ws_digest() { (cd "$1" && find . -path ./.ant -prune -o -type f -print0 | sort -z | xargs -0 sha256sum); }

# Large (> 4 KiB inline threshold) text files that look alike: pack frames
# put such objects of one write together.
big_files() { # <dir> <seed>
    mkdir -p "$1"
    python3 - "$1" "$2" << 'EOF'
import sys, random
d, seed = sys.argv[1], sys.argv[2]
base = [f"line {i:05d}: the quick brown fox jumps over the lazy dog {i * 7919 % 10007}" for i in range(1500)]
for k, name in enumerate("abcd"):
    r = random.Random(f"{seed}-{name}")
    lines = list(base)
    for _ in range(20):
        lines[r.randrange(len(lines))] = f"changed in {seed}/{name}: {r.random()}"
    open(f"{d}/{name}.txt", "w").write("\n".join(lines) + "\n")
EOF
}

# Index rows sharing one record = members of multi-object frames.
framed_rows() { sql "SELECT count(*) FROM $SCHEMA.object_index WHERE share IS NOT NULL OR (pack, off) IN (SELECT pack, off FROM $SCHEMA.object_index GROUP BY 1, 2 HAVING count(*) > 1)"; }

# State of a schema that a refused binary must not change.
fingerprint() {
    sql "SELECT version, name FROM $SCHEMA.schema_version ORDER BY 1"
    local t
    for t in objects object_index refs commits log changes derived changesets; do
        echo "$t $(sql "SELECT count(*) FROM $SCHEMA.$t")"
    done
    sql "SELECT name, encode(target, 'hex') FROM $SCHEMA.refs ORDER BY 1" 2>/dev/null || true
}

# refused <name> <command...>: exit 1, a `hint:` naming an upgrade, and no
# change to the schema.
refused() {
    local n="$1" rc=0 before after
    shift
    before=$(fingerprint)
    timeout 60 "$@" > "$OUT/refused.out" 2> "$OUT/refused.err" || rc=$?
    after=$(fingerprint)
    cat "$OUT/refused.err" >> "$OUT/run.log"
    if [ "$rc" = 1 ] && grep -q '^hint: .*upgrade' "$OUT/refused.err" && [ "$before" = "$after" ]; then
        pass "$n refused (exit 1, hint: $(grep '^hint:' "$OUT/refused.err" | head -1 | cut -c7-60)...)"
    else
        fail "$n: exit $rc, stderr: $(head -2 "$OUT/refused.err" | tr '\n' ' '), schema unchanged: $([ "$before" = "$after" ] && echo yes || echo no)"
    fi
}

# =================================================================== inplace
inplace() {
    log "== in-place upgrade"
    use_store "upgrade_${STAMP}_a" "ant-upgrade-$STAMP-a"
    local C="$WORK/a" s
    mkdir -p "$C"

    log "old: import $FROM of this repository"
    check "old import (schema created at the old version)" \
        "$OLD" import "$WORK/self.git" --git-ref refs/heads/imp --ref src "${STORE[@]}"
    if [ -n "$CORPUS" ]; then
        guard
        log "old: import $CORPUS"
        check "old import of the corpus" "$OLD" import "$CORPUS" --git-ref "$CORPUS_REF" --ref corpus "${STORE[@]}"
    fi
    "$OLD" admin schema "${STORE[@]}" > "$OUT/schema-old.txt"
    start_server OLD1 "$OLD"
    local o=(ant "$OLD" "$OLD1_GRPC" "$OLD1_HTTP" "$C/cache-old")
    R_SRC=$("${o[@]}" ref get src)
    [ -z "$CORPUS" ] || R_CORPUS=$("${o[@]}" ref get corpus)
    "${o[@]}" ref set main "$R_SRC" --create > /dev/null

    log "old: workspaces, lands, an unlanded changeset"
    "${o[@]}" ws new "$C/ws1" > /dev/null
    mkdir -p "$C/ws1/upgrade"
    printf 'first line\nsecond line\n' > "$C/ws1/upgrade/notes.txt"
    echo "upgrade test" >> "$C/ws1/README.md"
    big_files "$C/ws1/upgrade/big" one
    "${o[@]}" -C "$C/ws1" submit -m "upgrade: notes and look-alike files" > "$C/submit1"
    R_LANDED_SUBMIT=$(cat "$C/submit1")
    check "old client: land" "${o[@]}" -C "$C/ws1" land
    printf 'third line\n' >> "$C/ws1/upgrade/notes.txt"
    "${o[@]}" -C "$C/ws1" submit -m "upgrade: third line" > /dev/null
    check "old client: second land" "${o[@]}" -C "$C/ws1" land
    R_MAIN=$("${o[@]}" ref get main)
    # A changeset that is never landed: garbage once it is old.
    "${o[@]}" ws new "$C/ws-dead" > /dev/null
    echo "never landed" >> "$C/ws-dead/upgrade/notes.txt"
    R_DEAD=$("${o[@]}" -C "$C/ws-dead" submit -m "upgrade: never landed")
    # A sparse workspace with an uncommitted edit, carried over the upgrade.
    "${o[@]}" ws new "$C/ws-sparse" --sparse 'upgrade/**' --sparse 'crates/ant-core/**' > /dev/null
    echo "edit made before the upgrade" >> "$C/ws-sparse/upgrade/notes.txt"
    "${o[@]}" -C "$C/ws-sparse" status > "$C/status-before"
    check "old client: cache seed" "${o[@]}" cache seed "$R_MAIN"

    log "old: snapshot"
    snap "$OUT/a-old" "${o[@]:1}"
    s=$(sql "SELECT count(*) FROM $SCHEMA.derived WHERE kind = 'blame/v1'")
    if [ "$s" -gt 0 ]; then pass "old server wrote $s blame/v1 rows"; else fail "no blame/v1 rows"; fi
    for f in ls-root cat-readme blame-history blame-dead log log-range diff-patch landed-as; do
        [ "$(cat "$OUT/a-old/$f.rc")" = 0 ] || fail "old snapshot $f failed: $(cat "$OUT/a-old/$f.stderr")"
    done
    R_MAIN_GIT=""
    local d="$WORK/clone-pre"
    git clone -q --mirror "$OLD1_HTTP/repo.git" "$d"
    check "old export: git fsck" git -C "$d" fsck --no-progress --strict
    R_MAIN_GIT=$(git -C "$d" rev-parse refs/heads/main)
    rm -rf "$d"
    clone_check a-old "$OLD1_HTTP"

    log "new client against the old server"
    snap "$OUT/a-newclient" "$NEW" "$OLD1_GRPC" "$OLD1_HTTP" "$C/cache-new"
    same "new client = old client against the old server" "$OUT/a-old" "$OUT/a-newclient" || true
    local n=(ant "$NEW" "$OLD1_GRPC" "$OLD1_HTTP" "$C/cache-new")
    "${n[@]}" ws new "$C/ws-n" > /dev/null
    echo "from the new client" > "$C/ws-n/upgrade/new-client.txt"
    check "new client, old server: status" "${n[@]}" -C "$C/ws-n" status
    check "new client, old server: submit" "${n[@]}" -C "$C/ws-n" submit -m "new client on old server"
    check "new client, old server: land" "${n[@]}" -C "$C/ws-n" land
    check "new client, old server: ws update" "${o[@]}" -C "$C/ws1" ws update
    check "new client, old server: cache seed" "${n[@]}" cache seed main
    check "new client, old server: cache stats --tiers" "${n[@]}" cache stats --tiers
    # The new cache tier in forward mode in front of the old server (no
    # POST /o/check there: falls back to conditional GETs).
    local cp
    read -r cp < <(free_ports 1)
    "$NEW" cache-serve --upstream "$OLD1_HTTP" --listen "127.0.0.1:$cp" --cache "$C/tier-new" \
        --auth forward >> "$OUT/cache-serve-new.log" 2>&1 &
    PIDS+=("$!")
    sleep 1
    ant "$NEW" "$OLD1_GRPC" "http://127.0.0.1:$cp" "$C/cache-via-tier" ws new "$C/ws-tier" --at "$R_MAIN" > /dev/null || true
    "${o[@]}" ws new "$C/ws-direct" --at "$R_MAIN" > /dev/null
    same "new cache-serve in front of the old server: checkout" <(ws_digest "$C/ws-tier") <(ws_digest "$C/ws-direct")
    rm -rf "$C/ws-tier" "$C/ws-direct"
    stop_server OLD1

    # ------------------------------------------------------------ upgrade
    log "upgrade in place"
    # The new version must not read blame/v1 rows: make them unreadable, so
    # that reading one fails the identical-output checks below.
    sql "UPDATE $SCHEMA.derived SET value = '\\xdeadbeef'::bytea WHERE kind = 'blame/v1'"
    "$NEW" admin schema --no-migrate "${STORE[@]}" > "$OUT/schema-pending.txt" || true
    if grep -q '^pending 6 pack_frames' "$OUT/schema-pending.txt"; then pass "new binary lists pending migration 6"; else fail "pending migrations: $(cat "$OUT/schema-pending.txt")"; fi
    local want
    want=$(awk '/^supported /{print $2}' "$OUT/schema-pending.txt")
    start_server NEW1 "$NEW"
    s=$(sql "SELECT max(version) FROM $SCHEMA.schema_version")
    if [ -n "$want" ] && [ "$s" = "$want" ]; then pass "schema_version is $want (the new binary's) after the new server started"; else fail "schema_version $s, want $want"; fi
    snap "$OUT/b-old" "$OLD" "$NEW1_GRPC" "$NEW1_HTTP" "$C/cache-old"
    same "old client: every output identical after the upgrade" "$OUT/a-old" "$OUT/b-old"
    snap "$OUT/b-new" "$NEW" "$NEW1_GRPC" "$NEW1_HTTP" "$C/cache-new2"
    same "new client: every output identical after the upgrade" "$OUT/a-old" "$OUT/b-new"
    s=$(sql "SELECT count(*) FROM $SCHEMA.derived WHERE kind = 'blame/v2'")
    if [ "$s" -gt 0 ]; then pass "new server derives blame/v2 ($s rows), not reading blame/v1"; else fail "no blame/v2 rows"; fi
    clone_check b-upgraded "$NEW1_HTTP"

    log "old client state with the new client"
    o=(ant "$OLD" "$NEW1_GRPC" "$NEW1_HTTP" "$C/cache-old")
    n=(ant "$NEW" "$NEW1_GRPC" "$NEW1_HTTP" "$C/cache-old")
    "${n[@]}" -C "$C/ws-sparse" status > "$C/status-after" || true
    same "new client reads an old sparse workspace (status)" "$C/status-before" "$C/status-after"
    check "new client on old workspace: ws update" "${n[@]}" -C "$C/ws-sparse" ws update
    check "new client on old workspace: submit" "${n[@]}" -C "$C/ws-sparse" submit -m "edit from before the upgrade"
    check "new client on old workspace: land" "${n[@]}" -C "$C/ws-sparse" land
    check "new client on the old cache: cache stats" "${n[@]}" cache stats --tiers
    check "new client on the old cache: cache seed" "${n[@]}" cache seed main
    check "new client on the old cache: cache gc" "${n[@]}" cache gc
    check "old client, new server: ws update" "${o[@]}" -C "$C/ws1" ws update
    echo "old client after the upgrade" >> "$C/ws1/upgrade/notes.txt"
    check "old client, new server: status" "${o[@]}" -C "$C/ws1" status
    check "old client, new server: submit" "${o[@]}" -C "$C/ws1" submit -m "old client on new server"
    check "old client, new server: land" "${o[@]}" -C "$C/ws1" land
    check "old client, new server: cache seed" "${o[@]}" cache seed main
    check "old client, new server: log" "${o[@]}" log -n 5
    check "old client, new server: blame" "${o[@]}" blame main:upgrade/notes.txt
    # The old cache tier in front of the new server.
    read -r cp < <(free_ports 1)
    "$OLD" cache-serve --upstream "$NEW1_HTTP" --listen "127.0.0.1:$cp" --cache "$C/tier-old" \
        --auth forward >> "$OUT/cache-serve-old.log" 2>&1 &
    PIDS+=("$!")
    sleep 1
    ant "$OLD" "$NEW1_GRPC" "http://127.0.0.1:$cp" "$C/cache-via-tier2" ws new "$C/ws-tier" --at "$R_MAIN" > /dev/null || true
    "${n[@]}" ws new "$C/ws-direct" --at "$R_MAIN" > /dev/null
    same "old cache-serve in front of the new server: checkout" <(ws_digest "$C/ws-tier") <(ws_digest "$C/ws-direct")

    log "new data: frames, incremental import"
    "${n[@]}" -C "$C/ws-direct" ws update > /dev/null
    big_files "$C/ws-direct/upgrade/big2" two
    check "new client: land look-alike files" bash -c "'${NEW}' --server '$NEW1_GRPC' --cache '$C/cache-old' -C '$C/ws-direct' submit -m frames > /dev/null && '${NEW}' --server '$NEW1_GRPC' --cache '$C/cache-old' -C '$C/ws-direct' land"
    s=$(framed_rows)
    if [ "$s" -gt 0 ]; then pass "new server writes pack frames ($s framed index rows)"; else fail "no frames written"; fi
    check "new import: incremental $FROM -> HEAD" \
        "$NEW" import "$WORK/self.git" --git-ref refs/heads/head --ref src "${STORE[@]}"
    snap "$OUT/c-new" "$NEW" "$NEW1_GRPC" "$NEW1_HTTP" "$C/cache-new3"
    # ref-src moved by the import; everything else must not change.
    rm -f "$OUT"/c-new/ref-src*
    mkdir -p "$OUT/a-cmp"
    cp "$OUT"/a-old/* "$OUT/a-cmp/"
    rm -f "$OUT"/a-cmp/ref-src*
    same "after new writes: old outputs identical" "$OUT/a-cmp" "$OUT/c-new"
    d="$WORK/clone-head"
    git clone -q --mirror "$NEW1_HTTP/repo.git" "$d"
    check "after new writes: git fsck" git -C "$d" fsck --no-progress --strict
    rm -rf "$WORK/tree-a" "$WORK/tree-b"
    mkdir -p "$WORK/tree-a" "$WORK/tree-b"
    git -C "$d" archive refs/heads/src | tar -x -C "$WORK/tree-a"
    git -C "$WORK/self.git" archive refs/heads/head | tar -x -C "$WORK/tree-b"
    same "incremental import: export tree = git HEAD tree" "$WORK/tree-a" "$WORK/tree-b"
    rm -rf "$d" "$WORK/tree-a" "$WORK/tree-b"
    "${n[@]}" cat main:upgrade/big2/a.txt > "$C/big2-read"
    same "framed object reads back" "$C/big2-read" "$C/ws-direct/upgrade/big2/a.txt"

    log "gc"
    check "gc --dry-run" "$NEW" admin gc --dry-run "${STORE[@]}"
    check "gc (nothing old enough)" "$NEW" admin gc --quiesce 1s "${STORE[@]}"
    snap "$OUT/d-gc1" "$NEW" "$NEW1_GRPC" "$NEW1_HTTP" "$C/cache-new4"
    rm -f "$OUT"/d-gc1/ref-src*
    same "after gc: outputs identical" "$OUT/a-cmp" "$OUT/d-gc1"
    # A month passes: everything becomes old enough to collect.
    for t in objects object_index commits changesets; do
        sql "UPDATE $SCHEMA.$t SET written_at = written_at - interval '30 days'"
    done
    local v1_dead
    v1_dead=$(sql "SELECT count(*) FROM $SCHEMA.derived WHERE kind = 'blame/v1' AND substr(key, 1, 32) = decode('$R_DEAD', 'hex')")
    [ "$v1_dead" -gt 0 ] && pass "blame/v1 row of the unlanded commit present before gc" || fail "no blame/v1 row keyed by the unlanded commit"
    "$NEW" admin gc --dry-run "${STORE[@]}" > "$OUT/gc-dry.txt" 2>> "$OUT/run.log"
    "$NEW" admin gc --quiesce 1s "${STORE[@]}" > "$OUT/gc.txt" 2>> "$OUT/run.log" && pass "gc after a month: $(tr '\n' ' ' < "$OUT/gc.txt")" || fail "gc after a month"
    v1_dead=$(sql "SELECT count(*) FROM $SCHEMA.derived WHERE kind = 'blame/v1' AND substr(key, 1, 32) = decode('$R_DEAD', 'hex')")
    s=$(sql "SELECT count(*) FROM $SCHEMA.derived WHERE kind = 'blame/v1'")
    [ "$v1_dead" = 0 ] && pass "gc collected the blame/v1 rows of the dead commit ($s rows of live commits kept)" || fail "blame/v1 rows of the dead commit left: $v1_dead"
    # Restart: no cached state from before the collection.
    stop_server NEW1
    start_server NEW2 "$NEW"
    snap "$OUT/d-gc2" "$NEW" "$NEW2_GRPC" "$NEW2_HTTP" "$C/cache-new5"
    rm -f "$OUT"/d-gc2/ref-src*
    # The unlanded commit is garbage now: its reads must fail, the rest not.
    for f in blame-dead merge-base; do
        if [ "$(cat "$OUT/d-gc2/$f.rc")" != 0 ]; then pass "after gc: $f of the collected commit fails"; else fail "after gc: $f of the collected commit still answers"; fi
        rm -f "$OUT"/d-gc2/$f* "$OUT"/a-cmp/$f*
    done
    same "after a real gc: every other output identical" "$OUT/a-cmp" "$OUT/d-gc2"
    clone_check d-gc "$NEW2_HTTP" refs/heads/head

    log "downgrade attempt"
    refused "old ant admin schema" "$OLD" admin schema "${STORE[@]}"
    refused "old ant admin gc" "$OLD" admin gc --dry-run "${STORE[@]}"
    refused "old ant import" "$OLD" import "$WORK/self.git" --git-ref refs/heads/imp --ref other "${STORE[@]}"
    read -r cp s < <(free_ports 2)
    refused "old ant serve" "$OLD" serve "${STORE[@]}" --grpc-listen "127.0.0.1:$cp" --http-listen "127.0.0.1:$s"
    refused "old ant serve --no-migrate" "$OLD" serve --no-migrate "${STORE[@]}" --grpc-listen "127.0.0.1:$cp" --http-listen "127.0.0.1:$s"
    snap "$OUT/e-after-downgrade" "$NEW" "$NEW2_GRPC" "$NEW2_HTTP" "$C/cache-new6"
    rm -f "$OUT"/e-after-downgrade/ref-src* "$OUT"/e-after-downgrade/blame-dead* "$OUT"/e-after-downgrade/merge-base*
    same "after the refused downgrade: outputs identical" "$OUT/a-cmp" "$OUT/e-after-downgrade"
    stop_server NEW2
}

# =================================================================== rolling
rolling() {
    log "== rolling upgrade"
    use_store "upgrade_${STAMP}_b" "ant-upgrade-$STAMP-b"
    local C="$WORK/b" s
    mkdir -p "$C"
    check "old import" "$OLD" import "$WORK/self.git" --git-ref refs/heads/imp --ref main "${STORE[@]}"
    start_server OLDA "$OLD"
    start_server OLDB "$OLD"
    local oa=(ant "$OLD" "$OLDA_GRPC" "$OLDA_HTTP" "$C/cache-oa")
    local ob=(ant "$OLD" "$OLDB_GRPC" "$OLDB_HTTP" "$C/cache-ob")
    "${oa[@]}" ws new "$C/ws-old" > /dev/null
    big_files "$C/ws-old/upgrade/big1" one
    "${oa[@]}" -C "$C/ws-old" submit -m "old: look-alike files" > /dev/null
    check "old server A: land" "${oa[@]}" -C "$C/ws-old" land

    log "step 1: one new server with --pack-frames false (migrates)"
    start_server NEWA "$NEW" --pack-frames false
    s=$(sql "SELECT max(version) FROM $SCHEMA.schema_version")
    local want
    want=$("$NEW" admin schema --no-migrate "${STORE[@]}" | awk '/^supported /{print $2}')
    if [ -n "$want" ] && [ "$s" = "$want" ]; then pass "rolling: schema migrated to $want while old servers run"; else fail "rolling: schema_version $s, want $want"; fi
    local na=(ant "$NEW" "$NEWA_GRPC" "$NEWA_HTTP" "$C/cache-na")
    check "rolling: old server B still serves (readyz)" curl -fsS "$OLDB_HTTP/readyz"
    "${na[@]}" ws new "$C/ws-new" > /dev/null
    big_files "$C/ws-new/upgrade/big2" two
    "${na[@]}" -C "$C/ws-new" submit -m "new (frames off): look-alike files" > /dev/null
    check "rolling: land through the new server" "${na[@]}" -C "$C/ws-new" land
    check "rolling: incremental import with --pack-frames false" \
        "$NEW" import "$WORK/self.git" --git-ref refs/heads/head --ref head --pack-frames false "${STORE[@]}"
    s=$(framed_rows)
    [ "$s" = 0 ] && pass "rolling: no frames written while --pack-frames false" || fail "rolling: $s framed index rows with frames off"
    # Old servers read what the new one wrote, and keep writing.
    "${ob[@]}" cat main:upgrade/big2/a.txt > "$C/read-ob" 2>> "$OUT/run.log" || true
    same "rolling: old server reads a large object the new server wrote" "$C/read-ob" "$C/ws-new/upgrade/big2/a.txt"
    "${ob[@]}" cat head:crates/ant-store/src/packed/frame.rs > "$C/read-ob2" 2>> "$OUT/run.log" || true
    git -C "$WORK/self.git" show refs/heads/head:crates/ant-store/src/packed/frame.rs > "$C/read-git2"
    same "rolling: old server reads the new import" "$C/read-ob2" "$C/read-git2"
    check "rolling: old server B ws update" "${ob[@]}" -C "$C/ws-old" ws update
    echo "old server B after migration" > "$C/ws-old/upgrade/b.txt"
    "${ob[@]}" -C "$C/ws-old" submit -m "old B after migration" > /dev/null
    check "rolling: land through an old server after the migration" "${ob[@]}" -C "$C/ws-old" land
    check "rolling: new server sees the old server's land" "${na[@]}" cat main:upgrade/b.txt
    local d="$WORK/clone-rolling"
    git clone -q --mirror "$OLDB_HTTP/repo.git" "$d" >> "$OUT/run.log" 2>&1 && check "rolling: clone from an old server's export: git fsck" git -C "$d" fsck --no-progress --strict || fail "rolling: clone from old server"
    rm -rf "$d"
    git clone -q --mirror "$NEWA_HTTP/repo.git" "$d" >> "$OUT/run.log" 2>&1 && check "rolling: clone from the new server's export: git fsck" git -C "$d" fsck --no-progress --strict || fail "rolling: clone from new server"
    rm -rf "$d"
    local p1 p2
    read -r p1 p2 < <(free_ports 2)
    refused "rolling: restarting an old server" "$OLD" serve "${STORE[@]}" --grpc-listen "127.0.0.1:$p1" --http-listen "127.0.0.1:$p2"

    log "what the flag prevents: a frame read by an old server"
    start_server NEWX "$NEW"
    local nx=(ant "$NEW" "$NEWX_GRPC" "$NEWX_HTTP" "$C/cache-nx")
    "${nx[@]}" ws new "$C/ws-x" > /dev/null
    big_files "$C/ws-x/upgrade/big3" three
    "${nx[@]}" -C "$C/ws-x" submit -m "frames on: look-alike files" > /dev/null
    "${nx[@]}" -C "$C/ws-x" land > /dev/null
    s=$(framed_rows)
    [ "$s" -gt 0 ] && pass "frames written with the flag on ($s rows)" || fail "no frames with the flag on"
    local rc=0
    "${ob[@]}" cat main:upgrade/big3/a.txt > "$C/read-x" 2> "$C/read-x.err" || rc=$?
    if [ "$rc" = 1 ] && [ ! -s "$C/read-x" ]; then
        pass "old server refuses a framed object (exit 1: $(head -1 "$C/read-x.err" | cut -c1-120))"
    elif cmp -s "$C/read-x" "$C/ws-x/upgrade/big3/a.txt"; then
        pass "old server read a framed object (it was also stored alone)"
    else
        fail "old server returned wrong bytes for a framed object (exit $rc)"
    fi
    stop_server NEWX

    log "step 2: old servers gone, new servers restart with frames on"
    stop_server OLDA
    stop_server OLDB
    stop_server NEWA
    start_server NEWB "$NEW"
    local nb=(ant "$NEW" "$NEWB_GRPC" "$NEWB_HTTP" "$C/cache-nb")
    "${nb[@]}" -C "$C/ws-new" ws update > /dev/null
    big_files "$C/ws-new/upgrade/big4" four
    "${nb[@]}" -C "$C/ws-new" submit -m "frames on" > /dev/null
    check "rolling done: land with frames" "${nb[@]}" -C "$C/ws-new" land
    for f in big1 big2 big3 big4; do
        "${nb[@]}" cat "main:upgrade/$f/c.txt" > "$C/read-$f" 2>> "$OUT/run.log" || true
    done
    same "rolling done: objects of every phase read back (old)" "$C/read-big1" "$C/ws-old/upgrade/big1/c.txt"
    same "rolling done: objects of every phase read back (frames off)" "$C/read-big2" "$C/ws-new/upgrade/big2/c.txt"
    same "rolling done: objects of every phase read back (frames)" "$C/read-big4" "$C/ws-new/upgrade/big4/c.txt"
    git clone -q --mirror "$NEWB_HTTP/repo.git" "$d" >> "$OUT/run.log" 2>&1 && check "rolling done: git fsck" git -C "$d" fsck --no-progress --strict || fail "rolling done: clone"
    rm -rf "$d"
    stop_server NEWB
}

case "$ONLY" in
    "") inplace; rolling ;;
    inplace) inplace ;;
    rolling) rolling ;;
    *) echo "unknown scenario: $ONLY" >&2; exit 2 ;;
esac

log "results: $(grep -c '^PASS' "$RESULTS") passed, $FAILS failed ($RESULTS)"
[ "$FAILS" = 0 ]
