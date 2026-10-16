#!/usr/bin/env bash
# Keep a plain-git copy of every antimony ref, optionally pushed to a remote.
#
#   deploy/git-mirror.sh        (cron, e.g. every 10 minutes; see README.md)
#
# It fetches antimony's read-only git export (https://<domain>/repo.git, git
# smart HTTP v2) into a local bare repository, then pushes all branches to
# MIRROR_PUSH_URL. antimony ref `agents/x/try1` becomes branch `agents/x/try1`.
#
# antimony stores SHA-256 git objects, so the export is a SHA-256 repository.
#   MIRROR_FORMAT=sha256  push it as it is: same commit ids as antimony. The
#                         remote must support SHA-256 repositories (a bare
#                         repository over SSH does; many hosted services do
#                         not, GitHub among them at the time of writing: check).
#   MIRROR_FORMAT=sha1    (default) convert on the way: `git fast-export` from
#                         the SHA-256 copy into a SHA-1 repository, incremental
#                         through marks files. Commit ids differ from antimony's
#                         (SHA-1 ids of the same content and history); trees,
#                         messages, authors and dates are identical. Use this
#                         for GitHub/GitLab. The mapping stays in
#                         $MIRROR_DIR/marks-*.
# Without MIRROR_PUSH_URL it only maintains the local bare repository
# ($MIRROR_DIR/source.git, plus sha1.git in sha1 mode): already a complete
# plain-git backup you can `git clone`.
#
# Environment (or deploy/.env): MIRROR_SOURCE_URL (required, with the token as
# the password: https://mirror:TOKEN@ant.example.com/repo.git; the principal
# needs read access to the whole repository), MIRROR_DIR, MIRROR_PUSH_URL,
# MIRROR_FORMAT. Deleted antimony refs are deleted on the remote too (--prune):
# the push is a mirror, not an archive. Needs git >= 2.42.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
if [ -f "$here/.env" ]; then set -a; . "$here/.env"; set +a; fi

: "${MIRROR_SOURCE_URL:?set MIRROR_SOURCE_URL (see .env.example)}"
dir="${MIRROR_DIR:-/var/lib/antimony-mirror}"
format="${MIRROR_FORMAT:-sha1}"
push_url="${MIRROR_PUSH_URL:-}"
case "$format" in sha1 | sha256) ;; *) echo "MIRROR_FORMAT must be sha1 or sha256" >&2; exit 2 ;; esac

export GIT_TERMINAL_PROMPT=0
log() { printf '%s git-mirror: %s\n' "$(date -u +%FT%TZ)" "$*"; }

mkdir -p "$dir"
exec 9>"$dir/.lock"
flock -n 9 || { log "another run is active"; exit 0; }

src="$dir/source.git"
if [ ! -d "$src" ]; then
    git init -q --bare --object-format=sha256 "$src"
fi

# --- 1. antimony -> source.git (SHA-256, identical ids) --------------------------
# The URL (with its token) is passed on the command line, never stored in the
# repository's config.
log "fetching"
git -C "$src" fetch -q --prune --no-tags --force "$MIRROR_SOURCE_URL" \
    '+refs/heads/*:refs/heads/*'
if [ -z "$(git -C "$src" for-each-ref --count=1 refs/heads)" ]; then
    log "no refs yet; nothing to do"
    exit 0
fi

# --- 2. convert if the remote wants SHA-1 -------------------------------------------
out="$src"
if [ "$format" = sha1 ]; then
    out="$dir/sha1.git"
    [ -d "$out" ] || git init -q --bare --object-format=sha1 "$out"
    touch "$dir/marks-src" "$dir/marks-dst"
    cp "$dir/marks-src" "$dir/marks-src.new"
    cp "$dir/marks-dst" "$dir/marks-dst.new"
    log "converting to sha1"
    # Only commits not yet in the marks files are exported, so a run costs
    # what changed. Both marks files are replaced together, after both sides
    # succeeded; an interrupted run simply redoes its work.
    if ! { git -C "$src" fast-export --all --signed-tags=strip --reencode=yes \
                --import-marks="$dir/marks-src.new" --export-marks="$dir/marks-src.new" \
            | git -C "$out" fast-import --quiet --force \
                --import-marks="$dir/marks-dst.new" --export-marks="$dir/marks-dst.new"; }; then
        rm -f "$dir/marks-src.new" "$dir/marks-dst.new"
        log "conversion to sha1 failed. A history that contains submodule links (gitlinks)"
        log "cannot be converted (their ids belong to another repository); use MIRROR_FORMAT=sha256"
        exit 1
    fi
    mv "$dir/marks-src.new" "$dir/marks-src"
    mv "$dir/marks-dst.new" "$dir/marks-dst"
    # fast-export does not report deleted branches: drop them in the copy too.
    comm -13 <(git -C "$src" for-each-ref --format='%(refname)' refs/heads | sort) \
             <(git -C "$out" for-each-ref --format='%(refname)' refs/heads | sort) \
        | while read -r ref; do git -C "$out" update-ref -d "$ref"; done
fi

# --- 3. push -------------------------------------------------------------------------------
if [ -n "$push_url" ]; then
    log "pushing $(git -C "$out" for-each-ref refs/heads | wc -l) branch(es) to remote"
    git -C "$out" push -q --prune --force "$push_url" '+refs/heads/*:refs/heads/*'
fi

date -u +%Y%m%dT%H%M%SZ > "$dir/last-success"
log "done"
