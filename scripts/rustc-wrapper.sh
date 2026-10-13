#!/bin/sh
# Compiler cache shared by every checkout/worktree/agent on this machine.
# Falls back to plain rustc when sccache is unavailable (or ANT_NO_SCCACHE=1).
#
# Cache keys contain absolute paths, so each checkout normalizes paths against
# its own root (SCCACHE_BASEDIRS) and therefore runs its own sccache server
# (basedirs are read at server start); all servers share one on-disk cache.
# For hits across checkouts, build into the checkout's own ./target (the
# default) — do not point CARGO_TARGET_DIR elsewhere.
if [ -n "$ANT_NO_SCCACHE" ] || ! command -v sccache >/dev/null 2>&1; then
    exec "$@"
fi
root=$(cd "$(dirname "$0")/.." && pwd -P)
export SCCACHE_BASEDIRS="$root"
export SCCACHE_DIR="${SCCACHE_DIR:-$HOME/.cache/sccache}"
export SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-40G}"
port=$(( 42000 + $(printf '%s' "$root" | cksum | cut -d' ' -f1) % 2000 ))
export SCCACHE_SERVER_PORT="$port"
exec sccache "$@"
