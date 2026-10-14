//! End-to-end tests of `ant cache stats` and `ant cache gc` (no server).

mod common;

use std::path::Path;
use std::process::{Command, Output};

use ant_core::{EncodedObject, ObjectKind};
use ant_store::ObjectStore;
use antd::disk::DiskObjectStore;

const BLOB: usize = 20_000;

fn ant(cache: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ant"))
        .env("ANT_CACHE", cache)
        .env_remove("ANT_CACHE_BYTES")
        .args(args)
        .output()
        .expect("running ant")
}

fn ok(out: Output) -> String {
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf8")
}

/// `key value` lines to numbers, checking the keys and their order.
fn numbers(text: &str, keys: &[&str]) -> Vec<u64> {
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), keys.len(), "{text:?}");
    lines
        .iter()
        .zip(keys)
        .map(|(line, key)| {
            let (k, v) = line.split_once(' ').expect("`key value`");
            assert_eq!(k, *key, "{text:?}");
            v.parse().expect("number")
        })
        .collect()
}

fn blob(seed: u64) -> EncodedObject {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let body: Vec<u8> = (0..BLOB)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect();
    EncodedObject::new(ObjectKind::Blob, &body)
}

#[tokio::test]
async fn cache_stats_and_gc() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = dir.path().join("cache");
    let store = DiskObjectStore::open(&cache, u64::MAX).expect("open");
    let objs: Vec<_> = (0..20).map(blob).collect();
    store.put_many(&objs).await.expect("put");
    let on_disk = store.disk_usage().await.expect("usage");
    drop(store);

    // Empty-ish budget default is 20 GiB.
    let [n, bytes, budget] =
        numbers(&ok(ant(&cache, &["cache", "stats"])), &["objects", "bytes", "budget"])[..]
    else {
        panic!("three numbers")
    };
    assert_eq!((n, bytes, budget), (20, on_disk, 20 << 30));

    // Under budget: gc is a no-op.
    let out = ok(ant(&cache, &["cache", "gc"]));
    let v = numbers(&out, &["removed", "freed", "objects", "bytes"]);
    assert_eq!(v, vec![0, 0, 20, on_disk]);

    // Over a (flag) budget: down to 90% of it.
    let budget = on_disk / 2;
    let out = ok(ant(&cache, &["--cache-bytes", &budget.to_string(), "cache", "gc"]));
    let v = numbers(&out, &["removed", "freed", "objects", "bytes"]);
    assert!(v[0] > 0 && v[0] < 20, "{out}");
    assert_eq!(v[1], on_disk - v[3], "{out}");
    assert!(v[3] <= budget / 10 * 9, "{out}");
    let [n2, b2, bud] = numbers(
        &ok(ant(&cache, &["--cache-bytes", &budget.to_string(), "cache", "stats"])),
        &["objects", "bytes", "budget"],
    )[..] else {
        panic!("three numbers")
    };
    assert_eq!((n2, b2, bud), (v[2], v[3], budget));

    // `--to`: an explicit target, whatever the budget.
    let target = v[3] / 2;
    let out = ok(ant(&cache, &["cache", "gc", "--to", &target.to_string()]));
    let w = numbers(&out, &["removed", "freed", "objects", "bytes"]);
    assert!(w[3] <= target && w[2] < v[2], "{out}");
    let out = ok(ant(&cache, &["cache", "gc", "--to", "0"]));
    assert_eq!(numbers(&out, &["removed", "freed", "objects", "bytes"])[2..], [0, 0]);
    let stats = ok(ant(&cache, &["cache", "stats"]));
    assert_eq!(numbers(&stats, &["objects", "bytes", "budget"])[..2], [0, 0]);

    // The emptied cache is still usable.
    let store = DiskObjectStore::open(&cache, u64::MAX).expect("reopen");
    store.put_many(&objs[..1]).await.expect("put");
    assert!(store.get_many(&[objs[0].id()]).await.expect("get")[0].is_some());
}

#[test]
fn cache_commands_reject_bad_arguments() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = ant(dir.path(), &["cache", "gc", "--to", "lots"]);
    assert!(!out.status.success());
    let out = ant(dir.path(), &["cache"]);
    assert!(!out.status.success());
}
