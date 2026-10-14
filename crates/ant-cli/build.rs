//! Embeds the build identity into `ANT_VERSION`: `<crate version> (<short
//! source revision>[-dirty])`, or just the crate version when the source is
//! not a git checkout (for example a tarball build).

use std::process::Command;

fn main() {
    let pkg = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();
    // `--match` with a pattern no tag has makes describe fall back to the
    // abbreviated commit id, so a build on a release tag still shows its sha.
    let describe = Command::new("git")
        .args(["describe", "--match=^no-such-tag$", "--always", "--dirty", "--abbrev=12"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let version = match describe {
        Some(d) => format!("{pkg} ({d})"),
        None => pkg,
    };
    println!("cargo:rustc-env=ANT_VERSION={version}");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=build.rs");
}
