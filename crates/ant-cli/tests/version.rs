//! `ant --version` prints the crate version and, in a git checkout, the
//! short source revision: `ant <version> (<sha>[-dirty])`.

use std::process::Command;

#[test]
fn version_prints_crate_version_and_revision() {
    let out = Command::new(env!("CARGO_BIN_EXE_ant"))
        .arg("--version")
        .output()
        .expect("run ant --version");
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).expect("utf8");
    let line = text.trim_end();
    let pkg = env!("CARGO_PKG_VERSION");
    let rest = line
        .strip_prefix(&format!("ant {pkg}"))
        .unwrap_or_else(|| panic!("unexpected version line {line:?}"));
    // A source tarball has no revision; a git checkout has ` (<hex>[-dirty])`.
    if !rest.is_empty() {
        let sha = rest
            .strip_prefix(" (")
            .and_then(|r| r.strip_suffix(')'))
            .unwrap_or_else(|| panic!("unexpected revision suffix {rest:?}"));
        let sha = sha.strip_suffix("-dirty").unwrap_or(sha);
        assert!(
            !sha.is_empty() && sha.chars().all(|c| c.is_ascii_hexdigit()),
            "revision is not a hex id: {sha:?}"
        );
    }
}
