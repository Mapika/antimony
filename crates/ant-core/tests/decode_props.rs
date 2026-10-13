//! Property tests for the object codecs: decoders never panic on hostile
//! input, and encode/decode roundtrip for every valid value.
//!
//! Tests carrying `#[ignore = "BUG: ..."]` document real defects found by
//! these properties; each has a minimized plain regression test next to it.

use ant_core::{
    Commit, EncodedObject, EntryMode, Object, ObjectId, ObjectKind, Signature, Tree, TreeEntry,
};
use bytes::Bytes;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

fn any_id() -> impl Strategy<Value = ObjectId> {
    any::<[u8; 32]>().prop_map(ObjectId)
}

fn any_mode() -> impl Strategy<Value = EntryMode> {
    prop_oneof![
        Just(EntryMode::File),
        Just(EntryMode::Executable),
        Just(EntryMode::Symlink),
        Just(EntryMode::Tree),
        Just(EntryMode::Gitlink),
    ]
}

/// Names engineered to collide under git's `/`-suffix ordering and to differ
/// only in trailing characters, plus unicode.
fn tricky_name() -> impl Strategy<Value = String> {
    let stems = prop_oneof![
        Just("foo".to_string()),
        Just("a".to_string()),
        Just("é".to_string()),
        Just("日本".to_string()),
        Just("e\u{301}".to_string()), // decomposed é
        Just("\u{10ffff}".to_string()),
        Just("x y".to_string()),
        "[a-c]{1,3}",
    ];
    let suffixes = prop_oneof![
        Just(""),
        Just("-"),
        Just("."),
        Just(".c"),
        Just("0"),
        Just("\u{1}"),
        Just("\u{7f}"),
        Just("\u{80}"),
        Just("é"),
        Just("\u{ff}"),
        Just(" "),
    ];
    (stems, suffixes).prop_map(|(a, b)| format!("{a}{b}"))
}

fn valid_name() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => tricky_name(),
        1 => "[^/\0]{1,8}",
    ]
    .prop_filter("reserved name", |n| !n.is_empty() && n != "." && n != "..")
}

fn entries() -> impl Strategy<Value = Vec<TreeEntry>> {
    prop::collection::btree_map(valid_name(), (any_mode(), any_id()), 0..24).prop_map(|m| {
        m.into_iter().map(|(name, (mode, id))| TreeEntry { mode, name, id }).collect()
    })
}

fn any_tree() -> impl Strategy<Value = Tree> {
    // Feed entries to `Tree::new` in a random order: it must sort.
    entries().prop_shuffle().prop_map(|es| Tree::new(es).expect("valid entries"))
}

/// Text that survives a signature name/email roundtrip.
fn sane_ident() -> impl Strategy<Value = String> {
    "[^<>\\s\0]([^<>\n\r\0]{0,10}[^<>\\s\0])?"
}

fn tz_minutes() -> impl Strategy<Value = i32> {
    prop_oneof![
        Just(14 * 60),
        Just(-14 * 60),
        Just(0),
        Just(5 * 60 + 45),
        Just(-(9 * 60 + 30)),
        -14 * 60..=14 * 60,
    ]
}

fn any_sig() -> impl Strategy<Value = Signature> {
    (
        prop_oneof![sane_ident(), Just(String::new())],
        sane_ident(),
        prop_oneof![any::<i64>(), 0..2_000_000_000i64, Just(0), Just(-1)],
        tz_minutes(),
    )
        .prop_map(|(name, email, time, tz_offset_minutes)| Signature {
            name,
            email,
            time,
            tz_offset_minutes,
        })
}

fn header_key() -> impl Strategy<Value = String> {
    prop_oneof!["[a-z][a-z0-9-]{0,8}", Just("gpgsig".to_string()), Just("encoding".to_string())]
}

fn header_value() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[^\n]{0,12}",
        // multi-line, including blank lines and lines with leading spaces
        prop::collection::vec("[ a-z-]{0,10}", 1..5).prop_map(|ls| ls.join("\n")),
        Just("a\n\nb".to_string()),
        Just("trailing\n".to_string()),
        Just("\nleading".to_string()),
        Just("\n".to_string()),
    ]
}

fn any_message() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        Just("\n".to_string()),
        Just("\n\n".to_string()),
        "\\PC{0,40}",
        "[a-z\n ]{0,40}",
    ]
}

fn any_commit() -> impl Strategy<Value = Commit> {
    (
        any_id(),
        prop::collection::vec(any_id(), 0..4),
        any_sig(),
        any_sig(),
        prop::collection::vec((header_key(), header_value()), 0..4),
        any_message(),
    )
        .prop_map(|(tree, parents, author, committer, extra_headers, message)| Commit {
            tree,
            parents,
            author,
            committer,
            extra_headers,
            message,
        })
}

// ---------------------------------------------------------------------------
// Never panic
// ---------------------------------------------------------------------------

fn mutate(mut bytes: Vec<u8>, edits: Vec<(usize, u8, u8)>) -> Vec<u8> {
    for (pos, kind, val) in edits {
        if bytes.is_empty() {
            bytes.push(val);
            continue;
        }
        let pos = pos % bytes.len();
        match kind % 4 {
            0 => bytes[pos] = val,
            1 => bytes.insert(pos, val),
            2 => {
                bytes.remove(pos);
            }
            _ => bytes.truncate(pos),
        }
    }
    bytes
}

fn edits() -> impl Strategy<Value = Vec<(usize, u8, u8)>> {
    prop::collection::vec((any::<usize>(), any::<u8>(), any::<u8>()), 0..6)
}

proptest! {
    #[test]
    fn encoded_object_from_bytes_never_panics(data in prop::collection::vec(any::<u8>(), 0..64)) {
        if let Ok(o) = EncodedObject::from_bytes(Bytes::from(data)) {
            let _ = o.decode();
            let _ = o.id();
        }
    }

    #[test]
    fn encoded_object_from_mutated_never_panics(
        body in prop::collection::vec(any::<u8>(), 0..32), es in edits()
    ) {
        let enc = EncodedObject::new(ObjectKind::Blob, &body).as_bytes().to_vec();
        if let Ok(o) = EncodedObject::from_bytes(Bytes::from(mutate(enc, es))) {
            let _ = o.decode();
        }
    }

    #[test]
    fn tree_decode_never_panics_on_arbitrary(data in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = Tree::decode(&data);
    }

    #[test]
    fn tree_decode_never_panics_on_mutated(t in any_tree(), es in edits()) {
        let _ = Tree::decode(&mutate(t.encode(), es));
    }

    #[test]
    fn commit_decode_never_panics_on_arbitrary(data in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = Commit::decode(&data);
    }

    #[test]
    fn commit_decode_never_panics_on_arbitrary_text(s in "\\PC{0,200}") {
        let _ = Commit::decode(s.as_bytes());
    }

    #[test]
    fn commit_decode_never_panics_on_mutated(c in any_commit(), es in edits()) {
        let _ = Commit::decode(&mutate(c.encode(), es));
    }

    /// Hostile timezone / name fields in the signature line.
    #[test]
    fn commit_decode_never_panics_on_hostile_tz(tz in "[+-]?\\PC{0,4}", name in "\\PC{0,5}") {
        let tree = ObjectId([1; 32]);
        let text = format!("tree {tree}\nauthor {name} <e> 1 {tz}\ncommitter {name} <e> 1 {tz}\n\nm");
        let _ = Commit::decode(text.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Roundtrips
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn framing_roundtrip(kind in prop_oneof![Just(ObjectKind::Blob), Just(ObjectKind::Tree), Just(ObjectKind::Commit)],
                         body in prop::collection::vec(any::<u8>(), 0..64)) {
        let o = EncodedObject::new(kind, &body);
        let back = EncodedObject::from_bytes(o.as_bytes().clone()).unwrap();
        prop_assert_eq!(back.kind(), kind);
        prop_assert_eq!(back.body(), &body[..]);
        prop_assert_eq!(back.id(), o.id());
        prop_assert!(o.verify(&o.id()).is_ok());
    }

    #[test]
    fn tree_roundtrip(t in any_tree()) {
        let enc = t.encode();
        let dec = Tree::decode(&enc).unwrap();
        prop_assert_eq!(&dec, &t);
        // decoded tree re-encodes byte-identically
        prop_assert_eq!(dec.encode(), enc);
    }

    #[test]
    fn tree_entries_in_git_order(t in any_tree()) {
        let key = |e: &TreeEntry| {
            let mut k = e.name.as_bytes().to_vec();
            if e.mode.is_tree() { k.push(b'/'); }
            k
        };
        let keys: Vec<_> = t.entries().iter().map(key).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        prop_assert_eq!(keys, sorted);
    }

    #[test]
    fn tree_order_is_independent_of_input_order(es in entries()) {
        let mut rev = es.clone();
        rev.reverse();
        prop_assert_eq!(Tree::new(es).unwrap().encode(), Tree::new(rev).unwrap().encode());
    }

    #[test]
    fn tree_object_roundtrip_via_encoded_object(t in any_tree()) {
        let o = Object::Tree(t.clone()).encode();
        let back = EncodedObject::from_bytes(o.as_bytes().clone()).unwrap().decode().unwrap();
        prop_assert_eq!(back, Object::Tree(t));
    }

    #[test]
    fn commit_roundtrip(c in any_commit()) {
        let enc = c.encode();
        let dec = Commit::decode(&enc).unwrap();
        prop_assert_eq!(&dec, &c);
        prop_assert_eq!(dec.encode(), enc);
    }

    #[test]
    fn commit_roundtrip_extreme_tz(tz in -5999..=5999i32, time in any::<i64>()) {
        let sig = Signature { name: "n".into(), email: "e".into(), time, tz_offset_minutes: tz };
        let c = Commit {
            tree: ObjectId([0; 32]), parents: vec![], author: sig.clone(), committer: sig,
            extra_headers: vec![], message: String::new(),
        };
        prop_assert_eq!(Commit::decode(&c.encode()).unwrap(), c);
    }
}

// ---------------------------------------------------------------------------
// Edge cases the "sane" generators avoid
// ---------------------------------------------------------------------------

fn sig_with(name: &str, email: &str) -> Signature {
    Signature { name: name.into(), email: email.into(), time: 1, tz_offset_minutes: 0 }
}

fn commit_with(author: Signature) -> Commit {
    Commit {
        tree: ObjectId([0; 32]),
        parents: vec![],
        committer: author.clone(),
        author,
        extra_headers: vec![],
        message: "m".into(),
    }
}

proptest! {
    /// Any signature that avoids `<`/newline in the name must roundtrip.
    #[test]
    fn commit_signature_name_roundtrip_arbitrary(
        name in "[^<\n\0]{0,8}", email in "[^\n\0]{0,8}"
    ) {
        let c = commit_with(sig_with(&name, &email));
        prop_assert_eq!(Commit::decode(&c.encode()).unwrap(), c);
    }

    /// Empty header keys are invalid (git never writes them); a commit
    /// carrying one must fail to decode, not panic or silently change.
    #[test]
    fn commit_extra_header_empty_key_rejected(v in "[a-z]{0,3}") {
        let mut c = commit_with(sig_with("n", "e"));
        c.extra_headers.push((String::new(), v));
        prop_assert!(Commit::decode(&c.encode()).map(|d| d != c).unwrap_or(true));
    }
}

#[test]
fn regression_tz_multibyte_panics() {
    // tz = "é123": 5 bytes, first char is 2 bytes wide.
    let text = format!(
        "tree {}\nauthor n <e> 1 \u{e9}123\ncommitter n <e> 1 +0000\n\nm",
        ObjectId([0; 32])
    );
    let r = std::panic::catch_unwind(|| Commit::decode(text.as_bytes()));
    assert!(r.is_ok(), "Commit::decode panicked");
}

#[test]
fn regression_signature_name_trailing_space() {
    let c = commit_with(sig_with("a ", "e"));
    assert_eq!(Commit::decode(&c.encode()).unwrap(), c);
}

#[test]
fn regression_empty_header_key() {
    let mut c = commit_with(sig_with("n", "e"));
    c.extra_headers.push((String::new(), "v".into()));
    assert!(Commit::decode(&c.encode()).map(|d| d != c).unwrap_or(true));
}

// ---------------------------------------------------------------------------
// Canonical form: decode accepting non-canonical input
// ---------------------------------------------------------------------------

fn raw_entry(mode: &str, name: &str, id: u8) -> Vec<u8> {
    let mut v = format!("{mode} {name}\0").into_bytes();
    v.extend_from_slice(&[id; 32]);
    v
}

proptest! {
    /// If `Tree::decode` accepts bytes, re-encoding must give the same bytes,
    /// otherwise two different byte strings (hence object ids) denote one tree.
    #[test]
    fn tree_decode_accepted_bytes_reencode_identically(
        names in prop::collection::btree_set("[a-c]{1,2}", 1..5),
        modes in prop::collection::vec(prop_oneof![
            Just("100644"), Just("100664"), Just("100755"), Just("120000"),
            Just("40000"), Just("040000"), Just("160000")], 5),
        reverse in any::<bool>(),
    ) {
        let mut raw = Vec::new();
        let mut names: Vec<_> = names.into_iter().collect();
        if reverse { names.reverse(); }
        for (i, n) in names.iter().enumerate() {
            raw.extend(raw_entry(modes[i], n, i as u8));
        }
        if let Ok(t) = Tree::decode(&raw) {
            prop_assert_eq!(t.encode(), raw);
        }
    }

    /// Canonical modes only, entries in non-git order: must be rejected.
    #[test]
    fn tree_decode_rejects_unsorted(names in prop::collection::btree_set("[a-c]{1,2}", 2..5)) {
        let mut names: Vec<_> = names.into_iter().collect();
        names.reverse();
        let mut raw = Vec::new();
        for (i, n) in names.iter().enumerate() {
            raw.extend(raw_entry("100644", n, i as u8));
        }
        prop_assert!(Tree::decode(&raw).is_err());
    }

    /// Framing must be canonical too: accepting a non-canonical decimal
    /// length makes the content hash malleable.
    #[test]
    fn encoded_object_noncanonical_header(body in prop::collection::vec(any::<u8>(), 1..8),
                                          prefix in prop_oneof![Just("+"), Just("0"), Just("00")]) {
        let mut data = format!("blob {prefix}{}\0", body.len()).into_bytes();
        data.extend_from_slice(&body);
        if let Ok(o) = EncodedObject::from_bytes(Bytes::from(data.clone())) {
            prop_assert_eq!(o.as_bytes().as_ref(), &data[..]);
            let canon = EncodedObject::new(o.kind(), o.body());
            prop_assert_eq!(canon.as_bytes().as_ref(), &data[..]);
        }
    }
}

#[test]
fn regression_noncanonical_mode() {
    assert!(Tree::decode(&raw_entry("100664", "a", 1)).is_err());
}

#[test]
fn regression_zero_padded_tree_mode() {
    assert!(Tree::decode(&raw_entry("040000", "a", 1)).is_err());
}

#[test]
fn regression_unsorted_tree() {
    let mut raw = raw_entry("100644", "b", 1);
    raw.extend(raw_entry("100644", "a", 2));
    assert!(Tree::decode(&raw).is_err());
}

#[test]
fn regression_plus_length_header() {
    for bad in [&b"blob +3\0abc"[..], b"blob 03\0abc", b"blob \0"] {
        assert!(EncodedObject::from_bytes(Bytes::from_static(bad)).is_err(), "{bad:?}");
    }
    assert!(EncodedObject::from_bytes(Bytes::from_static(b"blob 0\0")).is_ok());
}

#[test]
fn tree_collision_order_fixed_cases() {
    // tree "foo" sorts after file "foo.c" and "foo-" but before "foo0".
    let id = ObjectId([1; 32]);
    let t = Tree::new(
        ["foo0", "foo", "foo.c", "foo-"]
            .iter()
            .map(|n| TreeEntry {
                mode: if *n == "foo" { EntryMode::Tree } else { EntryMode::File },
                name: (*n).into(),
                id,
            })
            .collect(),
    )
    .unwrap();
    let names: Vec<_> = t.entries().iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, ["foo-", "foo.c", "foo", "foo0"]);
    assert_eq!(Tree::decode(&t.encode()).unwrap(), t);
}
