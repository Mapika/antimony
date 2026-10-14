//! Core object model for antimony.
//!
//! Objects use git's on-disk object format (`"<kind> <len>\0<body>"`) hashed
//! with SHA-256, i.e. git's `objectFormat = sha256`. Everything that crosses a
//! crate boundary — stores, the wire protocol, the daemon — speaks in terms of
//! [`ObjectId`] plus *encoded* object bytes (header included).

mod commit;
mod error;
mod id;
pub mod merge;
mod object;
mod path;
mod tree;

pub use commit::{Commit, Signature};
pub use error::{Error, Result};
pub use id::ObjectId;
pub use object::{EncodedObject, Object, ObjectKind, hash_object};
pub use path::RepoPath;
pub use tree::{EntryMode, Tree, TreeEntry};
