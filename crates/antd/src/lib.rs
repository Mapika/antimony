//! Client-side engine: a local content-addressed object cache shared by all
//! agents on a machine, a read-through layer onto the server, and sparse
//! working copies.
//!
//! The disk cache is safe for concurrent use by many processes without a
//! coordinating daemon: objects are immutable and written via
//! write-temp-then-rename.

pub mod disk;
pub mod readthrough;
pub mod remote;
pub mod update;
pub mod workspace;
