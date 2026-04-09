//! # objstrd
//!
//! S3-compatible HTTP daemon that serves a
//! [`RawObjectStore`](rawobjstr::store::RawObjectStore) (or sharded
//! cluster) via the S3 protocol. Supports standalone, node, and coordinator
//! modes.

pub mod adapter;
pub mod config;
pub mod logging;
pub mod recovery;
pub mod registry;
pub mod viz;
