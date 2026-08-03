//! Generated gRPC bindings.
//!
//! The definitions live in `/proto` at the repository root so that other
//! languages can generate clients from the same files without depending on
//! anything Rust. This crate is only the Rust view of them.
//!
//! This is a contract crate. Wire changes are visible to every deployed
//! client, so they go through the contract owner.

#![allow(clippy::doc_markdown)]

pub mod v1 {
    tonic::include_proto!("orbita.v1");
}

pub use v1::*;
