//! Module containing HTTP handlers. Mapping to these from various routes is configured in
//! `ocypod-server.rs`.

pub mod health;
pub mod info;
pub mod job;
pub mod queue;
pub mod tag;
