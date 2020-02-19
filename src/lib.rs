//#![deny(missing_docs)]
//! An In-memory Key/Value Store

#[macro_use]
extern crate log;

pub mod engine;
pub mod err;
pub mod network;
pub mod thread_pool;
