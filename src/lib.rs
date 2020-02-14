//#![deny(missing_docs)]
//! An In-memory Key/Value Store

pub mod err;
pub mod kvs;

pub use crate::kvs::KvStore;
pub use crate::err::Result;
