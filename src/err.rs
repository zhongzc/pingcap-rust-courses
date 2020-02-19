use serde::export::Formatter;
use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug)]
pub struct KeyNonExist;

impl fmt::Display for KeyNonExist {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Key not found")
    }
}

impl Error for KeyNonExist {
    fn description(&self) -> &str {
        "Key not found"
    }
}

#[derive(Debug)]
pub struct ParseError;

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Parse failed")
    }
}

impl Error for ParseError {
    fn description(&self) -> &str {
        "Parse failed"
    }
}

#[derive(Debug)]
pub struct ServerNotMatch;

impl fmt::Display for ServerNotMatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Server not match")
    }
}

impl Error for ServerNotMatch {
    fn description(&self) -> &str {
        "Server not match"
    }
}
