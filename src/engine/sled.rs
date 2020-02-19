use crate::engine::KvsEngine;
use crate::err::{KeyNonExist, Result};
use sled;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

#[derive(Clone)]
pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<SledStore> {
        Self::try_open(path.into(), 3)
    }

    fn try_open(path: PathBuf, times: usize) -> Result<SledStore> {
        match sled::open(&path) {
            Err(e) => {
                if times <= 0 {
                    Err(Box::new(e))
                } else {
                    sleep(Duration::from_millis(10));
                    Self::try_open(path, times - 1)
                }
            }
            Ok(db) => Ok(SledStore { db }),
        }
    }
}

impl KvsEngine for SledStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        match self.db.insert(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                self.db.flush()?;
                Ok(())
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.db.get(key) {
            Ok(None) => Ok(None),
            Ok(Some(v)) => Ok(Some(String::from_utf8(v.to_vec())?)),
            Err(e) => Err(Box::new(e)),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        match self.db.remove(key) {
            Ok(Some(_)) => {
                self.db.flush()?;
                Ok(())
            }
            _ => Err(Box::new(KeyNonExist)),
        }
    }
}
