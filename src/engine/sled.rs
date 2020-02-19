use crate::engine::KvsEngine;
use crate::err::{KeyNonExist, Result};
use sled;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<SledStore> {
        let path = path.into();
        let db;
        let mut times = 0;
        loop {
            match sled::open(&path) {
                Ok(d) => {
                    db = d;
                    break;
                }
                Err(e) => {
                    if times < 3 {
                        times += 1;
                        sleep(Duration::from_millis(50));
                        continue;
                    } else {
                        return Err(Box::new(e));
                    }
                }
            }
        }
        Ok(SledStore { db })
    }
}

impl KvsEngine for SledStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        match self.db.insert(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                self.db.flush()?;
                Ok(())
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.db.get(key) {
            Ok(None) => Ok(None),
            Ok(Some(v)) => Ok(Some(String::from_utf8(v.to_vec())?)),
            Err(e) => Err(Box::new(e)),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.db.remove(key) {
            Ok(Some(_)) => {
                self.db.flush()?;
                Ok(())
            }
            _ => Err(Box::new(KeyNonExist)),
        }
    }
}
