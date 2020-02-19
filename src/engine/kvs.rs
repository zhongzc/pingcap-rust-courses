use super::KvsEngine;
use crate::err::{KeyNonExist, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{remove_file, rename, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn get_key(&self) -> String {
        match self {
            Command::Set { key, .. } => key.clone(),
            Command::Remove { key } => key.clone(),
        }
    }
}

struct RawKvStore {
    path: PathBuf,
    log_file: File,
    current_cursor: u64,
    mem_table: HashMap<String, u64>,
    threshold: u64,
}

#[derive(Clone)]
pub struct KvStore(Arc<Mutex<RawKvStore>>);

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.append_command(&Command::Set { key, value })
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let mut guard = self.0.lock().unwrap();
        let raw = &mut *guard;

        let off: u64;
        match raw.mem_table.get(&key) {
            None => return Ok(None),
            Some(offset) => {
                off = *offset;
            }
        };

        match Self::read_command_from(&mut raw.log_file, Some(off))?.0 {
            Command::Set { value, .. } => Ok(Some(value)),
            Command::Remove { .. } => Ok(None),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut guard = self.0.lock().unwrap();
        let raw = &mut *guard;

        match raw.mem_table.get(&key) {
            None => Err(Box::new(KeyNonExist)),
            _ => {
                drop(guard);
                self.append_command(&Command::Remove { key })
            }
        }
    }
}

impl KvStore {
    /// Open the KvStore at a given path.
    ///
    /// Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        let log = path.join("log");
        let mut log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(log)?;

        let mut mem_table = HashMap::new();
        let mut current_cursor = 0u64;
        while let Ok((cmd, size)) = Self::read_command_from(&mut log_file, None) {
            mem_table.insert(cmd.get_key(), current_cursor);
            current_cursor += size;
        }

        const DEFAULT_THRESHOLD: u64 = 128 * 1024;
        let threshold = if current_cursor < DEFAULT_THRESHOLD {
            DEFAULT_THRESHOLD
        } else {
            current_cursor * 2
        };

        Ok(Self(Arc::new(Mutex::new(RawKvStore {
            path,
            log_file,
            current_cursor,
            mem_table,
            threshold,
        }))))
    }

    fn append_command(&self, command: &Command) -> Result<()> {
        let mut guard = self.0.lock().unwrap();
        let raw = &mut *guard;

        let new_cursor = Self::append_command_to(&mut raw.log_file, command, raw.current_cursor)?;

        raw.mem_table.insert(command.get_key(), raw.current_cursor);
        raw.current_cursor = new_cursor;

        if raw.current_cursor >= raw.threshold {
            drop(guard);
            self.compact()?;
        }

        Ok(())
    }

    fn compact(&self) -> Result<()> {
        let mut guard = self.0.lock().unwrap();
        let raw = &mut *guard;

        let new_log = raw.path.join("new_log");
        let mut new_log = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(new_log)?;

        let mut new_cursor = 0u64;
        for (.., v) in std::mem::replace(&mut raw.mem_table, Default::default()) {
            let c = &Self::read_command_from(&mut raw.log_file, Some(v))?.0;
            if let Command::Set { key, .. } = c {
                let n = Self::append_command_to(&mut new_log, c, new_cursor)?;
                raw.mem_table.insert(key.clone(), new_cursor);
                new_cursor = n;
            }
        }

        raw.log_file = new_log;
        if new_cursor as f64 >= raw.threshold as f64 * 0.9 {
            raw.threshold *= 2;
        }
        raw.current_cursor = new_cursor;

        remove_file(raw.path.join("log"))?;
        rename(raw.path.join("new_log"), raw.path.join("log"))?;

        Ok(())
    }

    fn read_command_from(file: &mut File, offset: Option<u64>) -> Result<(Command, u64)> {
        if let Some(off) = offset {
            file.seek(SeekFrom::Start(off as u64))?;
        }

        let mut s = [0u8; 8];
        file.read_exact(&mut s)?;
        let vsize = usize::from_be_bytes(s);

        let mut e = vec![0; vsize];
        file.read_exact(&mut e)?;
        let r: Command = serde_json::from_slice(&e)?;

        Ok((r, vsize as u64 + 8))
    }

    fn append_command_to(file: &mut File, command: &Command, offset: u64) -> Result<u64> {
        file.seek(SeekFrom::Start(offset as u64))?;

        let s = serde_json::to_string(command)?;
        let b = s.into_bytes();
        let l = b.len().to_be_bytes();
        file.write_all(&l)?;
        file.write_all(&b)?;

        Ok(offset + 8 + b.len() as u64)
    }
}
