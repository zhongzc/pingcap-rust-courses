use super::err::Result;
use clap::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions, rename, remove_file};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

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

pub struct KvStore {
    path: PathBuf,
    log_file: File,
    current_cursor: usize,
    mem_table: HashMap<String, usize>,
    threshold: usize,
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
        let mut current_cursor = 0usize;
        while let Ok((cmd, size)) = read_command_from(&mut log_file, None) {
            mem_table.insert(cmd.get_key(), current_cursor);
            current_cursor += size;
        }

        const THRESHOLD: usize = 128 * 1024;
        let threshold = if current_cursor < THRESHOLD {
            THRESHOLD
        } else {
            current_cursor * 2
        };

        Ok(Self {
            path,
            log_file,
            current_cursor,
            mem_table,
            threshold,
        })
    }

    /// Set the value of a string key to a string.
    ///
    /// Return an error if the value is not written successfully.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.append_command(&Command::Set { key, value })
    }

    /// Get the string value of a string key.
    ///
    /// If the key does not exist, return None. Return an error
    /// if the value is not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let off: usize;
        match self.mem_table.get(&key) {
            None => return Ok(None),
            Some(offset) => {
                off = *offset;
            }
        };

        match read_command_from(&mut self.log_file, Some(off))?.0 {
            Command::Set { value, .. } => Ok(Some(value)),
            Command::Remove { .. } => Ok(None),
        }
    }

    /// Remove a given key.
    ///
    /// Return an error if the key does not exist or is not removed
    /// successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.mem_table.get(&key) {
            None => Err(Box::new(Error::with_description(
                "Key not found",
                ErrorKind::EmptyValue,
            ))),
            _ => self.append_command(&Command::Remove { key }),
        }
    }

    fn append_command(&mut self, command: &Command) -> Result<()> {
        let new_cursor = append_command_to(&mut self.log_file, command, self.current_cursor)?;

        self.mem_table.insert(command.get_key(), self.current_cursor);
        self.current_cursor = new_cursor;

        if self.current_cursor >= self.threshold {
            self.compact()?;
        }

        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let new_log = self.path.join("new_log");
        let mut new_log = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(new_log)?;

        let mut new_cursor = 0usize;
        for (.., v) in std::mem::replace(&mut self.mem_table, Default::default()) {
            let c = &read_command_from(&mut self.log_file, Some(v))?.0;
            if let Command::Set { key, .. } = c {
                let n = append_command_to(&mut new_log, c, new_cursor)?;
                self.mem_table.insert(key.clone(), new_cursor);
                new_cursor = n;
            }
        }

        self.log_file = new_log;
        if new_cursor as f64 >= self.threshold as f64 * 0.9 {
            self.threshold *= 2;
        }
        self.current_cursor = new_cursor;

        remove_file(self.path.join("log"))?;
        rename(self.path.join("new_log"), self.path.join("log"))?;

        Ok(())
    }
}

fn read_command_from(file: &mut File, offset: Option<usize>) -> Result<(Command, usize)> {
    if let Some(off) = offset {
        file.seek(SeekFrom::Start(off as u64))?;
    }

    let mut s = [0u8; 8];
    file.read_exact(&mut s)?;
    let vsize = usize::from_be_bytes(s);

    let mut e = vec![0; vsize];
    file.read_exact(&mut e)?;
    let r: Command = serde_json::from_slice(&e)?;

    Ok((r, vsize + 8))
}

fn append_command_to(file: &mut File, command: &Command, offset: usize) -> Result<usize> {
    file.seek(SeekFrom::Start(offset as u64))?;

    let s = serde_json::to_string(command)?;
    let b = s.into_bytes();
    let l = b.len().to_be_bytes();
    file.write_all(&l)?;
    file.write_all(&b)?;

    Ok(offset + 8 + b.len())
}
