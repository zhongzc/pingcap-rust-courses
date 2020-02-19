use crate::err::Result;
use std::thread;

pub struct NaiveThreadPool;

impl super::ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<NaiveThreadPool> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
