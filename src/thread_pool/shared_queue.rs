use crate::err::Result;
use num_cpus;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

type JobReceiver = Arc<Mutex<Receiver<Box<dyn FnOnce() + Send + 'static>>>>;
type JobSender = Sender<Box<dyn FnOnce() + Send + 'static>>;

struct Worker(JobReceiver);

impl Worker {
    fn on_loop(&self) -> bool {
        let job = {
            let r = self.0.lock().unwrap();
            r.recv()
        };
        let job = match job {
            Ok(job) => job,
            Err(..) => return false,
        };
        job();
        true
    }

    fn spawn_in_pool(receiver: JobReceiver) {
        thread::spawn(move || {
            let worker = Worker(receiver);
            while worker.on_loop() {}
        });
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            Worker::spawn_in_pool(self.0.clone())
        }
    }
}

pub struct SharedQueueThreadPool {
    jobs: JobSender,
}

impl super::ThreadPool for SharedQueueThreadPool {
    fn new(mut threads: u32) -> Result<SharedQueueThreadPool> {
        let (tx, rx) = channel();

        threads = if threads == 0 {
            num_cpus::get() as u32
        } else {
            threads
        };

        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..threads {
            Worker::spawn_in_pool(rx.clone())
        }
        Ok(SharedQueueThreadPool { jobs: tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.jobs.send(Box::new(job)).unwrap();
    }
}
