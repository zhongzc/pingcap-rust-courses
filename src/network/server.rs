use super::Request;
use super::Response;
use crate::engine::KvsEngine;
use crate::err::Result;
use crate::thread_pool::ThreadPool;
use std::net::{TcpListener, TcpStream};

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    tcp_listener: TcpListener,
    thread_pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, tcp_listener: TcpListener, thread_pool: P) -> Self {
        Self {
            engine,
            tcp_listener,
            thread_pool,
        }
    }

    pub fn do_loop(&mut self) -> Result<()> {
        for stream in self.tcp_listener.incoming() {
            let e = self.engine.clone();
            let stream = stream?;
            self.thread_pool.spawn(move || {
                let _ = Self::process_stream(&e, stream);
            })
        }

        Ok(())
    }

    fn process_stream(engine: &E, stream: TcpStream) -> Result<()> {
        let req: Request = serde_json::from_reader(&stream)?;
        info!("processing request {:?}", req);
        let resp = match req {
            Request::Get { key } => match engine.get(key).unwrap_or(None) {
                None => Response::NotFound,
                Some(v) => Response::Value(v),
            },
            Request::Set { key, value } => {
                let _ = engine.set(key, value);
                Response::Success
            }
            Request::Remove { key } => {
                if engine.remove(key).is_ok() {
                    Response::Success
                } else {
                    Response::NotFound
                }
            }
        };

        serde_json::to_writer(stream, &resp)?;
        info!("return {:?}", resp);
        Ok(())
    }
}
