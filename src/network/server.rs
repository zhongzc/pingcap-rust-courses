use super::Request;
use super::Response;
use crate::engine::KvsEngine;
use crate::err::Result;
use std::io::Read;
use std::net::TcpListener;

pub struct KvsServer {
    engine: Box<dyn KvsEngine>,
    tcp_listener: TcpListener,
}

impl KvsServer {
    pub fn new(engine: Box<dyn KvsEngine>, tcp_listener: TcpListener) -> Self {
        Self {
            engine,
            tcp_listener,
        }
    }

    pub fn do_loop(&mut self) -> Result<()> {
        loop {
            let (mut stream, addr) = self.tcp_listener.accept()?;
            info!("accept a connection from {}", addr);
            let mut s = String::new();
            stream.read_to_string(&mut s)?;

            let req: Request = serde_json::from_str(&s)?;

            info!("get request {:?} from {}", req, addr);

            let resp = match req {
                Request::Get { key } => match self.engine.get(key).unwrap_or(None) {
                    None => Response::NotFound,
                    Some(v) => Response::Value(v),
                },
                Request::Set { key, value } => {
                    let _ = self.engine.set(key, value);
                    Response::Success
                }
                Request::Remove { key } => {
                    if self.engine.remove(key).is_ok() {
                        Response::Success
                    } else {
                        Response::NotFound
                    }
                }
            };

            serde_json::to_writer(stream, &resp)?;
            info!("return {:?} to {}", resp, addr);
        }
    }
}
