use crate::err::Result;
use crate::network::{Request, Response};
use std::io::prelude::*;
use std::net::{Shutdown, SocketAddr, TcpStream};

pub struct KvsClient {
    tcp_stream: TcpStream,
}

impl KvsClient {
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        Ok(KvsClient {
            tcp_stream: TcpStream::connect(addr)?,
        })
    }

    pub fn do_request(&mut self, req: &Request) -> Result<Response> {
        serde_json::to_writer(&self.tcp_stream, req)?;
        self.tcp_stream.shutdown(Shutdown::Write)?;

        let mut s = String::new();
        self.tcp_stream.read_to_string(&mut s)?;
        let resp: Response = serde_json::from_str(&s)?;
        Ok(resp)
    }
}
