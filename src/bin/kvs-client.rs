use kvs::err::Result;
use kvs::network::client::KvsClient;
use kvs::network::{Request, Response};
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-client", version = env!("CARGO_PKG_VERSION"), about = env!("CARGO_PKG_DESCRIPTION"), author = env!("CARGO_PKG_AUTHORS"))]
enum Command {
    #[structopt(about = "Get the string value of a given string key.")]
    Get {
        #[structopt(name = "KEY", help = "the String key")]
        key: String,
        #[structopt(
            name = "IP-PORT",
            long = "addr",
            help = "server IP address",
            default_value = "127.0.0.1:4000"
        )]
        addr: SocketAddr,
    },

    #[structopt(about = "Set the value of a string key to a string.")]
    Set {
        #[structopt(name = "KEY", help = "the String key to set")]
        key: String,
        #[structopt(name = "VALUE", help = "the String value of the key")]
        value: String,
        #[structopt(
            name = "IP-PORT",
            long = "addr",
            help = "server IP address",
            default_value = "127.0.0.1:4000"
        )]
        addr: SocketAddr,
    },

    #[structopt(about = "Remove a given string key.")]
    Rm {
        #[structopt(name = "KEY", help = "the String key to remove")]
        key: String,
        #[structopt(
            name = "IP-PORT",
            long = "addr",
            help = "server IP address",
            default_value = "127.0.0.1:4000"
        )]
        addr: SocketAddr,
    },
}

fn main() -> Result<()> {
    let opt: Command = Command::from_args();
    let mut client;
    let req;
    match opt {
        Command::Get { key, addr } => {
            client = KvsClient::connect(addr)?;
            req = Request::Get { key };
            match client.do_request(&req)? {
                Response::NotFound => {
                    println!("Key not found");
                }
                Response::Value(value) => {
                    println!("{}", value);
                }
                _ => unreachable!(),
            }
        }
        Command::Set { key, value, addr } => {
            client = KvsClient::connect(addr)?;
            req = Request::Set { key, value };
            client.do_request(&req)?;
        }
        Command::Rm { key, addr } => {
            client = KvsClient::connect(addr)?;
            req = Request::Remove { key };
            if let Response::NotFound = client.do_request(&req)? {
                eprintln!("Key not found");
                exit(1);
            }
        }
    };
    Ok(())
}
