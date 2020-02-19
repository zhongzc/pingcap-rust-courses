use byteorder::{ReadBytesExt, WriteBytesExt};
use env_logger::{Builder, Target};
use kvs::engine::{KvStore, SledStore};
use kvs::err::{ParseError, Result, ServerNotMatch};
use kvs::network::server::KvsServer;
use kvs::thread_pool::{NaiveThreadPool, ThreadPool};
use log::LevelFilter;
use log::{error, info};
use std::env::current_dir;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
use std::net::{SocketAddr, TcpListener};
use std::process::exit;
use std::str::FromStr;
use structopt::StructOpt;

#[derive(Debug)]
enum Engine {
    Kvs,
    Sled,
}

impl FromStr for Engine {
    type Err = ParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::Kvs),
            "sled" => Ok(Engine::Sled),
            _ => Err(ParseError {}),
        }
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server", version = env!("CARGO_PKG_VERSION"), about = env!("CARGO_PKG_DESCRIPTION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Command {
    #[structopt(
        name = "IP-PORT",
        long = "addr",
        help = "the IP address to bind",
        default_value = "127.0.0.1:4000"
    )]
    addr: SocketAddr,

    #[structopt(
        name = "ENGINE-NAME",
        long = "engine",
        help = "the key/value engine to use",
        default_value = "kvs"
    )]
    engine: Engine,
}

enum EngineImpl {
    Kvs(KvStore),
    Sled(SledStore),
}

fn get_engine(command: &Command) -> Result<EngineImpl> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(".engine")?;

    // engine tag:
    //   0 for unknown, 1 for kvs, 2 for sled
    let opt_tag = file.read_u8().unwrap_or(0u8);
    file.seek(SeekFrom::Start(0u64))?;

    match &command.engine {
        Engine::Kvs if [0u8, 1u8].contains(&opt_tag) => {
            file.write_u8(1u8)?;
            Ok(EngineImpl::Kvs(KvStore::open(current_dir()?)?))
        }
        Engine::Sled if [0u8, 2u8].contains(&opt_tag) => {
            file.write_u8(2u8)?;
            Ok(EngineImpl::Sled(SledStore::open(current_dir()?)?))
        }
        _ => Err(Box::new(ServerNotMatch)),
    }
}

fn main() -> Result<()> {
    let opt: Command = Command::from_args();

    // init logger
    Builder::from_default_env()
        .target(Target::Stderr)
        .filter_level(LevelFilter::Info)
        .init();

    let engine = match get_engine(&opt) {
        Ok(e) => e,
        Err(err) => {
            error!("{}", err.to_string());
            exit(1);
        }
    };

    let listener = TcpListener::bind(&opt.addr)?;
    info!(
        "kvs-server init successfully, version: {}",
        env!("CARGO_PKG_VERSION")
    );
    info!("server is listening to {}", &opt.addr);

    let thread_pool = NaiveThreadPool::new(0)?;
    match engine {
        EngineImpl::Kvs(k) => KvsServer::new(k, listener, thread_pool).do_loop(),
        EngineImpl::Sled(s) => KvsServer::new(s, listener, thread_pool).do_loop(),
    }
}
