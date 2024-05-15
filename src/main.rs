use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use replica::RedisReplica;

use crate::server::RedisServer;

mod command;
mod parser;
mod server;
mod replica;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port to listed to
    #[arg(long, default_value_t = 6379)]
    port: u16,
    /// Replica of a master instance
    #[arg(long, num_args(2))]
    replicaof: Option<Vec<String>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));

    if let Some(replica) = args.replicaof {
        let replica = RedisReplica::new(SocketAddr::from_str(&replica.join(":"))?);
        replica.start_server(addr).await
    } else {
        let server = Arc::new(RedisServer::new());
        RedisServer::start_server(server, addr).await
    }
}
