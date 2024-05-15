use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use clap::Parser;

use crate::server::RedisServer;

mod command;
mod parser;
mod server;

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
        todo!("replica server not implement yet");
    } else {
        let server = Arc::new(RedisServer::new());
        RedisServer::start_server(server, addr).await
    }
}
