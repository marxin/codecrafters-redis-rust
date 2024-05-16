use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use clap::Parser;
use replica::RedisReplica;
use tracing::{info_span, Instrument};

use crate::server::RedisServer;

mod command;
mod parser;
mod replica;
mod server;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port to listed to
    #[arg(long, default_value_t = 6379)]
    port: u16,
    /// Replica of a master instance
    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));

    if let Some(replica) = args.replicaof {
        let replica = replica.replace(' ', ":");
        let replica_addr = replica
            .to_socket_addrs()?
            .find(|addr| matches!(addr, SocketAddr::V4(..)))
            .ok_or(anyhow::anyhow!(
                "could not parse --replicaof address: {replica}"
            ))?;
        let replica = RedisReplica::new(replica_addr);
        replica
            .start_replication(addr)
            .instrument(info_span!("replication"))
            .await
    } else {
        let server = Arc::new(RedisServer::new());
        RedisServer::start_server(server, addr).await
    }
}
