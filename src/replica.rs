use std::{any, net::SocketAddr};

use tokio::{io::{AsyncWriteExt, BufReader}, net::TcpStream};
use tracing::{debug, info};

use crate::parser::{self, RedisValue};

pub struct RedisReplica
{
    replicaof: SocketAddr,
}

impl RedisReplica {
    pub fn new(replicaof: SocketAddr) -> Self {
        Self { replicaof }
    }

    pub async fn start_server(&self, addr: SocketAddr) -> anyhow::Result<()> {
        info!("Replicating: {}, listening on: {}", self.replicaof, addr);
        let mut stream = BufReader::new(TcpStream::connect(self.replicaof).await?);
        let request = RedisValue::Array(vec!["replconf".to_string(), "listening-port".to_string(), addr.port().to_string()].into_iter().map(RedisValue::String).collect());
        stream.write_all(request.serialize().as_bytes()).await?;
        info!("REPLCONF 1 sent");

        let reply = parser::parse_token(&mut stream).await.unwrap();
        anyhow::ensure!(reply.0 == RedisValue::String("OK".to_string()));
        debug!("received reply: {:?}", reply.0);

        let request = RedisValue::Array(["replconf", "capa", "psync2"].iter().map(|v| RedisValue::String(v.to_string())).collect());
        stream.write_all(request.serialize().as_bytes()).await?;
        info!("REPLCONF 2 sent");

        Ok(())
    }
}
