use std::{any, net::SocketAddr};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tracing::{debug, info};

use crate::parser::{self, RedisValue};

pub struct RedisReplica {
    replicaof: SocketAddr,
}

impl RedisReplica {
    pub fn new(replicaof: SocketAddr) -> Self {
        Self { replicaof }
    }

    pub async fn start_server(&self, addr: SocketAddr) -> anyhow::Result<()> {
        info!("Replicating: {}, listening on: {}", self.replicaof, addr);
        let mut stream = BufReader::new(TcpStream::connect(self.replicaof).await?);

        stream
            .write_all(&RedisValue::Array(vec![RedisValue::String("ping".to_string())]).serialize())
            .await?;
        info!("PING sent");
        let reply = parser::parse_token(&mut stream).await.unwrap();
        anyhow::ensure!(reply.0 == RedisValue::String("PONG".to_string()));

        let request = RedisValue::Array(
            vec![
                "replconf".to_string(),
                "listening-port".to_string(),
                addr.port().to_string(),
            ]
            .into_iter()
            .map(RedisValue::String)
            .collect(),
        );
        stream.write_all(&request.serialize()).await?;
        info!("REPLCONF 1 sent");
        let reply = parser::parse_token(&mut stream).await.unwrap();
        anyhow::ensure!(reply.0 == RedisValue::String("OK".to_string()));

        let request = RedisValue::Array(
            ["replconf", "capa", "psync2"]
                .iter()
                .map(|v| RedisValue::String(v.to_string()))
                .collect(),
        );
        stream.write_all(&request.serialize()).await?;
        info!("REPLCONF 2 sent");
        let reply = parser::parse_token(&mut stream).await.unwrap();
        anyhow::ensure!(reply.0 == RedisValue::String("OK".to_string()));

        let request = RedisValue::Array(
            ["psync", "?", "-1"]
                .iter()
                .map(|v| RedisValue::String(v.to_string()))
                .collect(),
        );
        stream.write_all(&request.serialize()).await?;
        info!("PSYNC 2 sent");
        let reply = parser::parse_token(&mut stream).await.unwrap();
        info!("got reply for PSYNC: {:?}", reply.0);
        // TODO: check arguments of FULLRESYNC

        let reply = parser::parse_file(&mut stream).await.unwrap();
        info!("got RDB file, starting main loop in replica");

        loop {
            let token_result = parser::parse_token(&mut stream).await.unwrap();
            info!("parsed command: {token_result:?}");
            if matches!(token_result.0, RedisValue::None) {
                break;
            }
            stream
                .write_all(&RedisValue::String("OK".to_string()).serialize())
                .await?;
        }

        Ok(())
    }
}
