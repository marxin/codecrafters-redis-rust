use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, info, info_span, Instrument};

use crate::{
    command::{RedisRequest, RedisResponse},
    parser::{self, RedisValue},
};

type Storage = Arc<Mutex<HashMap<String, String>>>;

pub struct RedisReplica {
    storage: Storage,

    replicaof: SocketAddr,
}

impl RedisReplica {
    pub fn new(replicaof: SocketAddr) -> Self {
        Self {
            storage: Storage::default(),
            replicaof,
        }
    }

    pub async fn start_replication(&self, addr: SocketAddr) -> anyhow::Result<()> {
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
        let RedisValue::File(content) = reply else {
            panic!();
        };
        info!(
            "got RDB file ({}), starting main loop in replica",
            content.len()
        );

        let storage = self.storage.clone();
        tokio::spawn(async move {
            Self::start_server(storage, addr).await;
        });

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

    async fn handle_connection(stream: TcpStream) -> anyhow::Result<()> {
        let mut stream = BufReader::new(stream);

        loop {
            let query = parser::parse_token(&mut stream).await.unwrap();
            debug!("parsed request: {query:?}");
            let command = RedisRequest::try_from(query.0)?;

            let reply = match command {
                RedisRequest::Null => {
                    break;
                }
                RedisRequest::Ping => RedisResponse::String("PONG".to_string()),
                _ => todo!("unsupported request: {command:?}"),
            };

            stream.write_all(&reply.serialize()).await?;
        }

        Ok(())
    }

    async fn start_server(storage: Storage, addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on: {}", addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            let storage = storage.clone();
            tokio::spawn(
                async move {
                    // TODO
                    Self::handle_connection(stream).await;
                }
                .instrument(info_span!("connection", addr = %addr)),
            );
        }
    }
}
