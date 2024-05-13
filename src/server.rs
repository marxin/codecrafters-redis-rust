use std::collections::HashMap;

use crate::command::{RedisRequest, RedisResponse};

pub struct RedisServer {
    db: HashMap<String, String>,
    replicateof: Option<String>,
    replication_id: [u8; 20],
}

impl RedisServer {
    pub fn new(replicateof: Option<String>) -> Self {
        Self {
            db: HashMap::new(),
            replicateof,
            replication_id: rand::random(),
        }
    }

    pub fn run(&mut self, request: RedisRequest) -> anyhow::Result<RedisResponse> {
        match request {
            RedisRequest::Ping => Ok(RedisResponse::String("PONG".to_string())),
            RedisRequest::Echo { message } => Ok(RedisResponse::String(message)),
            RedisRequest::Get { key } => Ok(self.db.get(&key).map_or_else(
                || RedisResponse::Null,
                |value| RedisResponse::String(value.clone()),
            )),
            RedisRequest::Set { key, value, .. } => {
                self.db.insert(key, value);
                Ok(RedisResponse::String("OK".to_string()))
            }
            RedisRequest::Del { key } => Ok(RedisResponse::String(
                self.db.remove(&key).map_or(0, |_| 1).to_string(),
            )),
            RedisRequest::Info => {
                let output = if self.replicateof.is_some() {
                    "role:slave\n".to_string()
                } else {
                    format!(
                        "role:master\nmaster_replid:{}\nmaster_repl_offset:0\n",
                        hex::encode(self.replication_id)
                    )
                };
                Ok(RedisResponse::String(output))
            }
            _ => todo!(),
        }
    }
}
