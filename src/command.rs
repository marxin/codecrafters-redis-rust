use std::time::Duration;

use anyhow::Ok;

use crate::parser::{parse_token, RedisValue};

#[derive(Debug)]
pub enum RedisRequest {
    Null,
    Ping,
    Echo {
        message: String,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        expiration: Option<Duration>,
    },
    Info,
}

#[derive(Debug)]
pub enum RedisResponse {
    Null,
    String(String),
}

impl TryFrom<RedisValue> for RedisRequest {
    type Error = anyhow::Error;

    fn try_from(value: RedisValue) -> Result<Self, Self::Error> {
        if matches!(value, RedisValue::None) {
            return Ok(RedisRequest::Null);
        }

        let RedisValue::Array(array) = value else {
            anyhow::bail!("array expected for request");
        };

        anyhow::ensure!(!array.is_empty(), "non-empty array expected");
        let RedisValue::String(ref command) = array[0] else {
            anyhow::bail!("command must be string");
        };

        match command.to_lowercase().as_str() {
            "ping" => Ok(RedisRequest::Ping),
            "echo" => {
                let arg = array
                    .get(1)
                    .ok_or(anyhow::anyhow!("ECHO argument expected"))?;
                let RedisValue::String(arg) = arg else {
                    anyhow::bail!("ECHO argument must be string");
                };
                Ok(RedisRequest::Echo {
                    message: arg.clone(),
                })
            }
            "get" => {
                let key = array
                    .get(1)
                    .ok_or(anyhow::anyhow!("GET argument expected"))?;
                let RedisValue::String(key) = key else {
                    anyhow::bail!("GET key must be string");
                };
                // TODO: expiration
                Ok(RedisRequest::Get { key: key.clone() })
            }
            "set" => {
                let key = array
                    .get(1)
                    .ok_or(anyhow::anyhow!("SET argument expected"))?;
                let RedisValue::String(key) = key else {
                    anyhow::bail!("SET key must be string");
                };
                let value = array
                    .get(2)
                    .ok_or(anyhow::anyhow!("SET argument expected"))?;
                let RedisValue::String(value) = value else {
                    anyhow::bail!("SET value must be string");
                };
                // TODO: expiration
                Ok(RedisRequest::Set {
                    key: key.clone(),
                    value: value.clone(),
                    expiration: None,
                })
            }
            "info" => {
                let detail = array
                    .get(1)
                    .ok_or(anyhow::anyhow!("INFO argument expected"))?;
                let RedisValue::String(detail) = detail else {
                    anyhow::bail!("INFO detail must be string");
                };
                anyhow::ensure!(detail == "replication");
                Ok(RedisRequest::Info)
            }
            _ => todo!(),
        }

        /*


        "info" => {
            anyhow::ensure!(
                array.len() == 2,
                "unexpected arguments for INFO command: {array:?}"
            );
            let detail = &array[1];
            let RedisValue::String(detail) = detail else {
                anyhow::bail!("INFO argument key must be string: {detail:?}");
            };
            anyhow::ensure!(detail == "replication");

            let output = if self.replicateof.is_some() {
                "role:slave\n".to_string()
            } else {
                format!(
                    "role:master\nmaster_replid:{}\nmaster_repl_offset:0\n",
                    hex::encode(self.replication_id)
                )
            };

            Ok(RedisValue::String(output))
        }
        */
    }
}

impl RedisResponse {
    pub fn serialize(&self) -> String {
        let response = match self {
            RedisResponse::Null => RedisValue::None,
            RedisResponse::String(arg) => RedisValue::String(arg.to_owned()),
        };
        response.serialize()
    }
}
