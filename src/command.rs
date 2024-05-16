use std::time::Duration;

use anyhow::Ok;

use crate::parser::RedisValue;

#[derive(Debug, Clone)]
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
    Del {
        key: String,
    },
    Info,
    ReplConf {
        arg: String,
        value: String,
    },
    Wait {
        replicas: u64,
        timeout: Duration,
    },
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
                let mut expiration = None;
                if let Some(RedisValue::String(e)) = array.get(4) {
                    // TODO: check PX
                    expiration = Some(Duration::from_millis(e.parse::<u64>().unwrap()));
                }
                // TODO: expiration
                Ok(RedisRequest::Set {
                    key: key.clone(),
                    value: value.clone(),
                    expiration,
                })
            }
            "del" => {
                let key = array
                    .get(1)
                    .ok_or(anyhow::anyhow!("DEL argument expected"))?;
                let RedisValue::String(key) = key else {
                    anyhow::bail!("DEL key must be string");
                };
                Ok(RedisRequest::Del { key: key.clone() })
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
            "replconf" => {
                let arg = array
                    .get(1)
                    .ok_or(anyhow::anyhow!("REPLCONF argument 1 expected"))?;
                let RedisValue::String(arg) = arg else {
                    anyhow::bail!("REPLCONF arg must be string");
                };
                let value = array
                    .get(2)
                    .ok_or(anyhow::anyhow!("REPLCONF argument 2 expected"))?;
                let RedisValue::String(value) = value else {
                    anyhow::bail!("REPLCONF arg must be string");
                };
                Ok(RedisRequest::ReplConf {
                    arg: arg.clone(),
                    value: value.to_string(),
                })
            }
            "wait" => {
                let replicas = array
                    .get(1)
                    .ok_or(anyhow::anyhow!("WAIT argument 1 expected"))?;
                let RedisValue::String(replicas) = replicas else {
                    anyhow::bail!("WAIT arg must be string");
                };
                let timeout = array
                    .get(2)
                    .ok_or(anyhow::anyhow!("WAIT argument 2 expected"))?;
                let RedisValue::String(timeout) = timeout else {
                    anyhow::bail!("WAIT arg must be string");
                };

                Ok(RedisRequest::Wait {
                    replicas: replicas.parse().unwrap(),
                    timeout: Duration::from_millis(timeout.parse().unwrap()),
                })
            }
            command => anyhow::bail!("Unknown command: {command}"),
        }
    }
}

impl RedisResponse {
    pub fn serialize(&self) -> Vec<u8> {
        let response = match self {
            RedisResponse::Null => RedisValue::None,
            RedisResponse::String(arg) => RedisValue::String(arg.to_owned()),
        };
        response.serialize()
    }
}

impl RedisRequest {
    pub fn to_value(&self) -> RedisValue {
        match self {
            RedisRequest::Set { key, value, .. } => {
                ["SET", key.as_str(), value.as_str()][..].into()
            }
            RedisRequest::Del { key } => ["DEL", key][..].into(),
            _ => todo!(),
        }
    }
}
