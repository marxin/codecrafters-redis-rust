use std::pin::Pin;

use anyhow::Context;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

const SEPARATOR: &[u8; 2] = b"\r\n";
const SEPARATOR_STRING: &str = "\r\n";

#[derive(Debug)]
pub enum RedisValue {
    String(String),
    Array(Vec<RedisValue>),
    None,
}

async fn read_n<R: AsyncBufRead>(reader: &mut Pin<&mut R>, n: usize) -> anyhow::Result<Vec<u8>> {
    let mut buffer = vec![0u8; n];
    reader
        .read_exact(&mut buffer)
        .await
        .with_context(|| format!("Cannot read {n} bytes from the input"))?;
    Ok(buffer)
}

async fn next_char<R: AsyncBufRead>(reader: &mut Pin<&mut R>) -> anyhow::Result<u8> {
    read_n(reader, 1)
        .await?
        .into_iter()
        .next()
        .ok_or(anyhow::anyhow!("a non empty string expected"))
}

async fn next_part<R: AsyncBufRead>(reader: &mut Pin<&mut R>) -> anyhow::Result<String> {
    let mut buffer = Vec::new();
    reader.read_until(SEPARATOR[0], &mut buffer).await?;
    anyhow::ensure!(next_char(reader).await? == SEPARATOR[1]);

    // pop the trailing separator
    buffer.pop().ok_or(anyhow::anyhow!("pop trailing char"))?;
    Ok(String::from_utf8(buffer)?)
}

pub async fn parse_token<R: AsyncBufRead>(reader: &mut Pin<&mut R>) -> anyhow::Result<RedisValue> {
    let Ok(start_letter) = next_char(reader).await else {
        return Ok(RedisValue::None);
    };

    match start_letter {
        b'$' => {
            let length = next_part(reader).await?.parse::<usize>()?;
            let value = read_n(reader, length).await?;
            anyhow::ensure!(&read_n(reader, 2).await? == SEPARATOR);
            Ok(RedisValue::String(String::from_utf8(value)?))
        }
        b'*' => {
            let element_count = next_part(reader).await?.parse::<usize>()?;
            let mut elements = Vec::new();
            for _ in 0..element_count {
                elements.push(Box::pin(parse_token(reader)).await?);
            }
            Ok(RedisValue::Array(elements))
        }
        _ => anyhow::bail!(format!(
            "Unsupported leading character: '{}'",
            start_letter as char
        )),
    }
}

impl RedisValue {
    pub fn serialize(&self) -> String {
        match self {
            RedisValue::String(value) => {
                format!(
                    "${}{SEPARATOR_STRING}{value}{SEPARATOR_STRING}",
                    value.len()
                )
            }
            RedisValue::Array(array) => {
                let length = array.len();
                let content = array
                    .iter()
                    .map(|v| v.serialize())
                    .collect::<Vec<_>>()
                    .join("");
                format!("*{length}{SEPARATOR_STRING}{content}{SEPARATOR_STRING}")
            }
            RedisValue::None => format!("-1{SEPARATOR_STRING}"),
        }
    }
}
