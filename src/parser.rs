use anyhow::Context;
use bytes::buf;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::TcpStream,
};

const SEPARATOR: &[u8; 2] = b"\r\n";
const SEPARATOR_STRING: &str = "\r\n";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisValue {
    String(String),
    File(Vec<u8>),
    Array(Vec<RedisValue>),
    None,
}

async fn read_n(reader: &mut BufReader<TcpStream>, n: usize) -> anyhow::Result<Vec<u8>> {
    let mut buffer = vec![0u8; n];
    reader
        .read_exact(&mut buffer)
        .await
        .with_context(|| format!("Cannot read {n} bytes from the input"))?;
    Ok(buffer)
}

async fn next_char(reader: &mut BufReader<TcpStream>) -> anyhow::Result<u8> {
    read_n(reader, 1)
        .await?
        .into_iter()
        .next()
        .ok_or(anyhow::anyhow!("a non empty string expected"))
}

async fn next_part(
    reader: &mut BufReader<TcpStream>,
    read_bytes: &mut usize,
) -> anyhow::Result<String> {
    let mut buffer = Vec::new();
    (*read_bytes) += reader.read_until(SEPARATOR[0], &mut buffer).await?;
    anyhow::ensure!(next_char(reader).await? == SEPARATOR[1]);
    (*read_bytes) += 1;

    // pop the trailing separator
    buffer.pop().ok_or(anyhow::anyhow!("pop trailing char"))?;
    Ok(String::from_utf8(buffer)?)
}

pub async fn parse_token(reader: &mut BufReader<TcpStream>) -> anyhow::Result<(RedisValue, usize)> {
    let mut read_bytes = 0;
    let Ok(start_letter) = next_char(reader).await else {
        return Ok((RedisValue::None, read_bytes));
    };
    read_bytes += 1;

    match start_letter {
        b'$' => {
            let length = next_part(reader, &mut read_bytes).await?.parse::<usize>()?;
            let value = read_n(reader, length).await?;
            anyhow::ensure!(&read_n(reader, 2).await? == SEPARATOR);
            read_bytes += length + 2;
            Ok((RedisValue::String(String::from_utf8(value)?), read_bytes))
        }
        b'*' => {
            let element_count = next_part(reader, &mut read_bytes).await?.parse::<usize>()?;
            let mut elements = Vec::new();
            for _ in 0..element_count {
                let (element, read) = Box::pin(parse_token(reader)).await?;
                read_bytes += read;
                elements.push(element);
            }
            Ok((RedisValue::Array(elements), read_bytes))
        }
        b'+' => Ok((
            RedisValue::String(next_part(reader, &mut read_bytes).await?),
            read_bytes,
        )),
        _ => anyhow::bail!(format!(
            "Unsupported leading character: '0x{:x}'",
            start_letter
        )),
    }
}

pub async fn parse_file(reader: &mut BufReader<TcpStream>) -> anyhow::Result<RedisValue> {
    let start_letter = next_char(reader).await?;
    anyhow::ensure!(start_letter == b'$');

    let mut read_bytes = 0;
    let length = next_part(reader, &mut read_bytes).await?.parse::<usize>()?;
    let value = read_n(reader, length).await?;
    Ok(RedisValue::String(String::from_utf8(value)?))
}

impl RedisValue {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RedisValue::String(value) => format!(
                "${}{SEPARATOR_STRING}{value}{SEPARATOR_STRING}",
                value.len()
            )
            .as_bytes()
            .to_vec(),
            RedisValue::File(content) => {
                let mut buffer = Vec::new();
                buffer.push(b'$');
                buffer.extend(content.len().to_string().as_bytes());
                buffer.extend(content);
                buffer
            }
            RedisValue::Array(array) => {
                let length = array.len();
                let mut content = array
                    .iter()
                    .map(|v| v.serialize())
                    .collect::<Vec<_>>()
                    .concat();
                let mut buffer = Vec::new();
                buffer.push(b'*');
                buffer.extend(length.to_string().as_bytes());
                buffer.extend(SEPARATOR_STRING.as_bytes());
                buffer.extend(content);
                buffer
            }
            RedisValue::None => format!("$-1{SEPARATOR_STRING}").as_bytes().to_vec(),
        }
    }
}
