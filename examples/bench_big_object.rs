use asyncjsonstream::{AsyncJsonStreamReader, JsonToken};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::BufReader;

fn usage() {
    eprintln!(
        "Usage: bench_big_object --path <file> [--mode serde|async|async-light|both] [--repeat <n>]"
    );
}

fn parse_u64(value: &str, name: &str) -> u64 {
    value
        .parse()
        .unwrap_or_else(|_| panic!("Invalid {name}: {value}"))
}

fn parse_mode(value: &str) -> Mode {
    match value {
        "serde" => Mode::Serde,
        "async" => Mode::Async,
        "async-light" => Mode::AsyncLight,
        "both" => Mode::Both,
        _ => panic!("Invalid mode: {value}"),
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Serde,
    Async,
    AsyncLight,
    Both,
}

fn bench_serde(path: &PathBuf) -> Result<(u64, u64, u128), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let data = fs::read(path)?;
    let parsed: Value = serde_json::from_slice(&data)?;
    let rows = parsed
        .get("rows")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "missing rows array".to_string())?;

    let mut checksum: u64 = 0;
    for row in rows {
        if let Some(id) = row.get("id").and_then(|v| v.as_u64()) {
            checksum = checksum.wrapping_add(id);
        }
    }

    Ok((rows.len() as u64, checksum, start.elapsed().as_millis()))
}

async fn bench_async(path: &PathBuf) -> Result<(u64, u64, u128), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let file = tokio::fs::File::open(path).await?;
    let reader = BufReader::new(file);
    let mut reader = AsyncJsonStreamReader::new(reader);

    let mut rows: u64 = 0;
    let mut checksum: u64 = 0;

    while let Some(key) = reader.next_object_entry().await? {
        if key == "rows" {
            while reader.start_array_item().await? {
                let obj = reader.deserialize_object().await?;
                if let Some(id) = obj.get("id").and_then(|v| v.as_u64()) {
                    checksum = checksum.wrapping_add(id);
                }
                rows += 1;
            }
        }
    }

    Ok((rows, checksum, start.elapsed().as_millis()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Container {
    Object,
    Array,
}

async fn consume_value<R>(
    reader: &mut AsyncJsonStreamReader<R>,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let token = reader
        .next_token()
        .await?
        .ok_or_else(|| "unexpected EOF while consuming value".to_string())?;
    consume_value_from_token(reader, token).await
}

async fn consume_value_from_token<R>(
    reader: &mut AsyncJsonStreamReader<R>,
    mut token: JsonToken,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut stack: Vec<Container> = Vec::new();

    loop {
        match token {
            JsonToken::StartObject => stack.push(Container::Object),
            JsonToken::StartArray => stack.push(Container::Array),
            JsonToken::EndObject => match stack.pop() {
                Some(Container::Object) => {}
                _ => return Err("unexpected EndObject".into()),
            },
            JsonToken::EndArray => match stack.pop() {
                Some(Container::Array) => {}
                _ => return Err("unexpected EndArray".into()),
            },
            JsonToken::EndObjectOrListItem => {}
            JsonToken::Key(_) => {}
            JsonToken::String(_)
            | JsonToken::Number(_)
            | JsonToken::Boolean(_)
            | JsonToken::Null => {}
        }

        if stack.is_empty() {
            match token {
                JsonToken::String(_)
                | JsonToken::Number(_)
                | JsonToken::Boolean(_)
                | JsonToken::Null
                | JsonToken::EndObject
                | JsonToken::EndArray => break,
                _ => {}
            }
        }

        token = reader
            .next_token()
            .await?
            .ok_or_else(|| "unexpected EOF while consuming value".to_string())?;
    }

    Ok(())
}

async fn bench_async_light(path: &PathBuf) -> Result<(u64, u64, u128), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let file = tokio::fs::File::open(path).await?;
    let reader = BufReader::new(file);
    let mut reader = AsyncJsonStreamReader::new(reader);

    let mut rows: u64 = 0;
    let mut checksum: u64 = 0;

    let token = reader
        .next_token()
        .await?
        .ok_or_else(|| "empty input".to_string())?;
    if token != JsonToken::StartObject {
        return Err("expected root object".into());
    }

    loop {
        let token = reader
            .next_token()
            .await?
            .ok_or_else(|| "unexpected EOF in root object".to_string())?;
        match token {
            JsonToken::Key(key) => {
                if key == "rows" {
                    let next = reader
                        .next_token()
                        .await?
                        .ok_or_else(|| "unexpected EOF before rows array".to_string())?;
                    if next != JsonToken::StartArray {
                        return Err("expected rows array".into());
                    }

                    loop {
                        let token = reader
                            .next_token()
                            .await?
                            .ok_or_else(|| "unexpected EOF in rows array".to_string())?;
                        match token {
                            JsonToken::EndArray => break,
                            JsonToken::EndObjectOrListItem => continue,
                            JsonToken::StartObject => loop {
                                let token = reader.next_token().await?.ok_or_else(|| {
                                    "unexpected EOF while reading row".to_string()
                                })?;
                                match token {
                                    JsonToken::EndObject => {
                                        rows += 1;
                                        break;
                                    }
                                    JsonToken::EndObjectOrListItem => continue,
                                    JsonToken::Key(field) => {
                                        if field == "id" {
                                            let token =
                                                reader.next_token().await?.ok_or_else(|| {
                                                    "unexpected EOF reading id".to_string()
                                                })?;
                                            match token {
                                                JsonToken::Number(n) => {
                                                    if let Ok(id) = n.parse::<u64>() {
                                                        checksum = checksum.wrapping_add(id);
                                                    }
                                                }
                                                JsonToken::String(s) => {
                                                    if let Ok(id) = s.parse::<u64>() {
                                                        checksum = checksum.wrapping_add(id);
                                                    }
                                                }
                                                JsonToken::Null => {}
                                                other => {
                                                    return Err(format!(
                                                        "unexpected id token: {other:?}"
                                                    )
                                                    .into());
                                                }
                                            }
                                        } else {
                                            consume_value(&mut reader).await?;
                                        }
                                    }
                                    other => {
                                        return Err(
                                            format!("unexpected token in row: {other:?}").into()
                                        );
                                    }
                                }
                            },
                            other => consume_value_from_token(&mut reader, other).await?,
                        }
                    }
                } else {
                    consume_value(&mut reader).await?;
                }
            }
            JsonToken::EndObject => break,
            JsonToken::EndObjectOrListItem => continue,
            other => {
                return Err(format!("unexpected token in root: {other:?}").into());
            }
        }
    }

    Ok((rows, checksum, start.elapsed().as_millis()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut path: Option<PathBuf> = None;
    let mut mode = Mode::Both;
    let mut repeat: u64 = 1;

    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--path" => {
                i += 1;
                if i >= args.len() {
                    usage();
                    panic!("Missing value for --path");
                }
                path = Some(PathBuf::from(&args[i]));
            }
            "--mode" => {
                i += 1;
                if i >= args.len() {
                    usage();
                    panic!("Missing value for --mode");
                }
                mode = parse_mode(&args[i]);
            }
            "--repeat" => {
                i += 1;
                if i >= args.len() {
                    usage();
                    panic!("Missing value for --repeat");
                }
                repeat = parse_u64(&args[i], "repeat");
            }
            "--help" | "-h" => {
                usage();
                return Ok(());
            }
            other => {
                usage();
                panic!("Unknown argument: {other}");
            }
        }
        i += 1;
    }

    let path = path.unwrap_or_else(|| {
        usage();
        panic!("--path is required");
    });

    for run in 1..=repeat {
        if matches!(mode, Mode::Serde | Mode::Both) {
            let (rows, checksum, ms) = bench_serde(&path)?;
            println!("run={run} mode=serde rows={rows} checksum={checksum} elapsed_ms={ms}");
        }
        if matches!(mode, Mode::Async | Mode::Both) {
            let (rows, checksum, ms) = bench_async(&path).await?;
            println!("run={run} mode=async rows={rows} checksum={checksum} elapsed_ms={ms}");
        }
        if matches!(mode, Mode::AsyncLight) {
            let (rows, checksum, ms) = bench_async_light(&path).await?;
            println!("run={run} mode=async-light rows={rows} checksum={checksum} elapsed_ms={ms}");
        }
    }

    Ok(())
}
