use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

fn usage() {
    eprintln!(
        "Usage: generate_big_object --path <file> [--target-bytes <n>] [--payload-bytes <n>] [--rows <n>] [--rows-per-flush <n>]"
    );
}

fn parse_u64(value: &str, name: &str) -> u64 {
    value
        .parse()
        .unwrap_or_else(|_| panic!("Invalid {name}: {value}"))
}

fn main() -> std::io::Result<()> {
    let mut path = PathBuf::from("big_object.json");
    let mut target_bytes: u64 = 5 * 1024 * 1024 * 1024;
    let mut payload_bytes: usize = 1024;
    let mut rows: Option<u64> = None;
    let mut rows_per_flush: usize = 8192;

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
                path = PathBuf::from(&args[i]);
            }
            "--target-bytes" => {
                i += 1;
                if i >= args.len() {
                    usage();
                    panic!("Missing value for --target-bytes");
                }
                target_bytes = parse_u64(&args[i], "target-bytes");
            }
            "--payload-bytes" => {
                i += 1;
                if i >= args.len() {
                    usage();
                    panic!("Missing value for --payload-bytes");
                }
                payload_bytes = parse_u64(&args[i], "payload-bytes") as usize;
            }
            "--rows" => {
                i += 1;
                if i >= args.len() {
                    usage();
                    panic!("Missing value for --rows");
                }
                rows = Some(parse_u64(&args[i], "rows"));
            }
            "--rows-per-flush" => {
                i += 1;
                if i >= args.len() {
                    usage();
                    panic!("Missing value for --rows-per-flush");
                }
                rows_per_flush = parse_u64(&args[i], "rows-per-flush") as usize;
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

    let file = File::create(&path)?;
    let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
    let payload = "x".repeat(payload_bytes);

    let start = Instant::now();
    let mut bytes_written: u64 = 0;
    let mut row_count: u64 = 0;

    let header =
        format!("{{\"meta\":{{\"payload_bytes\":{payload_bytes},\"schema_version\":1}},\"rows\":[");
    writer.write_all(header.as_bytes())?;
    bytes_written += header.len() as u64;

    loop {
        if let Some(limit) = rows {
            if row_count >= limit {
                break;
            }
        } else if bytes_written >= target_bytes {
            break;
        }

        if row_count > 0 {
            writer.write_all(b",")?;
            bytes_written += 1;
        }

        let id = row_count;
        let value = (row_count % 10_000) as f64 / 100.0;
        let flag = if row_count % 2 == 0 { "true" } else { "false" };
        let row = format!(
            "{{\"id\":{id},\"value\":{value:.2},\"flag\":{flag},\"payload\":\"{payload}\"}}"
        );
        writer.write_all(row.as_bytes())?;
        bytes_written += row.len() as u64;
        row_count += 1;

        if rows_per_flush > 0 && (row_count as usize) % rows_per_flush == 0 {
            writer.flush()?;
        }
    }

    writer.write_all(b"]}")?;
    bytes_written += 2;
    writer.flush()?;

    let elapsed = start.elapsed();
    println!(
        "Generated {} rows to {} ({} bytes) in {:.2?}",
        row_count,
        path.display(),
        bytes_written,
        elapsed
    );

    Ok(())
}
