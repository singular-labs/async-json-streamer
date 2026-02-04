# asyncjsonstream

Async JSON stream reader for selective parsing of large payloads. This is the
standalone home of Extract's `AsyncJsonStreamReader` implementation.

[![crates.io](https://img.shields.io/crates/v/asyncjsonstream.svg)](https://crates.io/crates/asyncjsonstream)
[![docs.rs](https://docs.rs/asyncjsonstream/badge.svg)](https://docs.rs/asyncjsonstream)
[![license](https://img.shields.io/crates/l/asyncjsonstream.svg)](LICENSE-MIT)
[![ci](https://github.com/singular-labs/async-json-streamer/actions/workflows/ci.yml/badge.svg)](https://github.com/singular-labs/async-json-streamer/actions/workflows/ci.yml)

## Why asyncjsonstream

- Stream through large JSON without deserializing the full payload.
- Selectively read keys, values, and arrays using a tokenized reader.
- Handles chunk boundaries and escaped strings correctly.
- Built on Tokio `AsyncRead`.
- No unsafe code.

## Install

```bash
cargo add asyncjsonstream
```

## Quick start

```rust
use asyncjsonstream::AsyncJsonStreamReader;
use std::io::Cursor;

#[tokio::main]
async fn main() -> Result<(), asyncjsonstream::AsyncJsonStreamReaderError> {
    let data = r#"{"status":"success","results":[{"id":1},{"id":2}]}"#;
    let mut reader = AsyncJsonStreamReader::new(Cursor::new(data.as_bytes().to_vec()));

    while let Some(key) = reader.next_object_entry().await? {
        match key.as_str() {
            "status" => {
                let status = reader.read_string().await?;
                println!("status={status}");
            }
            "results" => {
                while reader.start_array_item().await? {
                    let obj = reader.deserialize_object().await?;
                    println!("id={}", obj["id"]);
                }
            }
            _ => {}
        }
    }

    Ok(())
}
```

## Common patterns

- Read object entries with `next_object_entry`.
- Skip values by calling `next_object_entry` again without consuming the value.
- Stream arrays with `start_array_item`.
- Parse string/number/bool with `read_string`, `read_number`, `read_boolean`.
- Deserialize a sub-object with `deserialize_object`.

## Error handling

All fallible operations return `AsyncJsonStreamReaderError`:

- `Io` for reader failures
- `JsonError` for malformed JSON
- `UnexpectedToken` when the stream doesn't match the expected structure

## MSRV

Minimum supported Rust version is **1.74**.

## Benchmark (Serde vs asyncjsonstream)

The examples folder includes a generator and benchmark for a single large JSON object with a
`rows` array. This comparison highlights the memory savings when you **stream and skip** large
fields instead of deserializing full objects.

### Generate a 5GB fixture

```bash
cargo run --release --example generate_big_object -- \
  --path /tmp/big_object.json \
  --target-bytes 5368709120 \
  --payload-bytes 1024
```

### Run benchmarks (macOS)

```bash
/usr/bin/time -l cargo run --release --example bench_big_object -- \
  --path /tmp/big_object.json --mode async

/usr/bin/time -l cargo run --release --example bench_big_object -- \
  --path /tmp/big_object.json --mode async-light

/usr/bin/time -l cargo run --release --example bench_big_object -- \
  --path /tmp/big_object.json --mode serde
```

`async` deserializes each row into a `serde_json::Value` (higher memory). `async-light` only
reads `id` and skips other fields using tokens (low memory).

### Results (MacBook Pro, macOS, 5GB file, payload 1KB)

| Mode         | Rows     | Elapsed (ms) | Max RSS (bytes) | Peak footprint (bytes) |
|--------------|----------|--------------|-----------------|------------------------|
| async        | 4,979,433 | 7,432        | 3,320,676,352   | 5,382,197,400          |
| async-light  | 4,979,433 | 10,340       | 2,916,352       | 2,146,616              |
| serde        | 4,979,433 | 6,662        | 10,902,372,352  | 14,253,713,704         |

Checksums matched across modes, confirming identical `id` aggregation.

## License

Licensed under either of:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)

at your option.
