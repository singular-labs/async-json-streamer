use asyncjsonstream::AsyncJsonStreamReader;
use std::io::Cursor;

#[tokio::main]
async fn main() -> Result<(), asyncjsonstream::AsyncJsonStreamReaderError> {
    let data = r#"{"status":"ok","results":[{"id":1},{"id":2}]}"#;
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
