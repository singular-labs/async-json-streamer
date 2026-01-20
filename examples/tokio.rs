use asyncjsonstream::AsyncJsonStreamReader;
use tokio::io::BufReader;

#[tokio::main]
async fn main() -> Result<(), asyncjsonstream::AsyncJsonStreamReaderError> {
    let file = tokio::fs::File::open("data.json").await?;
    let reader = BufReader::new(file);
    let mut stream_reader = AsyncJsonStreamReader::new(reader);

    while let Some(key) = stream_reader.next_object_entry().await? {
        if key == "payload" {
            let payload = stream_reader.deserialize_object().await?;
            println!("payload keys: {}", payload.len());
        }
    }

    Ok(())
}
