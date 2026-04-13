use fred::prelude::*;
use futures::stream::StreamExt;
async fn test(client: &fred::clients::RedisClient) -> anyhow::Result<()> {
    let mut stream = client.scan_buffered("jobs:*", Some(100), None);
    while let Some(res) = stream.next().await {
        let key = res?;
        let s = key.as_str().unwrap_or_default();
    }
    Ok(())
}
