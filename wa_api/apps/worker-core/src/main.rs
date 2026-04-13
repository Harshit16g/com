use std::sync::Arc;
use std::time::Duration;
use tokio::signal;

#[tokio::main]
async fn main() {
    let state = Arc::new(shared::state::init().await);

    // Spawn scheduler task
    tokio::spawn(scheduler::start(state.clone()));

    // Start worker loop with internal resilience and retry mechanism
    tokio::spawn(async move {
        loop {
            if let Err(e) = worker::start(state.clone()).await {
                eprintln!("Worker crashed: {:?}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    });

    signal::ctrl_c().await.unwrap();
    println!("Worker shutting down");
}
