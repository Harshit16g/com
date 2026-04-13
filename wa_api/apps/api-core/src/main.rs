use std::sync::Arc;
use tokio::signal;

#[tokio::main]
async fn main() {
    let state = Arc::new(shared::state::init().await);

    let mut handles = vec![];

    handles.push(tokio::spawn(api_gateway::start_server(state.clone())));
    handles.push(tokio::spawn(pool_manager::start(state.clone())));
    handles.push(tokio::spawn(health_monitor::start(state.clone())));

    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Shutdown signal received");
        }
    }

    println!("Shutting down tasks...");
}
