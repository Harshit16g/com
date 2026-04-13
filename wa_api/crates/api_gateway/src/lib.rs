use std::sync::Arc;

use anyhow::Result;
use axum::{middleware, Router};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod auth;
mod routes;
mod services;


use shared::state::AppState;

pub async fn start_server(state: Arc<AppState>) -> Result<()> {
    let port = state.config.server_port;

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Routes that require partner auth (x-api-key)
    let authed_routes = Router::new()
        .merge(routes::message::router())
        .merge(routes::campaign::router())
        .merge(routes::instance::router())
        .merge(routes::analytics::router())
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::auth_middleware,
        ));

    // Admin routes (x-admin-key)
    let admin_routes = Router::new()
        .merge(routes::admin::router())
        .route_layer(middleware::from_fn(auth::admin_auth_middleware));

    let webhook_routes = routes::webhook::router();

    // Health route
    let health_route = Router::new().route(
        "/health",
        axum::routing::get(|| async { "OK" }),
    );

    let app = Router::new()
        .merge(health_route)
        .merge(authed_routes)
        .merge(admin_routes)
        .merge(webhook_routes)
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("Starting API server...");
    
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Listening on {}", addr);
    
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("wa_api gateway shut down gracefully.");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
}
