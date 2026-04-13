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
mod state;

use state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false)
                .pretty(),
        )
        .init();

    let config = shared::config::AppConfig::from_env()?;
    let port = config.server_port;

    let state = Arc::new(AppState::new(config).await?);

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

    // Webhook endpoint — no auth (Evolution API calls this)
    let webhook_routes = routes::webhook::router();

    let app = Router::new()
        .merge(authed_routes)
        .merge(admin_routes)
        .merge(webhook_routes)
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("wa_api gateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
