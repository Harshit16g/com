use std::sync::Arc;

use anyhow::Result;
use axum::{middleware, Router};
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

mod auth;
mod routes;
mod services;

use shared::state::AppState;

pub async fn start_server(state: Arc<AppState>) -> Result<()> {
    let port = state.config.server_port;

    // Build CORS layer from config
    let cors = build_cors_layer(&state.config.cors_allowed_origins);

    // Routes that require partner auth (x-api-key + x-tenant-id)
    // Partners can: send messages, view their campaigns, check instance health, view analytics
    let partner_routes = Router::new()
        .merge(routes::message::router())
        .merge(routes::campaign::partner_router())
        .merge(routes::instance::router())
        .merge(routes::analytics::router())
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::auth_middleware,
        ));

    // Admin routes (x-admin-key)
    // Admin can: manage instances, start/pause/cancel campaigns, update tenant limits
    let admin_routes = Router::new()
        .merge(routes::admin::router())
        .merge(routes::campaign::admin_router())
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::admin_auth_middleware,
        ));

    // Webhook routes — authenticated via x-webhook-secret from evo API instances
    let webhook_routes =
        Router::new()
            .merge(routes::webhook::router())
            .route_layer(middleware::from_fn_with_state(
                Arc::clone(&state),
                auth::webhook_auth_middleware,
            ));

    // Health route (unauthenticated — used by load balancers)
    let health_route = Router::new().route("/health", axum::routing::get(|| async { "OK" }));

    let app = Router::new()
        .merge(health_route)
        .merge(partner_routes)
        .merge(admin_routes)
        .merge(webhook_routes)
        .layer(RequestBodyLimitLayer::new(2 * 1024 * 1024)) // 2MB body limit
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

/// Build CORS layer from configured allowed origins.
fn build_cors_layer(origins: &[String]) -> CorsLayer {
    if origins.len() == 1 && origins[0] == "*" {
        CorsLayer::new()
            .allow_origin(AllowOrigin::any())
            .allow_methods(AllowMethods::any())
            .allow_headers(AllowHeaders::any())
    } else {
        let parsed: Vec<_> = origins.iter().filter_map(|o| o.parse().ok()).collect();
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(parsed))
            .allow_methods(AllowMethods::any())
            .allow_headers(AllowHeaders::any())
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
}
