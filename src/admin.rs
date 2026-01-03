use crate::config::AdminCfg;
use crate::engine::EngineHandle;
use anyhow::Result;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;

#[derive(Clone)]
struct AdminState {
    cfg: AdminCfg,
    engine: EngineHandle,
}

fn authorized(cfg: &AdminCfg, headers: &HeaderMap) -> bool {
    if !cfg.require_token {
        return true;
    }
    let token = match std::env::var("ADMIN_TOKEN") {
        Ok(t) => t,
        Err(_) => return false,
    };
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    auth == format!("Bearer {}", token)
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn readyz(State(st): State<AdminState>) -> StatusCode {
    if st.engine.ready.load(std::sync::atomic::Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn status(State(st): State<AdminState>) -> Json<serde_json::Value> {
    Json(st.engine.status_json().await)
}

async fn kill(headers: HeaderMap, State(st): State<AdminState>) -> StatusCode {
    if !authorized(&st.cfg, &headers) {
        return StatusCode::UNAUTHORIZED;
    }
    st.engine.kill.store(true, std::sync::atomic::Ordering::Relaxed);
    StatusCode::OK
}

async fn metrics() -> (StatusCode, String) {
    // Hook up real Registry in production; keep this endpoint for Prometheus scraping.
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buf = vec![];
    let _ = encoder.encode(&metric_families, &mut buf);
    (StatusCode::OK, String::from_utf8_lossy(&buf).to_string())
}

pub async fn serve(cfg: AdminCfg, engine: EngineHandle) -> Result<()> {
    let st = AdminState { cfg: cfg.clone(), engine };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .route("/kill", post(kill))
        .route("/metrics", get(metrics))
        .with_state(st);

    let addr = cfg.bind.parse()?;
    tracing::info!(bind=%cfg.bind, "admin server listening");
    axum::Server::bind(&addr).serve(app.into_make_service()).await?;
    Ok(())
}
