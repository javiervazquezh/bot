mod admin;
mod config;
mod engine;
mod event_log;
mod exchange;
mod observability;
mod persistence;
mod risk;
mod strategy;
mod types;

use anyhow::Result;
use config::AppConfig;
use observability::init_tracing;
use persistence::SqliteStore;
use std::sync::Arc;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = AppConfig::load()?;
    init_tracing(&cfg.observability)?;

    let store = Arc::new(SqliteStore::new(&cfg.persistence.sqlite_path).await?);
    store.init_schema().await?;

    let mut engine = engine::Engine::new(cfg.clone(), store.clone()).await?;

    // Admin API (kill switch / status / metrics)
    let admin_handle = {
        let admin_cfg = cfg.admin.clone();
        let engine_handle = engine.handle();
        tokio::spawn(async move {
            if let Err(e) = admin::serve(admin_cfg, engine_handle).await {
                tracing::error!(error=?e, "admin server failed");
            }
        })
    };

    // Start engine tasks
    let engine_handle = tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            tracing::error!(error=?e, "engine terminated with error");
        }
    });

    // Graceful shutdown on SIGINT/SIGTERM
    tokio::select! {
        _ = signal::ctrl_c() => {
            tracing::warn!("ctrl_c received; initiating shutdown");
        }
        _ = &mut engine_handle => {
            tracing::warn!("engine task ended; shutting down");
        }
    }

    // Best-effort shutdown
    // (Admin server will exit when process exits.)
    let _ = admin_handle.abort();
    Ok(())
}
