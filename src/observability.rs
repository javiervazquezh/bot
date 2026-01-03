use crate::config::ObservabilityCfg;
use tracing_subscriber::EnvFilter;

pub fn init_tracing(cfg: &ObservabilityCfg) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,binance_bot=debug"));

    if cfg.log_json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .init();
    }
    Ok(())
}
