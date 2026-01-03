use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    DryRun,
    Paper,
    Live,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Env {
    Testnet,
    Prod,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceCfg {
    pub rest_base_url: String,
    pub ws_stream_base_url: String,
    pub ws_api_base_url: String,
    pub recv_window_ms: u64,
    pub time_sync_interval_sec: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UniverseCfg {
    pub symbols: Vec<String>,
    pub timeframe: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyCfg {
    pub ma_crossover: MaCrossoverCfg,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaCrossoverCfg {
    pub fast: usize,
    pub slow: usize,
    pub min_warmup_bars: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskCfg {
    pub max_position_notional_per_symbol: String,
    pub max_portfolio_exposure: String,
    pub risk_per_trade_pct: f64,
    pub max_daily_loss: String,
    pub max_drawdown: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionCfg {
    pub use_limit_orders: bool,
    pub max_slippage_bps: i64,
    pub cancel_stale_after_sec: u64,
    pub order_client_id_prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersistenceCfg {
    pub sqlite_path: String,
    pub snapshot_every_n_events: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReconcilerCfg {
    pub enabled: bool,
    pub interval_sec: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdminCfg {
    pub bind: String,
    pub require_token: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityCfg {
    pub log_json: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub mode: Mode,
    pub env: Env,
    pub binance: BinanceCfg,
    pub universe: UniverseCfg,
    pub strategy: StrategyCfg,
    pub risk: RiskCfg,
    pub execution: ExecutionCfg,
    pub persistence: PersistenceCfg,
    pub reconciler: ReconcilerCfg,
    pub admin: AdminCfg,
    pub observability: ObservabilityCfg,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let mut builder = config::Config::builder()
            .add_source(config::File::with_name("config").required(false))
            .add_source(config::File::with_name("config.example").required(false))
            .add_source(config::Environment::default().separator("__"));

        if let Ok(path) = std::env::var("BOT_CONFIG") {
            builder = builder.add_source(config::File::with_name(&path).required(true));
        }

        builder
            .build()
            .context("failed to build config")?
            .try_deserialize()
            .context("failed to deserialize config")
    }
}
