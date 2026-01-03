use crate::types::{AccountSnapshot, Candle, OrderIntent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Event {
    EngineStarted { version: String },

    MarketCandleClosed(Candle),

    StrategySignal {
        symbol: String,
        side: String,
        strength: f64,
        reason: String,
    },

    OrderIntentCreated(OrderIntent),
    OrderIntentRejected { client_order_id: String, reason: String },

    OrderSubmitAttempted { client_order_id: String, attempt: u32 },
    OrderSubmitted { client_order_id: String, exchange_order_id: Option<i64> },
    OrderSubmitFailed { client_order_id: String, error: String },

    OrderUpdate {
        client_order_id: String,
        symbol: String,
        status: String,
        executed_qty: Decimal,
        cumulative_quote_qty: Decimal,
        exchange_order_id: Option<i64>,
        update_ts_ms: i64,
        source: String, // "user_stream" | "reconciler"
    },

    Fill {
        client_order_id: String,
        symbol: String,
        trade_id: Option<i64>,
        price: Decimal,
        qty: Decimal,
        commission: Option<Decimal>,
        commission_asset: Option<String>,
        ts_ms: i64,
    },

    AccountSnapshot(AccountSnapshot),

    ReconciliationStarted { run_id: String },
    ReconciliationFinished { run_id: String, ok: bool, notes: String },

    KillSwitchEngaged { reason: String },
    KillSwitchCleared { reason: String },

    Error { where_: String, error: String },
}

impl Event {
    pub fn kind(&self) -> &'static str {
        match self {
            Event::EngineStarted { .. } => "engine_started",
            Event::MarketCandleClosed(_) => "market_candle_closed",
            Event::StrategySignal { .. } => "strategy_signal",
            Event::OrderIntentCreated(_) => "order_intent_created",
            Event::OrderIntentRejected { .. } => "order_intent_rejected",
            Event::OrderSubmitAttempted { .. } => "order_submit_attempted",
            Event::OrderSubmitted { .. } => "order_submitted",
            Event::OrderSubmitFailed { .. } => "order_submit_failed",
            Event::OrderUpdate { .. } => "order_update",
            Event::Fill { .. } => "fill",
            Event::AccountSnapshot(_) => "account_snapshot",
            Event::ReconciliationStarted { .. } => "reconciliation_started",
            Event::ReconciliationFinished { .. } => "reconciliation_finished",
            Event::KillSwitchEngaged { .. } => "kill_switch_engaged",
            Event::KillSwitchCleared { .. } => "kill_switch_cleared",
            Event::Error { .. } => "error",
        }
    }
}
