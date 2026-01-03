use crate::config::RiskCfg;
use crate::types::{OrderIntent, Side};
use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct RiskManager {
    cfg: RiskCfg,
    max_pos_notional: Decimal,
    max_portfolio_exposure: Decimal,
    max_daily_loss: Decimal,
}

#[derive(Debug, Clone, Default)]
pub struct RiskState {
    pub daily_realized_pnl: Decimal,
    pub peak_equity: Option<Decimal>,
    pub equity: Option<Decimal>,
}

impl RiskManager {
    pub fn new(cfg: RiskCfg) -> Result<Self> {
        Ok(Self {
            max_pos_notional: Decimal::from_str(&cfg.max_position_notional_per_symbol)?,
            max_portfolio_exposure: Decimal::from_str(&cfg.max_portfolio_exposure)?,
            max_daily_loss: Decimal::from_str(&cfg.max_daily_loss)?,
            cfg,
        })
    }

    pub fn pre_trade_check(
        &self,
        intent: &OrderIntent,
        mid_price: Decimal,
        current_symbol_exposure: Decimal,
        current_portfolio_exposure: Decimal,
        state: &RiskState,
    ) -> Result<()> {
        let notional = mid_price * intent.qty;

        if current_symbol_exposure + notional > self.max_pos_notional {
            anyhow::bail!("symbol exposure limit exceeded");
        }
        if current_portfolio_exposure + notional > self.max_portfolio_exposure {
            anyhow::bail!("portfolio exposure limit exceeded");
        }

        // Daily loss circuit breaker
        if state.daily_realized_pnl < Decimal::ZERO && (-state.daily_realized_pnl) >= self.max_daily_loss {
            anyhow::bail!("max daily loss exceeded");
        }

        // Optional: side-specific checks, position limits, etc.
        let _ = match intent.side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        Ok(())
    }
}
