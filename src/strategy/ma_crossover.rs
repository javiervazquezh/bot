use crate::config::MaCrossoverCfg;
use crate::types::{Candle, OrderIntent, Side};
use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub struct MaCrossover {
    cfg: MaCrossoverCfg,
    closes: std::collections::HashMap<String, VecDeque<Decimal>>,
}

impl MaCrossover {
    pub fn new(cfg: MaCrossoverCfg) -> Self {
        Self {
            cfg,
            closes: Default::default(),
        }
    }

    fn sma(vals: &VecDeque<Decimal>, n: usize) -> Option<Decimal> {
        if vals.len() < n {
            return None;
        }
        let sum = vals.iter().rev().take(n).fold(Decimal::ZERO, |a, x| a + *x);
        Some(sum / Decimal::from(n as i64))
    }
}

impl crate::strategy::Strategy for MaCrossover {
    fn on_candle(&mut self, candle: &Candle) -> Result<Vec<OrderIntent>> {
        let q = self.closes.entry(candle.symbol.clone()).or_default();
        q.push_back(candle.close);
        while q.len() > (self.cfg.slow + 5) {
            q.pop_front();
        }

        if q.len() < self.cfg.min_warmup_bars {
            return Ok(vec![]);
        }

        let fast = Self::sma(q, self.cfg.fast);
        let slow = Self::sma(q, self.cfg.slow);
        if fast.is_none() || slow.is_none() {
            return Ok(vec![]);
        }

        let fast = fast.unwrap();
        let slow = slow.unwrap();

        let mut intents = vec![];
        if fast > slow {
            intents.push(OrderIntent {
                client_order_id: format!("{}-{}", "bot", Uuid::new_v4()),
                symbol: candle.symbol.clone(),
                side: Side::Buy,
                qty: Decimal::from(1), // placeholder sizing; real sizing done in risk/execution
                limit_price: Some(candle.close),
                created_ts_ms: now_ms(),
            });
        } else if fast < slow {
            intents.push(OrderIntent {
                client_order_id: format!("{}-{}", "bot", Uuid::new_v4()),
                symbol: candle.symbol.clone(),
                side: Side::Sell,
                qty: Decimal::from(1),
                limit_price: Some(candle.close),
                created_ts_ms: now_ms(),
            });
        }

        Ok(intents)
    }
}
