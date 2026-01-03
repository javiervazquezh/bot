pub mod ma_crossover;

use crate::types::{Candle, OrderIntent};
use anyhow::Result;

pub trait Strategy: Send + Sync {
    fn on_candle(&mut self, candle: &Candle) -> Result<Vec<OrderIntent>>;
}
