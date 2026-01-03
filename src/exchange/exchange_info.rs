use anyhow::{Context, Result};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct SymbolRules {
    pub symbol: String,
    pub tick_size: Option<Decimal>,
    pub step_size: Option<Decimal>,
    pub min_qty: Option<Decimal>,
    pub max_qty: Option<Decimal>,
    pub min_notional: Option<Decimal>,
    pub max_notional: Option<Decimal>,
}

impl SymbolRules {
    pub fn round_price_down(&self, price: Decimal) -> Result<Decimal> {
        if let Some(tick) = self.tick_size {
            if tick.is_zero() {
                return Ok(price);
            }
            let steps = (price / tick).floor();
            Ok(steps * tick)
        } else {
            Ok(price)
        }
    }

    pub fn round_qty_down(&self, qty: Decimal) -> Result<Decimal> {
        if let Some(step) = self.step_size {
            if step.is_zero() {
                return Ok(qty);
            }
            let steps = (qty / step).floor();
            Ok(steps * step)
        } else {
            Ok(qty)
        }
    }

    pub fn validate(&self, price: Decimal, qty: Decimal) -> Result<()> {
        if let Some(minq) = self.min_qty {
            if qty < minq {
                anyhow::bail!("qty {} < minQty {}", qty, minq);
            }
        }
        if let Some(maxq) = self.max_qty {
            if qty > maxq {
                anyhow::bail!("qty {} > maxQty {}", qty, maxq);
            }
        }
        let notional = price * qty;
        if let Some(minn) = self.min_notional {
            if notional < minn {
                anyhow::bail!("notional {} < minNotional {}", notional, minn);
            }
        }
        if let Some(maxn) = self.max_notional {
            if notional > maxn {
                anyhow::bail!("notional {} > maxNotional {}", notional, maxn);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct ExchangeInfoResp {
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct SymbolInfo {
    symbol: String,
    filters: Vec<serde_json::Value>,
}

pub async fn fetch_symbol_rules(rest_base_url: &str, symbols: &[String]) -> Result<HashMap<String, SymbolRules>> {
    let client = Client::new();
    // Keep it simple: call /api/v3/exchangeInfo and filter in-process.
    // In production, pass `symbols` param to reduce payload.
    let url = format!("{}/v3/exchangeInfo", rest_base_url.trim_end_matches("/api"));
    let resp: ExchangeInfoResp = client.get(url).send().await?.json().await?;

    let mut out = HashMap::new();
    for s in resp.symbols {
        if !symbols.contains(&s.symbol) {
            continue;
        }
        let mut rules = SymbolRules {
            symbol: s.symbol.clone(),
            tick_size: None,
            step_size: None,
            min_qty: None,
            max_qty: None,
            min_notional: None,
            max_notional: None,
        };

        for f in s.filters {
            let t = f.get("filterType").and_then(|x| x.as_str()).unwrap_or("");
            match t {
                "PRICE_FILTER" => {
                    if let Some(ts) = f.get("tickSize").and_then(|x| x.as_str()) {
                        rules.tick_size = Some(Decimal::from_str(ts).context("tickSize parse")?);
                    }
                }
                "LOT_SIZE" => {
                    if let Some(ss) = f.get("stepSize").and_then(|x| x.as_str()) {
                        rules.step_size = Some(Decimal::from_str(ss).context("stepSize parse")?);
                    }
                    if let Some(mn) = f.get("minQty").and_then(|x| x.as_str()) {
                        rules.min_qty = Some(Decimal::from_str(mn).context("minQty parse")?);
                    }
                    if let Some(mx) = f.get("maxQty").and_then(|x| x.as_str()) {
                        rules.max_qty = Some(Decimal::from_str(mx).context("maxQty parse")?);
                    }
                }
                "MIN_NOTIONAL" => {
                    if let Some(mn) = f.get("minNotional").and_then(|x| x.as_str()) {
                        rules.min_notional = Some(Decimal::from_str(mn).context("minNotional parse")?);
                    }
                }
                "NOTIONAL" => {
                    if let Some(mn) = f.get("minNotional").and_then(|x| x.as_str()) {
                        rules.min_notional = Some(Decimal::from_str(mn).context("minNotional parse")?);
                    }
                    if let Some(mx) = f.get("maxNotional").and_then(|x| x.as_str()) {
                        rules.max_notional = Some(Decimal::from_str(mx).context("maxNotional parse")?);
                    }
                }
                _ => {}
            }
        }

        out.insert(rules.symbol.clone(), rules);
    }
    Ok(out)
}
