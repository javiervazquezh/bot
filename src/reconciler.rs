use crate::event_log::Event;
use crate::exchange::binance_rest::BinanceRest;
use crate::persistence::SqliteStore;
use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone)]
pub struct Reconciler {
    rest: BinanceRest,
    store: Arc<SqliteStore>,
    symbols: Vec<String>,
    client_id_prefix: String,
    state: Arc<RwLock<crate::engine::EngineState>>,
}

impl Reconciler {
    pub fn new(
        rest: BinanceRest,
        store: Arc<SqliteStore>,
        symbols: Vec<String>,
        client_id_prefix: String,
        state: Arc<RwLock<crate::engine::EngineState>>,
    ) -> Self {
        Self { rest, store, symbols, client_id_prefix, state }
    }

    pub async fn run_once(&self, now_ms: i64) -> Result<()> {
        let run_id = Uuid::new_v4().to_string();
        let _ = self.store.append_event(&Event::ReconciliationStarted { run_id: run_id.clone() }, Some(format!("recon:start:{}", run_id))).await?;

        // 1) openOrders per symbol (avoid weight explosion) 
        let mut exchange_open: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for sym in &self.symbols {
            match self.rest.open_orders(sym, now_ms).await {
                Ok(orders) => {
                    exchange_open.insert(sym.clone(), orders);
                }
                Err(e) => {
                    let _ = self.store.append_event(&Event::Error { where_: "reconciler.open_orders".into(), error: format!("{:?}", e) }, None).await?;
                }
            }
        }

        // 2) apply open order updates (only manage our bot orders by client id prefix)
        for (sym, orders) in exchange_open.iter() {
            for o in orders {
                let client_id = o.get("clientOrderId").and_then(|v| v.as_str()).unwrap_or("").to_string();
                if !client_id.starts_with(&self.client_id_prefix) {
                    continue;
                }
                let status = o.get("status").and_then(|v| v.as_str()).unwrap_or("UNKNOWN").to_string();
                let executed_qty = o.get("executedQty").and_then(|v| v.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO);
                let cum_quote = o.get("cummulativeQuoteQty").and_then(|v| v.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO);
                let order_id = o.get("orderId").and_then(|v| v.as_i64());

                let ev = Event::OrderUpdate {
                    client_order_id: client_id.clone(),
                    symbol: sym.clone(),
                    status: status.clone(),
                    executed_qty,
                    cumulative_quote_qty: cum_quote,
                    exchange_order_id: order_id,
                    update_ts_ms: now_ms,
                    source: "reconciler".into(),
                };

                // Dedup key based on (client, status, executed_qty, cum_quote)
                let dedup = format!("recon:order:{}:{}:{}:{}", client_id, status, executed_qty, cum_quote);
                if let Some(_) = self.store.append_event(&ev, Some(dedup)).await? {
                    self.store.upsert_order_row(
                        client_id, sym.clone(), status, order_id, executed_qty.to_string(), cum_quote.to_string(), now_ms
                    ).await?;
                }
            }
        }

        // 3) For locally-tracked open orders missing on exchange, query /api/v3/order by clientOrderId 
        let local = { self.state.read().await.open_orders.clone() };
        for (client_id, tracked) in local {
            if !client_id.starts_with(&self.client_id_prefix) {
                continue;
            }
            let sym = tracked.symbol.clone();
            let still_open_on_exchange = exchange_open
                .get(&sym)
                .map(|os| os.iter().any(|o| o.get("clientOrderId").and_then(|v| v.as_str()) == Some(&client_id)))
                .unwrap_or(false);

            if still_open_on_exchange {
                continue;
            }

            // Query authoritative status
            match self.rest.query_order_by_client_id(&sym, &client_id, now_ms).await {
                Ok(o) => {
                    let status = o.get("status").and_then(|v| v.as_str()).unwrap_or("UNKNOWN").to_string();
                    let executed_qty = o.get("executedQty").and_then(|v| v.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO);
                    let cum_quote = o.get("cummulativeQuoteQty").and_then(|v| v.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO);
                    let order_id = o.get("orderId").and_then(|v| v.as_i64());

                    let ev = Event::OrderUpdate {
                        client_order_id: client_id.clone(),
                        symbol: sym.clone(),
                        status: status.clone(),
                        executed_qty,
                        cumulative_quote_qty: cum_quote,
                        exchange_order_id: order_id,
                        update_ts_ms: now_ms,
                        source: "reconciler_query".into(),
                    };

                    let dedup = format!("recon:q:{}:{}:{}:{}", client_id, status, executed_qty, cum_quote);
                    if let Some(_) = self.store.append_event(&ev, Some(dedup)).await? {
                        self.store.upsert_order_row(
                            client_id, sym, status, order_id, executed_qty.to_string(), cum_quote.to_string(), now_ms
                        ).await?;
                    }
                }
                Err(e) => {
                    let _ = self.store.append_event(&Event::Error { where_: "reconciler.query_order".into(), error: format!("{:?}", e) }, None).await?;
                }
            }
        }

        // 4) Account snapshot (balances) 
        match self.rest.get_account(now_ms).await {
            Ok(v) => {
                let mut balances = HashMap::new();
                if let Some(arr) = v.get("balances").and_then(|b| b.as_array()) {
                    for b in arr {
                        let asset = b.get("asset").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        let free = b.get("free").and_then(|x| x.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO);
                        let locked = b.get("locked").and_then(|x| x.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO);
                        if free.is_zero() && locked.is_zero() {
                            continue;
                        }
                        balances.insert(asset.clone(), crate::types::Balance { asset, free, locked });
                    }
                }
                let snap = crate::types::AccountSnapshot { ts_ms: now_ms, balances };
                let _ = self.store.append_event(&Event::AccountSnapshot(snap), Some(format!("acct:{}", now_ms))).await?;
            }
            Err(e) => {
                let _ = self.store.append_event(&Event::Error { where_: "reconciler.account".into(), error: format!("{:?}", e) }, None).await?;
            }
        }

        let _ = self.store.append_event(&Event::ReconciliationFinished {
            run_id: run_id.clone(),
            ok: true,
            notes: "done".into()
        }, Some(format!("recon:finish:{}", run_id))).await?;

        Ok(())
    }
}
