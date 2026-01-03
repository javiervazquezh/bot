use crate::config::{AppConfig, Mode};
use crate::event_log::Event;
use crate::exchange::binance_rest::BinanceRest;
use crate::exchange::binance_ws_api::{WsApiClient, WsApiHandle};
use crate::exchange::exchange_info::fetch_symbol_rules;
use crate::exchange::signer::Signer;
use crate::persistence::SqliteStore;
use crate::reconciler::Reconciler;
use crate::risk::{RiskManager, RiskState};
use crate::strategy::ma_crossover::MaCrossover;
use crate::strategy::Strategy;
use anyhow::{Context, Result};
use lru::LruCache;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackedOrder {
    pub client_order_id: String,
    pub symbol: String,
    pub status: String,
    pub exchange_order_id: Option<i64>,
    pub executed_qty: Decimal,
    pub cumulative_quote_qty: Decimal,
    pub last_update_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineState {
    pub kill_switch: bool,
    pub mode: Mode,
    pub last_event_id: i64,
    pub open_orders: HashMap<String, TrackedOrder>,
    pub risk: RiskState,
}

impl EngineState {
    pub fn new(mode: Mode) -> Self {
        Self {
            kill_switch: false,
            mode,
            last_event_id: 0,
            open_orders: HashMap::new(),
            risk: RiskState::default(),
        }
    }

    pub fn apply(&mut self, ev: &Event) {
        match ev {
            Event::KillSwitchEngaged { .. } => self.kill_switch = true,
            Event::KillSwitchCleared { .. } => self.kill_switch = false,
            Event::OrderUpdate { client_order_id, symbol, status, executed_qty, cumulative_quote_qty, exchange_order_id, update_ts_ms, .. } => {
                self.open_orders.insert(client_order_id.clone(), TrackedOrder {
                    client_order_id: client_order_id.clone(),
                    symbol: symbol.clone(),
                    status: status.clone(),
                    exchange_order_id: *exchange_order_id,
                    executed_qty: *executed_qty,
                    cumulative_quote_qty: *cumulative_quote_qty,
                    last_update_ts_ms: *update_ts_ms,
                });
            }
            _ => {}
        }
    }
}

#[derive(Clone)]
pub struct EngineHandle {
    pub ready: Arc<AtomicBool>,
    pub kill: Arc<AtomicBool>,
    pub state: Arc<RwLock<EngineState>>,
}

impl EngineHandle {
    pub async fn status_json(&self) -> serde_json::Value {
        let s = self.state.read().await;
        serde_json::json!({
            "ready": self.ready.load(Ordering::Relaxed),
            "kill_switch": s.kill_switch,
            "mode": format!("{:?}", s.mode),
            "open_orders": s.open_orders.len(),
            "last_event_id": s.last_event_id
        })
    }

    pub async fn engage_kill(&self, reason: &str, store: Arc<SqliteStore>) -> Result<()> {
        store.append_event(&Event::KillSwitchEngaged{ reason: reason.to_string() }, Some(format!("kill:{}", now_ms()))).await?;
        self.kill.store(true, Ordering::Relaxed);
        Ok(())
    }
}

pub struct Engine {
    cfg: AppConfig,
    store: Arc<SqliteStore>,

    state: Arc<RwLock<EngineState>>,
    ready: Arc<AtomicBool>,
    kill: Arc<AtomicBool>,

    // Event ingress
    ev_tx: mpsc::Sender<Event>,
    ev_rx: mpsc::Receiver<Event>,
}

impl Engine {
    pub async fn new(cfg: AppConfig, store: Arc<SqliteStore>) -> Result<Self> {
        let (ev_tx, ev_rx) = mpsc::channel(2048);

        // Load snapshot + replay
        let state = if let Some((last_id, state_json)) = store.load_latest_snapshot().await? {
            let mut s: EngineState = serde_json::from_str(&state_json).context("snapshot decode")?;
            s.last_event_id = last_id;
            s
        } else {
            EngineState::new(cfg.mode.clone())
        };

        Ok(Self {
            cfg,
            store,
            state: Arc::new(RwLock::new(state)),
            ready: Arc::new(AtomicBool::new(false)),
            kill: Arc::new(AtomicBool::new(false)),
            ev_tx,
            ev_rx,
        })
    }

    pub fn handle(&self) -> EngineHandle {
        EngineHandle {
            ready: self.ready.clone(),
            kill: self.kill.clone(),
            state: self.state.clone(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        // replay remaining events since snapshot
        self.replay().await?;

        // Init exchangeInfo filters
        let rules = fetch_symbol_rules(&self.cfg.binance.rest_base_url, &self.cfg.universe.symbols).await?;
        tracing::info!(symbols=%rules.len(), "loaded exchange rules");

        // Load signers from env
        let (api_key, rest_signer_opt, ws_signer_opt) = Signer::from_env()?;
        let rest_signer = rest_signer_opt.context("BINANCE_API_SECRET required for reconciliation + live REST")?;
        let ws_signer = ws_signer_opt.context("BINANCE_ED25519_PRIVATE_KEY_PEM required for WS API session.logon (preferred)")?;

        // REST client (for reconciliation)
        let mut rest = BinanceRest::new(
            self.cfg.binance.rest_base_url.clone(),
            api_key.clone(),
            rest_signer,
            self.cfg.binance.recv_window_ms,
        );

        // Time sync loop
        let time_sync_cfg = self.cfg.binance.clone();
        let mut rest_for_time = rest.clone();
        tokio::spawn(async move {
            loop {
                match rest_for_time.server_time().await {
                    Ok(server_ms) => {
                        let local_ms = now_ms();
                        let offset = server_ms - local_ms;
                        rest_for_time.set_time_offset_ms(offset);
                        tracing::info!(offset_ms=offset, "time offset updated");
                    }
                    Err(e) => tracing::warn!(error=?e, "time sync failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(time_sync_cfg.time_sync_interval_sec)).await;
            }
        });

        // Market data WS
        let market_runner = {
            let base = self.cfg.binance.ws_stream_base_url.clone();
            let symbols = self.cfg.universe.symbols.clone();
            let interval = self.cfg.universe.timeframe.clone();
            let tx = self.ev_tx.clone();
            tokio::spawn(async move {
                let md = crate::exchange::binance_ws_streams::MarketStreams::new(base, symbols, interval, tx);
                if let Err(e) = md.run().await {
                    tracing::error!(error=?e, "market streams failed");
                }
            })
        };

        // User data WS API
        let (user_tx, mut user_rx) = mpsc::channel::<serde_json::Value>(2048);
        let ws_url = self.cfg.binance.ws_api_base_url.clone();

        let ws_handle: WsApiHandle = {
            let client = WsApiClient::new(api_key.clone(), ws_signer, ws_url, user_tx);
            client.connect_and_run().await?
        };

        // Authenticate and subscribe (preferred path)
        ws_handle.session_logon(now_ms(), self.cfg.binance.recv_window_ms).await?;
        let sub_id = ws_handle.user_data_subscribe().await?;
        tracing::info!(subscription_id=sub_id, "user data stream subscribed"); // 

        // Forward user events into engine as OrderUpdate / Fill events
        let store_for_user = self.store.clone();
        let state_for_user = self.state.clone();
        let ev_tx_user = self.ev_tx.clone();
        tokio::spawn(async move {
            // at-least-once dedup cache (fast-path); db dedup is authoritative
            let mut seen = LruCache::<String, ()>::new(NonZeroUsize::new(10_000).unwrap());
            while let Some(raw) = user_rx.recv().await {
                // ExecutionReport is the key event we care about
                let et = raw.get("e").and_then(|x| x.as_str()).unwrap_or("");
                if et != "executionReport" {
                    continue;
                }

                let rep: crate::exchange::models::ExecutionReport = match serde_json::from_value(raw.clone()) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = store_for_user.append_event(&Event::Error {
                            where_: "user_stream.parse".into(),
                            error: format!("{:?}", e),
                        }, None).await;
                        continue;
                    }
                };

                let executed_qty = rep.cumulative_filled_decimal();
                let cum_quote = rep.cumulative_quote_decimal();

                // Dedup key: trade events use order_id+trade_id; others use order_id+execution_type+event_time
                let dedup = if rep.execution_type == "TRADE" {
                    format!("exec:TRADE:{}:{}", rep.order_id, rep.trade_id.unwrap_or(-1))
                } else {
                    format!("exec:{}:{}:{}", rep.execution_type, rep.order_id, rep.event_time)
                };

                if seen.contains(&dedup) {
                    continue;
                }
                seen.put(dedup.clone(), ());

                // Persist + emit OrderUpdate event
                let upd = Event::OrderUpdate {
                    client_order_id: rep.client_order_id.clone(),
                    symbol: rep.symbol.clone(),
                    status: rep.order_status.clone(),
                    executed_qty,
                    cumulative_quote_qty: cum_quote,
                    exchange_order_id: Some(rep.order_id),
                    update_ts_ms: rep.event_time,
                    source: "user_stream".into(),
                };

                let _ = store_for_user.append_event(&upd, Some(dedup.clone())).await;
                let _ = ev_tx_user.send(upd).await;

                // Fill event for TRADE
                if rep.execution_type == "TRADE" {
                    if let (Some(px), Some(qty), Some(ts)) = (rep.last_filled_price.as_deref(), rep.last_filled_qty.as_deref(), rep.trade_time) {
                        let price: Decimal = px.parse().unwrap_or(Decimal::ZERO);
                        let qtyd: Decimal = qty.parse().unwrap_or(Decimal::ZERO);
                        let fill = Event::Fill {
                            client_order_id: rep.client_order_id.clone(),
                            symbol: rep.symbol.clone(),
                            trade_id: rep.trade_id,
                            price,
                            qty: qtyd,
                            commission: rep.commission.as_deref().and_then(|c| c.parse().ok()),
                            commission_asset: rep.commission_asset.clone(),
                            ts_ms: ts,
                        };
                        let fill_dedup = format!("fill:{}:{}", rep.order_id, rep.trade_id.unwrap_or(-1));
                        let _ = store_for_user.append_event(&fill, Some(fill_dedup.clone())).await;

                        // update fills table (optional convenience)
                        let _ = store_for_user.insert_fill(
                            fill_dedup,
                            rep.client_order_id.clone(),
                            rep.symbol.clone(),
                            rep.trade_id,
                            price.to_string(),
                            qtyd.to_string(),
                            rep.commission.clone(),
                            rep.commission_asset.clone(),
                            ts,
                        ).await;
                    }
                }

                // Update state eagerly (engine loop also applies)
                let mut s = state_for_user.write().await;
                s.apply(&Event::OrderUpdate {
                    client_order_id: rep.client_order_id,
                    symbol: rep.symbol,
                    status: rep.order_status,
                    executed_qty,
                    cumulative_quote_qty: cum_quote,
                    exchange_order_id: Some(rep.order_id),
                    update_ts_ms: rep.event_time,
                    source: "user_stream".into(),
                });
            }
        });

        // Reconciler loop (periodic)
        if self.cfg.reconciler.enabled {
            let rec = Reconciler::new(
                rest.clone(),
                self.store.clone(),
                self.cfg.universe.symbols.clone(),
                self.cfg.execution.order_client_id_prefix.clone(),
                self.state.clone(),
            );
            let interval = self.cfg.reconciler.interval_sec;
            tokio::spawn(async move {
                loop {
                    let t = now_ms();
                    if let Err(e) = rec.run_once(t).await {
                        tracing::warn!(error=?e, "reconciliation run failed");
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                }
            });
        }

        // Strategy
        let mut strategy: Box<dyn Strategy> = Box::new(MaCrossover::new(self.cfg.strategy.ma_crossover.clone()));
        let risk = RiskManager::new(self.cfg.risk.clone())?;

        // Mark ready only after both market + user stream are running
        self.ready.store(true, Ordering::Relaxed);

        // Engine event loop
        let mut events_since_snapshot: u64 = 0;
        loop {
            if let Some(ev) = self.ev_rx.recv().await {
                // Persist (dedup handled by store where applicable)
                let _ = self.store.append_event(&ev, None).await?;
                {
                    let mut s = self.state.write().await;
                    s.apply(&ev);
                    s.last_event_id = self.store.last_event_id().await?;
                }

                events_since_snapshot += 1;

                // Snapshotting
                if events_since_snapshot >= self.cfg.persistence.snapshot_every_n_events {
                    let s = self.state.read().await;
                    let json = serde_json::to_string(&*s)?;
                    self.store.save_snapshot(s.last_event_id, json).await?;
                    events_since_snapshot = 0;
                }

                // React: on candle, run strategy, produce intents
                if let Event::MarketCandleClosed(c) = &ev {
                    let intents = strategy.on_candle(c)?;
                    for intent in intents {
                        let intent_ev = Event::OrderIntentCreated(intent.clone());
                        self.store.append_event(&intent_ev, Some(format!("intent:{}", intent.client_order_id))).await?;
                        // Risk + mode gating (placeholder sizing & exposure)
                        let mid = c.close;
                        let symbol_exposure = Decimal::ZERO;
                        let portfolio_exposure = Decimal::ZERO;

                        let s = self.state.read().await;
                        if s.kill_switch || self.kill.load(Ordering::Relaxed) {
                            self.store.append_event(&Event::OrderIntentRejected {
                                client_order_id: intent.client_order_id.clone(),
                                reason: "kill_switch".into(),
                            }, Some(format!("intent_rej:{}", intent.client_order_id))).await?;
                            continue;
                        }

                        if let Err(e) = risk.pre_trade_check(&intent, mid, symbol_exposure, portfolio_exposure, &s.risk) {
                            self.store.append_event(&Event::OrderIntentRejected {
                                client_order_id: intent.client_order_id.clone(),
                                reason: format!("{:?}", e),
                            }, Some(format!("intent_rej:{}", intent.client_order_id))).await?;
                            continue;
                        }

                        match self.cfg.mode {
                            Mode::DryRun => {
                                tracing::info!(client_order_id=%intent.client_order_id, "dry-run: would place order");
                            }
                            Mode::Paper => {
                                tracing::info!(client_order_id=%intent.client_order_id, "paper: simulate fill at close");
                            }
                            Mode::Live => {
                                // Live execution omitted in this snippet to keep focus on requested WS user-data + recon + event log.
                                // Hook here: enforce rules[symbol], build order payload, REST POST /api/v3/order with newClientOrderId.
                                tracing::warn!("live execution is intentionally not wired in this skeleton section");
                            }
                        }
                    }
                }
            }
        }

        // let _ = market_runner; // keep alive
    }

    async fn replay(&self) -> Result<()> {
        let mut after_id = {
            let s = self.state.read().await;
            s.last_event_id
        };

        loop {
            let batch = self.store.load_events_after(after_id, 5000).await?;
            if batch.is_empty() {
                break;
            }
            for (id, payload) in batch {
                let ev: Event = serde_json::from_str(&payload).context("event decode")?;
                {
                    let mut s = self.state.write().await;
                    s.apply(&ev);
                    s.last_event_id = id;
                }
                after_id = id;
            }
        }

        // record engine started
        let _ = self.store.append_event(&Event::EngineStarted { version: env!("CARGO_PKG_VERSION").into() }, Some(format!("engine_started:{}", now_ms()))).await?;
        Ok(())
    }
}
