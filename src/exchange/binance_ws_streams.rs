use crate::event_log::Event;
use crate::types::Candle;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
pub struct MarketStreams {
    base_url: String,
    symbols: Vec<String>,
    interval: String,
    out_tx: mpsc::Sender<Event>,
}

impl MarketStreams {
    pub fn new(base_url: String, symbols: Vec<String>, interval: String, out_tx: mpsc::Sender<Event>) -> Self {
        Self { base_url, symbols, interval, out_tx }
    }

    pub async fn run(self) -> Result<()> {
        loop {
            let streams = self.symbols.iter()
                .map(|s| format!("{}@kline_{}", s.to_lowercase(), self.interval))
                .collect::<Vec<_>>()
                .join("/");

            let url = format!("{}/stream?streams={}", self.base_url.trim_end_matches("/ws"), streams);
            tracing::info!(%url, "connecting to market data streams");

            let (ws, _) = match tokio_tungstenite::connect_async(&url).await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(error=?e, "market ws connect failed; retrying");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();

            // Keepalive: unsolicited pong frames are allowed (varies by stream server). We'll just respond to ping.
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(t)) => {
                        let v: serde_json::Value = match serde_json::from_str(&t) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!(error=?e, "market ws invalid json");
                                continue;
                            }
                        };

                        let data = v.get("data").cloned().unwrap_or(v);
                        let e = data.get("e").and_then(|x| x.as_str()).unwrap_or("");
                        if e != "kline" {
                            continue;
                        }

                        let k = &data["k"];
                        let is_closed = k.get("x").and_then(|x| x.as_bool()).unwrap_or(false);
                        if !is_closed {
                            continue;
                        }

                        let symbol = k.get("s").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        let interval = k.get("i").and_then(|x| x.as_str()).unwrap_or("").to_string();

                        let candle = Candle {
                            symbol,
                            interval,
                            open_time_ms: k.get("t").and_then(|x| x.as_i64()).unwrap_or(0),
                            close_time_ms: k.get("T").and_then(|x| x.as_i64()).unwrap_or(0),
                            open: k.get("o").and_then(|x| x.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO),
                            high: k.get("h").and_then(|x| x.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO),
                            low: k.get("l").and_then(|x| x.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO),
                            close: k.get("c").and_then(|x| x.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO),
                            volume: k.get("v").and_then(|x| x.as_str()).unwrap_or("0").parse().unwrap_or(Decimal::ZERO),
                        };

                        let _ = self.out_tx.send(Event::MarketCandleClosed(candle)).await;
                    }
                    Ok(Message::Ping(p)) => {
                        // echo ping payload
                        let _ = write.send(Message::Pong(p)).await;
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error=?e, "market ws error");
                        break;
                    }
                }
            }

            tracing::warn!("market ws disconnected; reconnecting");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
