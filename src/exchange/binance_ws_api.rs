use crate::exchange::models::{WsApiResponse, WsApiUserEventEnvelope};
use crate::exchange::signer::Signer;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

#[derive(Clone)]
pub struct WsApiClient {
    api_key: String,
    ws_signer: Signer,
    url: String,

    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Result<WsApiResponse>>>>>,
    user_events_tx: mpsc::Sender<serde_json::Value>,
}

impl WsApiClient {
    pub fn new(
        api_key: String,
        ws_signer: Signer,
        url: String,
        user_events_tx: mpsc::Sender<serde_json::Value>,
    ) -> Self {
        Self {
            api_key,
            ws_signer,
            url,
            pending: Arc::new(Mutex::new(HashMap::new())),
            user_events_tx,
        }
    }

    pub async fn connect_and_run(self) -> Result<WsApiHandle> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .context("connect ws-api")?;
        tracing::info!(url=%self.url, "connected to ws-api");

        let (mut write, mut read) = ws_stream.split();
        let pending = self.pending.clone();
        let user_tx = self.user_events_tx.clone();

        // Reader task: route responses + user events, respond to ping
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(t)) => {
                        let v: serde_json::Value = match serde_json::from_str(&t) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!(error=?e, text=%t, "ws-api invalid json");
                                continue;
                            }
                        };

                        // response path
                        if v.get("id").is_some() && v.get("status").is_some() {
                            let resp: WsApiResponse = match serde_json::from_value(v) {
                                Ok(r) => r,
                                Err(e) => {
                                    tracing::warn!(error=?e, "ws-api response parse failed");
                                    continue;
                                }
                            };
                            if let Some(id) = resp.id.clone() {
                                let mut map = pending.lock().await;
                                if let Some(tx) = map.remove(&id) {
                                    let _ = tx.send(Ok(resp));
                                }
                            }
                            continue;
                        }

                        // user data stream events look like: { "subscriptionId": 0, "event": {...} }
                        if v.get("subscriptionId").is_some() && v.get("event").is_some() {
                            // forward raw event (engine will parse)
                            if let Some(ev) = v.get("event") {
                                let _ = user_tx.send(ev.clone()).await;
                            }
                            continue;
                        }

                        tracing::debug!(raw=%t, "ws-api other message");
                    }
                    Ok(Message::Ping(payload)) => {
                        // NOTE: Binance WS API requires pong with copied payload 
                        if let Err(e) = write.send(Message::Pong(payload)).await {
                            tracing::warn!(error=?e, "failed to send pong");
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(frame)) => {
                        tracing::warn!(?frame, "ws-api closed");
                        break;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error=?e, "ws-api read error");
                        break;
                    }
                }
            }
        });

        // Writer is owned by handle
        Ok(WsApiHandle {
            api_key: self.api_key,
            ws_signer: self.ws_signer,
            write: Arc::new(Mutex::new(write)),
            pending: self.pending.clone(),
        })
    }
}

#[derive(Clone)]
pub struct WsApiHandle {
    api_key: String,
    ws_signer: Signer,
    write: Arc<Mutex<futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>>>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Result<WsApiResponse>>>>>,
}

impl WsApiHandle {
    async fn send_request(&self, method: &str, params: serde_json::Value) -> Result<WsApiResponse> {
        let id = Uuid::new_v4().to_string();
        let req = json!({
            "id": id,
            "method": method,
            "params": params
        });

        let (tx, rx) = oneshot::channel();
        {
            let mut map = self.pending.lock().await;
            map.insert(req["id"].as_str().unwrap().to_string(), tx);
        }

        {
            let mut w = self.write.lock().await;
            w.send(Message::Text(req.to_string())).await?;
        }

        let resp = rx.await.context("ws-api response dropped")??;
        Ok(resp)
    }

    /// Preferred authentication flow: session.logon (SIGNED) 
    pub async fn session_logon(&self, timestamp_ms: i64, recv_window_ms: u64) -> Result<()> {
        let mut params = BTreeMap::<String, String>::new();
        params.insert("apiKey".to_string(), self.api_key.clone());
        params.insert("timestamp".to_string(), timestamp_ms.to_string());
        params.insert("recvWindow".to_string(), recv_window_ms.to_string());

        let sig = self.ws_signer.sign_ws_params(&params)?;
        params.insert("signature".to_string(), sig);

        let params_json = serde_json::to_value(params)?;
        let resp = self.send_request("session.logon", params_json).await?;
        if resp.status != Some(200) {
            anyhow::bail!("session.logon failed: {:?}", resp);
        }
        Ok(())
    }

    /// Start user data stream subscription on this connection. 
    pub async fn user_data_subscribe(&self) -> Result<i64> {
        let resp = self.send_request("userDataStream.subscribe", json!({})).await?;
        if resp.status != Some(200) {
            anyhow::bail!("userDataStream.subscribe failed: {:?}", resp);
        }
        let sub_id = resp
            .result
            .and_then(|r| r.get("subscriptionId").cloned())
            .and_then(|v| v.as_i64())
            .context("missing subscriptionId")?;
        Ok(sub_id)
    }

    /// Optional fallback: subscribe via signature (does not require authenticated session). 
    pub async fn user_data_subscribe_signature(&self, timestamp_ms: i64) -> Result<i64> {
        let mut params = BTreeMap::<String, String>::new();
        params.insert("apiKey".to_string(), self.api_key.clone());
        params.insert("timestamp".to_string(), timestamp_ms.to_string());

        let sig = self.ws_signer.sign_ws_params(&params)?;
        params.insert("signature".to_string(), sig);

        let resp = self
            .send_request("userDataStream.subscribe.signature", serde_json::to_value(params)?)
            .await?;
        if resp.status != Some(200) {
            anyhow::bail!("userDataStream.subscribe.signature failed: {:?}", resp);
        }
        let sub_id = resp
            .result
            .and_then(|r| r.get("subscriptionId").cloned())
            .and_then(|v| v.as_i64())
            .context("missing subscriptionId")?;
        Ok(sub_id)
    }
}
