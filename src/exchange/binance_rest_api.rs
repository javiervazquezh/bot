use crate::exchange::signer::Signer;
use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct BinanceRest {
    base_url: String, // e.g. https://testnet.binance.vision/api
    api_key: String,
    signer: Signer,
    recv_window_ms: u64,
    client: Client,
    time_offset_ms: i64,
}

impl BinanceRest {
    pub fn new(base_url: String, api_key: String, signer: Signer, recv_window_ms: u64) -> Self {
        Self {
            base_url,
            api_key,
            signer,
            recv_window_ms,
            client: Client::new(),
            time_offset_ms: 0,
        }
    }

    pub fn set_time_offset_ms(&mut self, offset: i64) {
        self.time_offset_ms = offset;
    }

    fn signed_query(&self, mut params: BTreeMap<String, String>, now_ms: i64) -> Result<String> {
        params.insert("timestamp".to_string(), (now_ms + self.time_offset_ms).to_string());
        params.insert("recvWindow".to_string(), self.recv_window_ms.to_string());

        let query = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        let sig = self.signer.sign_rest_query(&query)?;
        Ok(format!("{}&signature={}", query, sig))
    }

    pub async fn get_account(&self, now_ms: i64) -> Result<serde_json::Value> {
        // GET /api/v3/account 
        let query = self.signed_query(BTreeMap::new(), now_ms)?;
        let url = format!("{}/v3/account?{}", self.base_url, query);
        let resp = self
            .client
            .get(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;
        Ok(resp)
    }

    pub async fn open_orders(&self, symbol: &str, now_ms: i64) -> Result<Vec<serde_json::Value>> {
        // GET /api/v3/openOrders 
        let mut p = BTreeMap::new();
        p.insert("symbol".to_string(), symbol.to_string());
        let query = self.signed_query(p, now_ms)?;
        let url = format!("{}/v3/openOrders?{}", self.base_url, query);
        let resp = self
            .client
            .get(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<serde_json::Value>>()
            .await?;
        Ok(resp)
    }

    pub async fn query_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
        now_ms: i64,
    ) -> Result<serde_json::Value> {
        // GET /api/v3/order 
        let mut p = BTreeMap::new();
        p.insert("symbol".to_string(), symbol.to_string());
        p.insert("origClientOrderId".to_string(), client_order_id.to_string());
        let query = self.signed_query(p, now_ms)?;
        let url = format!("{}/v3/order?{}", self.base_url, query);
        let resp = self
            .client
            .get(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;
        Ok(resp)
    }

    pub async fn server_time(&self) -> Result<i64> {
        #[derive(Deserialize)]
        struct TimeResp {
            serverTime: i64,
        }
        let url = format!("{}/v3/time", self.base_url);
        let t: TimeResp = self.client.get(url).send().await?.json().await?;
        Ok(t.serverTime)
    }
}
