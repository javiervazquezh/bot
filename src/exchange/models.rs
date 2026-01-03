use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct WsApiResponse {
    pub id: Option<String>,
    pub status: Option<i64>,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
    pub rate_limits: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct WsApiUserEventEnvelope {
    pub subscription_id: i64,
    pub event: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionReport {
    #[serde(rename = "e")]
    pub event_type: String, // "executionReport"
    #[serde(rename = "E")]
    pub event_time: i64,

    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,

    #[serde(rename = "i")]
    pub order_id: i64,

    #[serde(rename = "X")]
    pub order_status: String, // NEW, FILLED, CANCELED...
    #[serde(rename = "x")]
    pub execution_type: String, // NEW, CANCELED, TRADE...

    #[serde(rename = "z")]
    pub cumulative_filled_qty: String,

    #[serde(rename = "Z")]
    pub cumulative_quote_qty: Option<String>,

    #[serde(rename = "l")]
    pub last_filled_qty: Option<String>,
    #[serde(rename = "L")]
    pub last_filled_price: Option<String>,

    #[serde(rename = "t")]
    pub trade_id: Option<i64>,

    #[serde(rename = "n")]
    pub commission: Option<String>,
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,

    #[serde(rename = "T")]
    pub trade_time: Option<i64>,
}

impl ExecutionReport {
    pub fn cumulative_filled_decimal(&self) -> Decimal {
        self.cumulative_filled_qty.parse().unwrap_or(Decimal::ZERO)
    }
    pub fn cumulative_quote_decimal(&self) -> Decimal {
        self.cumulative_quote_qty
            .as_deref()
            .unwrap_or("0")
            .parse()
            .unwrap_or(Decimal::ZERO)
    }
}
