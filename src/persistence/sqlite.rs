use crate::event_log::Event;
use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_rusqlite::Connection;

#[derive(Clone)]
pub struct SqliteStore {
    conn: Connection,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn sha256_hex(input: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(input);
    hex::encode(h.finalize())
}

impl SqliteStore {
    pub async fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path).await.context("open sqlite")?;
        Ok(Self { conn })
    }

    pub async fn init_schema(&self) -> Result<()> {
        self.conn
            .call(|c| {
                c.execute_batch(
                    r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS event_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  kind TEXT NOT NULL,
  dedup_key TEXT UNIQUE,
  payload_json TEXT NOT NULL,
  prev_hash TEXT,
  hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS state_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  last_event_id INTEGER NOT NULL,
  state_json TEXT NOT NULL,
  hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  client_order_id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  exchange_order_id INTEGER,
  executed_qty TEXT NOT NULL,
  cumulative_quote_qty TEXT NOT NULL,
  last_update_ts_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dedup_key TEXT UNIQUE,
  client_order_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  trade_id INTEGER,
  price TEXT NOT NULL,
  qty TEXT NOT NULL,
  commission TEXT,
  commission_asset TEXT,
  ts_ms INTEGER NOT NULL
);
"#,
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Append an event with optional dedup_key.
    /// Returns Some(event_id) if inserted, or None if dedup prevented insert.
    pub async fn append_event(&self, event: &Event, dedup_key: Option<String>) -> Result<Option<i64>> {
        let kind = event.kind().to_string();
        let ts_ms = now_ms();
        let payload_json = serde_json::to_string(event).context("serialize event")?;

        let inserted = self
            .conn
            .call(move |c| -> Result<Option<i64>> {
                // fetch last hash
                let prev_hash: Option<String> = c
                    .query_row(
                        "SELECT hash FROM event_log ORDER BY id DESC LIMIT 1",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?;

                let material = format!(
                    "{}|{}|{}|{}",
                    prev_hash.clone().unwrap_or_default(),
                    ts_ms,
                    kind,
                    payload_json
                );
                let hash = sha256_hex(material.as_bytes());

                let r = c.execute(
                    "INSERT INTO event_log (ts_ms, kind, dedup_key, payload_json, prev_hash, hash)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![ts_ms, kind, dedup_key, payload_json, prev_hash, hash],
                );

                match r {
                    Ok(_) => {
                        let id = c.last_insert_rowid();
                        Ok(Some(id))
                    }
                    Err(e) => {
                        // Dedup via UNIQUE constraint on dedup_key
                        if let rusqlite::Error::SqliteFailure(err, _) = &e {
                            if err.code == rusqlite::ErrorCode::ConstraintViolation
                                || err.code == rusqlite::ErrorCode::ConstraintUnique
                            {
                                return Ok(None);
                            }
                        }
                        Err(e).context("insert event")?
                    }
                }
            })
            .await?;

        Ok(inserted)
    }

    pub async fn load_events_after(&self, after_id: i64, limit: i64) -> Result<Vec<(i64, String)>> {
        self.conn
            .call(move |c| -> Result<Vec<(i64, String)>> {
                let mut stmt = c.prepare(
                    "SELECT id, payload_json FROM event_log WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![after_id, limit], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })?;
                let mut out = vec![];
                for r in rows {
                    out.push(r?);
                }
                Ok(out)
            })
            .await
            .context("load_events_after")
    }

    pub async fn last_event_id(&self) -> Result<i64> {
        self.conn
            .call(|c| -> Result<i64> {
                let v: Option<i64> = c
                    .query_row("SELECT id FROM event_log ORDER BY id DESC LIMIT 1", [], |r| r.get(0))
                    .optional()?;
                Ok(v.unwrap_or(0))
            })
            .await
            .context("last_event_id")
    }

    pub async fn upsert_order_row(
        &self,
        client_order_id: String,
        symbol: String,
        status: String,
        exchange_order_id: Option<i64>,
        executed_qty: String,
        cumulative_quote_qty: String,
        last_update_ts_ms: i64,
    ) -> Result<()> {
        self.conn
            .call(move |c| -> Result<()> {
                c.execute(
                    r#"
INSERT INTO orders (client_order_id, symbol, status, exchange_order_id, executed_qty, cumulative_quote_qty, last_update_ts_ms)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
ON CONFLICT(client_order_id) DO UPDATE SET
  status=excluded.status,
  exchange_order_id=excluded.exchange_order_id,
  executed_qty=excluded.executed_qty,
  cumulative_quote_qty=excluded.cumulative_quote_qty,
  last_update_ts_ms=excluded.last_update_ts_ms
"#,
                    params![
                        client_order_id,
                        symbol,
                        status,
                        exchange_order_id,
                        executed_qty,
                        cumulative_quote_qty,
                        last_update_ts_ms
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn insert_fill(
        &self,
        dedup_key: String,
        client_order_id: String,
        symbol: String,
        trade_id: Option<i64>,
        price: String,
        qty: String,
        commission: Option<String>,
        commission_asset: Option<String>,
        ts_ms: i64,
    ) -> Result<bool> {
        let inserted = self.conn.call(move |c| -> Result<bool> {
            let r = c.execute(
                r#"
INSERT INTO fills (dedup_key, client_order_id, symbol, trade_id, price, qty, commission, commission_asset, ts_ms)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
"#,
                params![dedup_key, client_order_id, symbol, trade_id, price, qty, commission, commission_asset, ts_ms],
            );
            match r {
                Ok(_) => Ok(true),
                Err(e) => {
                    if let rusqlite::Error::SqliteFailure(err, _) = &e {
                        if err.code == rusqlite::ErrorCode::ConstraintViolation
                            || err.code == rusqlite::ErrorCode::ConstraintUnique
                        {
                            return Ok(false);
                        }
                    }
                    Err(e).context("insert fill")?
                }
            }
        }).await?;
        Ok(inserted)
    }

    pub async fn save_snapshot(&self, last_event_id: i64, state_json: String) -> Result<()> {
        let ts_ms = now_ms();
        self.conn
            .call(move |c| -> Result<()> {
                let material = format!("{}|{}|{}", ts_ms, last_event_id, state_json);
                let hash = sha256_hex(material.as_bytes());
                c.execute(
                    "INSERT INTO state_snapshots (ts_ms, last_event_id, state_json, hash) VALUES (?1, ?2, ?3, ?4)",
                    params![ts_ms, last_event_id, state_json, hash],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn load_latest_snapshot(&self) -> Result<Option<(i64, String)>> {
        self.conn
            .call(|c| -> Result<Option<(i64, String)>> {
                c.query_row(
                    "SELECT last_event_id, state_json FROM state_snapshots ORDER BY id DESC LIMIT 1",
                    [],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()
            })
            .await
            .context("load_latest_snapshot")
    }
}
