use anyhow::{Context, Result};
use base64::Engine;
use ed25519_dalek::SigningKey;
use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::str::FromStr;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub enum Signer {
    Hmac { api_secret: SecretString },
    Ed25519 { signing_key: SigningKey },
}

impl Signer {
    /// WS API signature payload: sort params, join as k=v&..., UTF-8 bytes, sign.
    /// Ed25519: sign then base64 encode. 
    pub fn sign_ws_params(&self, params: &BTreeMap<String, String>) -> Result<String> {
        let payload = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        match self {
            Signer::Ed25519 { signing_key } => {
                let sig = signing_key.sign(payload.as_bytes());
                let b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
                Ok(b64)
            }
            Signer::Hmac { api_secret } => {
                let mut mac = HmacSha256::new_from_slice(api_secret.expose_secret().as_bytes())
                    .context("hmac init")?;
                mac.update(payload.as_bytes());
                Ok(hex::encode(mac.finalize().into_bytes()))
            }
        }
    }

    /// REST signed endpoints on Binance Spot typically use HMAC SHA256 of query string.
    pub fn sign_rest_query(&self, query: &str) -> Result<String> {
        match self {
            Signer::Hmac { api_secret } => {
                let mut mac = HmacSha256::new_from_slice(api_secret.expose_secret().as_bytes())
                    .context("hmac init")?;
                mac.update(query.as_bytes());
                Ok(hex::encode(mac.finalize().into_bytes()))
            }
            Signer::Ed25519 { .. } => anyhow::bail!("REST signing with Ed25519 not implemented in this skeleton"),
        }
    }

    pub fn ed25519_from_pem(pem: &str) -> Result<SigningKey> {
        let key = SigningKey::from_pkcs8_pem(pem).context("parse ed25519 pkcs8 pem")?;
        Ok(key)
    }

    pub fn from_env() -> Result<(String, Option<Signer>, Option<Signer>)> {
        // BINANCE_API_KEY required
        let api_key = std::env::var("BINANCE_API_KEY").context("BINANCE_API_KEY not set")?;

        // REST signer (HMAC) optional (required for live REST trading/recon)
        let rest_signer = std::env::var("BINANCE_API_SECRET")
            .ok()
            .map(|s| Signer::Hmac {
                api_secret: SecretString::from_str(&s).unwrap(),
            });

        // WS API signer (Ed25519) optional (preferred)
        let ws_signer = std::env::var("BINANCE_ED25519_PRIVATE_KEY_PEM")
            .ok()
            .and_then(|pem| Signer::ed25519_from_pem(&pem).ok())
            .map(|k| Signer::Ed25519 { signing_key: k });

        Ok((api_key, rest_signer, ws_signer))
    }
}
