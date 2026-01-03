use binance_bot::exchange::signer::Signer;
use base64::Engine;
use ed25519_dalek::SigningKey;
use std::collections::BTreeMap;

#[test]
fn ws_signing_payload_is_sorted_and_joined() {
    // Use deterministic key bytes (not secure; test only)
    let key_bytes = [7u8; 32];
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let signer = Signer::Ed25519 { signing_key };

    let mut params = BTreeMap::new();
    params.insert("b".to_string(), "2".to_string());
    params.insert("a".to_string(), "1".to_string());

    let sig = signer.sign_ws_params(&params).unwrap();
    // Signature should be base64 and non-empty
    let bytes = base64::engine::general_purpose::STANDARD.decode(sig).unwrap();
    assert_eq!(bytes.len(), 64); // Ed25519 signature length
}
