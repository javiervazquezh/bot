use binance_bot::exchange::exchange_info::SymbolRules;
use rust_decimal::Decimal;
use std::str::FromStr;

#[test]
fn rounding_down_respects_tick_and_step() {
    let rules = SymbolRules {
        symbol: "BTCUSDT".into(),
        tick_size: Some(Decimal::from_str("0.10").unwrap()),
        step_size: Some(Decimal::from_str("0.001").unwrap()),
        min_qty: Some(Decimal::from_str("0.001").unwrap()),
        max_qty: None,
        min_notional: None,
        max_notional: None,
    };

    let price = rules.round_price_down(Decimal::from_str("100.17").unwrap()).unwrap();
    assert_eq!(price, Decimal::from_str("100.10").unwrap());

    let qty = rules.round_qty_down(Decimal::from_str("0.0019").unwrap()).unwrap();
    assert_eq!(qty, Decimal::from_str("0.001").unwrap());
}
