use binance_bot::event_log::Event;
use binance_bot::persistence::SqliteStore;
use std::sync::Arc;

#[tokio::test]
async fn event_log_dedup_works() {
    let store = Arc::new(SqliteStore::new(":memory:").await.unwrap());
    store.init_schema().await.unwrap();

    let ev = Event::KillSwitchEngaged { reason: "test".into() };
    let id1 = store.append_event(&ev, Some("dedup_key".into())).await.unwrap();
    let id2 = store.append_event(&ev, Some("dedup_key".into())).await.unwrap();

    assert!(id1.is_some());
    assert!(id2.is_none());
}
