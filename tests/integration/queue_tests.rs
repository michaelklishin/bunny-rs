// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use crate::test_helpers::connect;
use bunny_rs::options::{
    ExchangeDeclareOptions, ExchangeDeleteOptions, PublishOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use bunny_rs::protocol::types::FieldTable;

#[tokio::test]
async fn test_durable_queue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let result = ch
        .queue_declare("bunny-rs.test.durable-q", QueueDeclareOptions::durable())
        .await
        .unwrap();
    assert_eq!(result.name.as_str(), "bunny-rs.test.durable-q");

    ch.queue_delete("bunny-rs.test.durable-q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_exclusive_queue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let result = ch
        .queue_declare("", QueueDeclareOptions::exclusive())
        .await
        .unwrap();
    assert!(result.name.starts_with("amq.gen-"));

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_with_ttl() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let result = ch
        .queue_declare(
            "bunny-rs.test.ttl-q",
            QueueDeclareOptions::default().message_ttl(Duration::from_millis(5000)),
        )
        .await
        .unwrap();
    assert_eq!(result.name.as_str(), "bunny-rs.test.ttl-q");

    ch.queue_delete("bunny-rs.test.ttl-q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_with_max_length() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.maxlen-q",
        QueueDeclareOptions::default().max_length(100),
    )
    .await
    .unwrap();

    ch.queue_delete("bunny-rs.test.maxlen-q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_quorum_queue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let result = ch
        .queue_declare("bunny-rs.test.quorum-q", QueueDeclareOptions::quorum())
        .await
        .unwrap();
    assert_eq!(result.name.as_str(), "bunny-rs.test.quorum-q");

    ch.queue_delete("bunny-rs.test.quorum-q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_temporary_queue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let q = ch.temporary_queue().await.unwrap();
    assert!(q.name.starts_with("amq.gen-"));

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_declare_passive_existing() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.passive-exists",
        QueueDeclareOptions::default(),
    )
    .await
    .unwrap();

    let info = ch
        .queue_declare_passive("bunny-rs.test.passive-exists")
        .await
        .unwrap();
    assert_eq!(info.name, "bunny-rs.test.passive-exists");

    ch.queue_delete(
        "bunny-rs.test.passive-exists",
        QueueDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_declare_passive_nonexistent() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let result = ch
        .queue_declare_passive("bunny-rs.test.does-not-exist-xyz")
        .await;
    assert!(result.is_err());

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_exchange_declare_passive_existing() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    // amq.direct always exists
    ch.exchange_declare_passive("amq.direct").await.unwrap();

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_purge_returns_message_count() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.purge-count", QueueDeclareOptions::default())
        .await
        .unwrap();

    // publish 3 messages
    for i in 0..3 {
        let body = format!("msg-{i}");
        ch.basic_publish(
            "",
            "bunny-rs.test.purge-count",
            &PublishOptions::default(),
            body.as_bytes(),
        )
        .await
        .unwrap();
    }

    // give broker a moment to route
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let purged = ch.queue_purge("bunny-rs.test.purge-count").await.unwrap();
    assert_eq!(purged, 3);

    ch.queue_delete("bunny-rs.test.purge-count", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_unbind() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.unbind-x",
        "direct",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("bunny-rs.test.unbind-q", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_bind(
        "bunny-rs.test.unbind-q",
        "bunny-rs.test.unbind-x",
        "rk",
        FieldTable::new(),
    )
    .await
    .unwrap();

    // publish before unbind — message should arrive
    ch.basic_publish(
        "bunny-rs.test.unbind-x",
        "rk",
        &PublishOptions::default(),
        b"before-unbind",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let d = ch.basic_get("bunny-rs.test.unbind-q", true).await.unwrap();
    assert!(d.is_some());

    // unbind
    ch.queue_unbind(
        "bunny-rs.test.unbind-q",
        "bunny-rs.test.unbind-x",
        "rk",
        FieldTable::new(),
    )
    .await
    .unwrap();

    // publish after unbind — message should NOT arrive
    ch.basic_publish(
        "bunny-rs.test.unbind-x",
        "rk",
        &PublishOptions::default(),
        b"after-unbind",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let d = ch.basic_get("bunny-rs.test.unbind-q", true).await.unwrap();
    assert!(d.is_none());

    ch.queue_delete("bunny-rs.test.unbind-q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete("bunny-rs.test.unbind-x", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
