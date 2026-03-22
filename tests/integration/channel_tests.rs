// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{
    ExchangeDeclareOptions, ExchangeDeleteOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use bunny_rs::protocol::types::FieldTable;

#[tokio::test]
async fn test_open_and_close_channel() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    assert_eq!(ch.id(), 1);
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_open_multiple_channels() {
    let conn = connect().await;
    let ch1 = conn.open_channel().await.unwrap();
    let ch2 = conn.open_channel().await.unwrap();
    assert_eq!(ch1.id(), 1);
    assert_eq!(ch2.id(), 2);
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_declare_and_delete() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let result = ch
        .queue_declare(
            "bunny-rs.test.queue.declare",
            QueueDeclareOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.name.as_str(), "bunny-rs.test.queue.declare");

    ch.queue_delete("bunny-rs.test.queue.declare", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_server_named_queue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let result = ch
        .queue_declare("", QueueDeclareOptions::exclusive())
        .await
        .unwrap();

    // Server generates a name like "amq.gen-..."
    assert!(result.name.starts_with("amq.gen-"));

    ch.queue_delete(result.name.as_str(), QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_exchange_declare_and_delete() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.exchange",
        "fanout",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();

    ch.exchange_delete("bunny-rs.test.exchange", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_bind_and_unbind() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.bind.exchange",
        "direct",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();

    ch.queue_declare("bunny-rs.test.bind.queue", QueueDeclareOptions::default())
        .await
        .unwrap();

    ch.queue_bind(
        "bunny-rs.test.bind.queue",
        "bunny-rs.test.bind.exchange",
        "test-key",
        FieldTable::new(),
    )
    .await
    .unwrap();

    // cleanup
    ch.queue_delete("bunny-rs.test.bind.queue", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete(
        "bunny-rs.test.bind.exchange",
        ExchangeDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_basic_qos() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.basic_qos(10).await.unwrap();

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_confirm_select() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
