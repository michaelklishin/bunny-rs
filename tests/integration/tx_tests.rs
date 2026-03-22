// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

#[tokio::test]
async fn test_tx_commit() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.tx-commit", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.tx-commit").await.unwrap();

    ch.tx_select().await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.tx-commit",
        &PublishOptions::default(),
        b"tx-msg-1",
    )
    .await
    .unwrap();
    ch.basic_publish(
        "",
        "bunny-rs.test.tx-commit",
        &PublishOptions::default(),
        b"tx-msg-2",
    )
    .await
    .unwrap();

    ch.tx_commit().await.unwrap();

    // messages should be visible after commit
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let d1 = ch.basic_get("bunny-rs.test.tx-commit", true).await.unwrap();
    assert!(d1.is_some());
    let d2 = ch.basic_get("bunny-rs.test.tx-commit", true).await.unwrap();
    assert!(d2.is_some());

    ch.queue_delete("bunny-rs.test.tx-commit", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_tx_rollback() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.tx-rollback", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.tx-rollback").await.unwrap();

    ch.tx_select().await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.tx-rollback",
        &PublishOptions::default(),
        b"rolled-back",
    )
    .await
    .unwrap();

    ch.tx_rollback().await.unwrap();

    // message should NOT be visible after rollback
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let d = ch
        .basic_get("bunny-rs.test.tx-rollback", true)
        .await
        .unwrap();
    assert!(d.is_none());

    ch.queue_delete("bunny-rs.test.tx-rollback", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
