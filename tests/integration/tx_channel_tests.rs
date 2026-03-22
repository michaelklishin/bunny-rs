// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

#[tokio::test]
async fn test_into_tx_mode_commit() {
    let conn = connect().await;
    let ch = conn.open_channel().await.unwrap();
    let mut tx_ch = ch.into_tx_mode().await.unwrap();

    tx_ch
        .queue_declare("bunny-rs.test.tx-ch", QueueDeclareOptions::default())
        .await
        .unwrap();
    tx_ch.queue_purge("bunny-rs.test.tx-ch").await.unwrap();

    tx_ch
        .basic_publish(
            "",
            "bunny-rs.test.tx-ch",
            &PublishOptions::default(),
            b"tx-msg",
        )
        .await
        .unwrap();

    tx_ch.commit().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let d = tx_ch.basic_get("bunny-rs.test.tx-ch", true).await.unwrap();
    assert!(d.is_some());

    tx_ch
        .queue_delete("bunny-rs.test.tx-ch", QueueDeleteOptions::default())
        .await
        .unwrap();
    tx_ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_into_tx_mode_rollback() {
    let conn = connect().await;
    let ch = conn.open_channel().await.unwrap();
    let mut tx_ch = ch.into_tx_mode().await.unwrap();

    tx_ch
        .queue_declare("bunny-rs.test.tx-ch-rb", QueueDeclareOptions::default())
        .await
        .unwrap();
    tx_ch.queue_purge("bunny-rs.test.tx-ch-rb").await.unwrap();

    tx_ch
        .basic_publish(
            "",
            "bunny-rs.test.tx-ch-rb",
            &PublishOptions::default(),
            b"rolled-back",
        )
        .await
        .unwrap();

    tx_ch.rollback().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let d = tx_ch
        .basic_get("bunny-rs.test.tx-ch-rb", true)
        .await
        .unwrap();
    assert!(d.is_none());

    tx_ch
        .queue_delete("bunny-rs.test.tx-ch-rb", QueueDeleteOptions::default())
        .await
        .unwrap();
    tx_ch.close().await.unwrap();
    conn.close().await.unwrap();
}
