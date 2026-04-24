// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

#[tokio::test]
async fn test_into_confirm_mode() {
    let conn = connect().await;
    let ch = conn.open_channel().await.unwrap();
    let mut confirm_ch = ch.into_confirm_mode().await.unwrap();

    confirm_ch
        .queue_declare("bunny-rs.test.confirm-ch", QueueDeclareOptions::default())
        .await
        .unwrap();

    let confirm = confirm_ch
        .publish(
            "",
            "bunny-rs.test.confirm-ch",
            &PublishOptions::default(),
            b"typed-confirm",
        )
        .await
        .unwrap();
    confirm.wait().await.unwrap();

    confirm_ch
        .queue_delete("bunny-rs.test.confirm-ch", QueueDeleteOptions::default())
        .await
        .unwrap();
    confirm_ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_confirm_channel_seq_no() {
    let conn = connect().await;
    let ch = conn.open_channel().await.unwrap();
    let mut confirm_ch = ch.into_confirm_mode().await.unwrap();

    assert_eq!(confirm_ch.next_publish_seq_no(), 1);

    confirm_ch
        .queue_declare("bunny-rs.test.cc-seq", QueueDeclareOptions::default())
        .await
        .unwrap();

    confirm_ch
        .publish("", "bunny-rs.test.cc-seq", &PublishOptions::default(), b"a")
        .await
        .unwrap();
    assert_eq!(confirm_ch.next_publish_seq_no(), 2);

    confirm_ch
        .queue_delete("bunny-rs.test.cc-seq", QueueDeleteOptions::default())
        .await
        .unwrap();
    confirm_ch.close().await.unwrap();
    conn.close().await.unwrap();
}
