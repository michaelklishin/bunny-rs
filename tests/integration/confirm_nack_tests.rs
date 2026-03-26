// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::connection::ConnectionError;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

#[tokio::test]
async fn test_confirm_pending_drained_on_close() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.confirm-drain",
        QueueDeclareOptions::default(),
    )
    .await
    .unwrap();

    // Publish some messages, collect confirm handles
    let mut confirms = Vec::new();
    for i in 0..5u8 {
        let c = ch
            .basic_publish(
                "",
                "bunny-rs.test.confirm-drain",
                &PublishOptions::default(),
                &[i],
            )
            .await
            .unwrap()
            .unwrap();
        confirms.push(c);
    }

    // Close the channel before waiting for confirms — the close()
    // should drain pending confirms with nack so callers get a
    // clean PublishNacked error.
    ch.close().await.unwrap();

    // At least some confirms should have resolved (either acked by broker
    // before close, or nacked by the drain). None should hang forever.
    for c in confirms {
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), c.wait()).await;
        assert!(result.is_ok(), "confirm should not hang after channel close");
        // The result is either Ok (broker acked before close) or
        // Err(PublishNacked) from the drain — both are acceptable.
    }

    let mut cleanup = conn.open_channel().await.unwrap();
    cleanup
        .queue_delete(
            "bunny-rs.test.confirm-drain",
            QueueDeleteOptions::default(),
        )
        .await
        .unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_to_nonexistent_exchange_with_confirm() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();

    // Publishing to a non-existent exchange causes a channel error
    // (channel.close from broker), which should resolve pending confirms.
    let confirm = ch
        .basic_publish(
            "bunny-rs.test.no-such-exchange",
            "key",
            &PublishOptions::default(),
            b"payload",
        )
        .await;

    match confirm {
        Ok(Some(c)) => {
            // The confirm should resolve (likely with an error since
            // the broker closes the channel).
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(5), c.wait()).await;
            assert!(result.is_ok(), "confirm should resolve, not hang");
            // It's expected to be a nack/error since the channel was closed
            match result.unwrap() {
                Err(ConnectionError::PublishNacked | ConnectionError::ChannelNotOpen) => {}
                Ok(()) => {} // broker may have acked before processing
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        Ok(None) => panic!("expected confirm handle in confirm mode"),
        Err(_) => {} // Channel already closed — acceptable
    }

    conn.close().await.unwrap();
}
