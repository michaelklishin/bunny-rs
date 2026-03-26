// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::connection::ConnectionEvent;
use bunny_rs::options::PublishOptions;

fn rabbitmqctl(args: &[&str]) -> bool {
    let cmd =
        std::env::var("BUNNY_RS_RABBITMQCTL").unwrap_or_else(|_| "rabbitmqctl".into());
    std::process::Command::new(&cmd)
        .args(args)
        .current_dir(std::env::temp_dir())
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Test that connection.blocked and connection.unblocked events are received
/// when the broker triggers a memory alarm.
#[tokio::test]
#[ignore = "manipulates broker memory watermark, may affect other tests"]
async fn test_connection_blocked_unblocked() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let mut events = conn.events();

    assert!(!conn.is_blocked().await);

    // Trigger memory alarm by setting watermark extremely low
    assert!(
        rabbitmqctl(&["set_vm_memory_high_watermark", "0.0001"]),
        "failed to set memory watermark — set BUNNY_RS_RABBITMQCTL to the absolute path"
    );

    // The broker sends connection.blocked when a publisher tries to write.
    // Spawn a publish to trigger it.
    let conn2 = conn.clone();
    tokio::spawn(async move {
        let mut pub_ch = conn2.open_channel().await.unwrap();
        // This publish will block until the alarm clears
        let _ = pub_ch
            .basic_publish("", "amq.rabbitmq.reply-to", &PublishOptions::default(), b"x")
            .await;
    });

    // Wait for Blocked event
    let blocked = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match events.recv().await {
                Ok(ConnectionEvent::Blocked(reason)) => return reason,
                Ok(_) => continue,
                Err(e) => panic!("event error: {e}"),
            }
        }
    })
    .await
    .expect("timed out waiting for Blocked event");

    assert!(!blocked.is_empty());
    assert!(conn.is_blocked().await);
    assert!(conn.blocked_reason().await.is_some());

    // Restore normal watermark to unblock
    assert!(
        rabbitmqctl(&["set_vm_memory_high_watermark", "0.4"]),
        "failed to restore memory watermark"
    );

    // Wait for Unblocked event
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match events.recv().await {
                Ok(ConnectionEvent::Unblocked) => return,
                Ok(_) => continue,
                Err(e) => panic!("event error: {e}"),
            }
        }
    })
    .await
    .expect("timed out waiting for Unblocked event");

    assert!(!conn.is_blocked().await);
    assert!(conn.blocked_reason().await.is_none());

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
