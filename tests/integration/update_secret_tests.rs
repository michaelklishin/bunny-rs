// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;

#[tokio::test]
async fn test_update_secret() {
    let conn = connect().await;
    // Re-send the same password; the server accepts it and replies with update-secret-ok.
    conn.update_secret("guest", "token refresh").await.unwrap();
    assert!(conn.is_open());
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_update_secret_twice() {
    let conn = connect().await;
    conn.update_secret("guest", "first refresh").await.unwrap();
    conn.update_secret("guest", "second refresh").await.unwrap();
    assert!(conn.is_open());
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_update_secret_on_closed_connection() {
    let conn = connect().await;
    conn.close().await.unwrap();

    let err = conn
        .update_secret("guest", "after close")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not connected"),
        "expected NotConnected, got: {err}"
    );
}
