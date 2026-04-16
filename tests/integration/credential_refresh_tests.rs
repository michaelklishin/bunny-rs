// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bunny_rs::connection::{Connection, ConnectionEvent, ConnectionOptions};
use bunny_rs::credentials::{Credentials, CredentialsProvider, Password};
use bunny_rs::errors::BoxError;

use crate::test_helpers::amqp_uri;

struct TestProvider {
    ttl: Duration,
    call_count: AtomicU32,
}

impl TestProvider {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            call_count: AtomicU32::new(0),
        }
    }
}

impl CredentialsProvider for TestProvider {
    fn credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, BoxError>> + Send + '_>> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        Box::pin(std::future::ready(Ok(Credentials {
            username: "guest".into(),
            password: Password::from("guest"),
            valid_until: Some(self.ttl),
        })))
    }
}

/// Provider that fails N calls after the initial handshake call, then succeeds.
struct FlakeyProvider {
    /// Number of refresh calls that should fail (handshake call always succeeds).
    refresh_failures: u32,
    ttl: Duration,
    call_count: AtomicU32,
}

impl FlakeyProvider {
    fn new(refresh_failures: u32, ttl: Duration) -> Self {
        Self {
            refresh_failures,
            ttl,
            call_count: AtomicU32::new(0),
        }
    }
}

impl CredentialsProvider for FlakeyProvider {
    fn credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, BoxError>> + Send + '_>> {
        let n = self.call_count.fetch_add(1, Ordering::Relaxed);
        // Call #0 is the handshake and always succeeds.
        // Calls #1..refresh_failures fail to exercise retry logic.
        if n >= 1 && n <= self.refresh_failures {
            return Box::pin(std::future::ready(Err("transient error".into())));
        }
        Box::pin(std::future::ready(Ok(Credentials {
            username: "guest".into(),
            password: Password::from("guest"),
            valid_until: Some(self.ttl),
        })))
    }
}

#[tokio::test]
async fn test_connect_with_static_provider() {
    let provider = Arc::new(TestProvider::new(Duration::from_secs(3600)));
    let mut opts = ConnectionOptions::from_uri(&amqp_uri()).unwrap();
    opts.credentials_provider = Some(provider.clone());

    let conn = Connection::open(opts).await.unwrap();
    assert!(conn.is_open());
    // Provider was called once for the initial handshake.
    assert!(provider.call_count.load(Ordering::Relaxed) >= 1);
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_credential_refresh_fires() {
    let provider = Arc::new(TestProvider::new(Duration::from_secs(2)));
    let mut opts = ConnectionOptions::from_uri(&amqp_uri()).unwrap();
    opts.credentials_provider = Some(provider.clone());

    let conn = Connection::open(opts).await.unwrap();
    let mut events = conn.events();

    // Wait for the CredentialRefreshed event (should fire at ~80% of 2s = ~1.6s).
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(ConnectionEvent::CredentialRefreshed) = events.recv().await {
                return;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "expected CredentialRefreshed event");
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_credential_refresh_multiple_cycles() {
    let provider = Arc::new(TestProvider::new(Duration::from_secs(2)));
    let mut opts = ConnectionOptions::from_uri(&amqp_uri()).unwrap();
    opts.credentials_provider = Some(provider.clone());

    let conn = Connection::open(opts).await.unwrap();
    let mut events = conn.events();

    // Wait for at least 2 refresh events.
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let mut count = 0u32;
        loop {
            if let Ok(ConnectionEvent::CredentialRefreshed) = events.recv().await {
                count += 1;
                if count >= 2 {
                    return;
                }
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "expected at least 2 CredentialRefreshed events"
    );
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_credential_refresh_stops_on_close() {
    let provider = Arc::new(TestProvider::new(Duration::from_secs(2)));
    let mut opts = ConnectionOptions::from_uri(&amqp_uri()).unwrap();
    opts.credentials_provider = Some(provider.clone());

    let conn = Connection::open(opts).await.unwrap();
    conn.close().await.unwrap();

    // Give the refresh loop a chance to notice the connection closed.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // No panic, no error — the loop exited cleanly.
    let calls_after_close = provider.call_count.load(Ordering::Relaxed);
    // Should not keep calling the provider after close.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        provider.call_count.load(Ordering::Relaxed),
        calls_after_close,
        "provider should not be called after connection close"
    );
}

#[tokio::test]
async fn test_credential_provider_error_retries() {
    // The first refresh call (#1) and second (#2) fail; the third retry (#3) succeeds.
    // Call #0 (handshake) always succeeds.
    let provider = Arc::new(FlakeyProvider::new(2, Duration::from_secs(2)));
    let mut opts = ConnectionOptions::from_uri(&amqp_uri()).unwrap();
    opts.credentials_provider = Some(provider.clone());

    let conn = Connection::open(opts).await.unwrap();
    let mut events = conn.events();

    // The first refresh attempt (call #1) will fail, retry (call #2) will fail,
    // but the third retry (call #3) should succeed.
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(ConnectionEvent::CredentialRefreshed) = events.recv().await {
                return;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "expected CredentialRefreshed after retries");
    conn.close().await.unwrap();
}
