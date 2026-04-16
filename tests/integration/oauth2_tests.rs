// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

//! Tests that exercise `connection.update-secret` with real JWT tokens
//! against a RabbitMQ node set up to use OAuth 2 for authN and authZ.
//!
//! Skipped by default. Set `RUN_OAUTH2_TESTS=1` to enable.
//! See `tests/oauth2/rabbitmq.conf` for the local RabbitMQ configuration.

use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header};

use bunny_rs::connection::{AuthMechanism, Connection, ConnectionEvent, ConnectionOptions};
use bunny_rs::credentials::{Credentials, CredentialsProvider, Password};
use bunny_rs::errors::BoxError;

fn should_run() -> bool {
    env::var("RUN_OAUTH2_TESTS").is_ok_and(|v| v == "1")
}

fn signing_key_pem() -> Vec<u8> {
    let path = env::var("OAUTH2_SIGNING_KEY_PATH")
        .unwrap_or_else(|_| "/tmp/oauth2-test-rmq/signing_key.pem".into());
    std::fs::read(&path).unwrap_or_else(|e| panic!("cannot read signing key at {path}: {e}"))
}

fn oauth2_port() -> u16 {
    env::var("OAUTH2_RABBITMQ_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5680)
}

/// Mint a JWT signed with RS256 matching the RabbitMQ OAuth 2 backend config.
fn mint_token(key_pem: &[u8], ttl: Duration) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let header = Header {
        alg: Algorithm::RS256,
        kid: Some("test-key".into()),
        ..Default::default()
    };

    let claims = serde_json::json!({
        "sub": "oauth2-test-user",
        "aud": "rabbitmq",
        "iss": "test-issuer",
        "scope": "rabbitmq.configure:*/* rabbitmq.read:*/* rabbitmq.write:*/*",
        "iat": now,
        "exp": now + ttl.as_secs(),
    });

    let key = EncodingKey::from_rsa_pem(key_pem).expect("invalid RSA PEM");
    jsonwebtoken::encode(&header, &claims, &key).expect("JWT encoding failed")
}

fn connect_opts(token: &str) -> ConnectionOptions {
    ConnectionOptions {
        host: "localhost".into(),
        port: oauth2_port(),
        // The OAuth 2 backend ignores the username from the SASL handshake
        // and extracts it from the JWT `sub` claim instead.
        username: "oauth2-test-user".into(),
        password: token.into(),
        virtual_host: "/".into(),
        auth_mechanism: AuthMechanism::Plain,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_oauth2_update_secret() {
    if !should_run() {
        eprintln!("skipping: set RUN_OAUTH2_TESTS=1 to run OAuth 2 tests");
        return;
    }

    let key = signing_key_pem();
    let token = mint_token(&key, Duration::from_secs(600));
    let conn = Connection::open(connect_opts(&token)).await.unwrap();
    assert!(conn.is_open());

    // Refresh with a new token.
    let new_token = mint_token(&key, Duration::from_secs(600));
    conn.update_secret(&new_token, "token refresh")
        .await
        .unwrap();
    assert!(conn.is_open());

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_oauth2_multiple_refreshes() {
    if !should_run() {
        eprintln!("skipping: set RUN_OAUTH2_TESTS=1 to run OAuth 2 tests");
        return;
    }

    let key = signing_key_pem();
    let token = mint_token(&key, Duration::from_secs(600));
    let conn = Connection::open(connect_opts(&token)).await.unwrap();

    for i in 0..3 {
        let refreshed = mint_token(&key, Duration::from_secs(600));
        conn.update_secret(&refreshed, &format!("refresh #{}", i + 1))
            .await
            .unwrap();
    }
    assert!(conn.is_open());
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_oauth2_invalid_token_rejected() {
    if !should_run() {
        eprintln!("skipping: set RUN_OAUTH2_TESTS=1 to run OAuth 2 tests");
        return;
    }

    let key = signing_key_pem();
    let token = mint_token(&key, Duration::from_secs(600));
    let conn = Connection::open(connect_opts(&token)).await.unwrap();

    // Send garbage as the new secret. The server should close the connection
    // with 530 NOT_ALLOWED.
    let result = conn.update_secret("not-a-valid-jwt", "bad token").await;
    assert!(result.is_err(), "expected error for invalid token");
}

/// A [`CredentialsProvider`] that mints fresh JWTs on every call.
struct JwtProvider {
    key_pem: Vec<u8>,
    ttl: Duration,
}

impl CredentialsProvider for JwtProvider {
    fn credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, BoxError>> + Send + '_>> {
        let token = mint_token(&self.key_pem, self.ttl);
        Box::pin(std::future::ready(Ok(Credentials {
            username: "oauth2-test-user".into(),
            password: Password::from(token),
            valid_until: Some(self.ttl),
        })))
    }
}

#[tokio::test]
async fn test_oauth2_credential_refresh_loop() {
    if !should_run() {
        eprintln!("skipping: set RUN_OAUTH2_TESTS=1 to run OAuth 2 tests");
        return;
    }

    // 5s TTL → refresh fires at 80% = 4s, leaving 1s headroom before expiry.
    let provider = Arc::new(JwtProvider {
        key_pem: signing_key_pem(),
        ttl: Duration::from_secs(5),
    });

    let opts = ConnectionOptions {
        host: "localhost".into(),
        port: oauth2_port(),
        virtual_host: "/".into(),
        auth_mechanism: AuthMechanism::Plain,
        credentials_provider: Some(provider),
        ..Default::default()
    };

    let conn = Connection::open(opts).await.unwrap();
    let mut events = conn.events();

    // Wait for the refresh loop to fire and the server to accept the new JWT.
    let got_event = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(ConnectionEvent::CredentialRefreshed) = events.recv().await {
                return;
            }
        }
    })
    .await;
    assert!(got_event.is_ok(), "expected CredentialRefreshed event");
    assert!(
        conn.is_open(),
        "connection should remain open after refresh"
    );

    conn.close().await.unwrap();
}
