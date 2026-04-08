// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

#[cfg(feature = "tls")]
mod tls {
    use std::path::PathBuf;

    use bunny_rs::SubscribeOptions;
    use bunny_rs::connection::{Connection, ConnectionOptions};
    use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
    use bunny_rs::transport::tls::TlsOptions;

    /// Resolve the TLS certificate directory.
    ///
    /// Checked in order:
    /// 1. `TLS_CERTS_DIR` env var (set by CI)
    /// 2. `tests/tls/certs/` relative to `CARGO_MANIFEST_DIR`
    /// 3. `~/Development/Opensource/tls-gen.git/basic/result` (local dev)
    fn tls_certs_dir() -> PathBuf {
        if let Ok(dir) = std::env::var("TLS_CERTS_DIR") {
            return PathBuf::from(dir);
        }

        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        let in_repo = PathBuf::from(&manifest).join("tests/tls/certs");
        if in_repo.exists() {
            return in_repo;
        }

        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        PathBuf::from(home).join("Development/Opensource/tls-gen.git/basic/result")
    }

    #[tokio::test]
    #[ignore = "requires TLS certificates and RabbitMQ TLS listener on port 5671"]
    async fn test_tls_connection() {
        let certs = tls_certs_dir();
        let ca_pem = certs.join("ca_certificate.pem");

        if !ca_pem.exists() {
            eprintln!("skipping TLS test: {} not found", ca_pem.display());
            return;
        }

        let tls = TlsOptions::with_ca_pem("localhost", &ca_pem).unwrap();

        let opts = ConnectionOptions {
            host: "localhost".into(),
            port: 5671,
            tls: Some(tls),
            ..Default::default()
        };

        let conn = Connection::open(opts).await.unwrap();
        assert!(conn.is_open());

        let mut ch = conn.open_channel().await.unwrap();
        ch.queue_declare("bunny-rs.test.tls-q", QueueDeclareOptions::default())
            .await
            .unwrap();
        ch.basic_publish(
            "",
            "bunny-rs.test.tls-q",
            &PublishOptions::default(),
            b"tls-hello",
        )
        .await
        .unwrap();
        ch.queue_delete("bunny-rs.test.tls-q", QueueDeleteOptions::default())
            .await
            .unwrap();
        ch.close().await.unwrap();
        conn.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires TLS certificates and RabbitMQ TLS listener on port 5671"]
    async fn test_tls_connection_via_uri() {
        let certs = tls_certs_dir();
        let ca_pem = certs.join("ca_certificate.pem");

        if !ca_pem.exists() {
            eprintln!("skipping TLS test: {} not found", ca_pem.display());
            return;
        }

        let tls = TlsOptions::with_ca_pem("localhost", &ca_pem).unwrap();
        let mut opts = ConnectionOptions::from_uri("amqps://guest:guest@localhost:5671/").unwrap();
        opts.tls = Some(tls);

        let conn = Connection::open(opts).await.unwrap();
        assert!(conn.is_open());
        conn.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires TLS certificates and RabbitMQ TLS listener on port 5671"]
    async fn test_mutual_tls() {
        let certs = tls_certs_dir();
        let ca_pem = certs.join("ca_certificate.pem");
        let client_cert = certs.join("client_certificate.pem");
        let client_key = certs.join("client_key.pem");

        if !ca_pem.exists() || !client_cert.exists() || !client_key.exists() {
            eprintln!("skipping mTLS test: certificates not found");
            return;
        }

        let tls = TlsOptions::mutual("localhost", &ca_pem, &client_cert, &client_key).unwrap();
        let opts = ConnectionOptions {
            host: "localhost".into(),
            port: 5671,
            tls: Some(tls),
            ..Default::default()
        };

        let conn = Connection::open(opts).await.unwrap();
        assert!(conn.is_open());
        conn.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires TLS certificates and RabbitMQ TLS listener on port 5671"]
    async fn test_tls_publish_consume() {
        let certs = tls_certs_dir();
        let ca_pem = certs.join("ca_certificate.pem");

        if !ca_pem.exists() {
            eprintln!("skipping TLS test: {} not found", ca_pem.display());
            return;
        }

        let tls = TlsOptions::with_ca_pem("localhost", &ca_pem).unwrap();
        let opts = ConnectionOptions {
            host: "localhost".into(),
            port: 5671,
            tls: Some(tls),
            ..Default::default()
        };

        let conn = Connection::open(opts).await.unwrap();
        let mut ch = conn.open_channel().await.unwrap();

        ch.queue_declare("bunny-rs.test.tls-pub-con", QueueDeclareOptions::default())
            .await
            .unwrap();

        let mut sub = ch
            .queue("bunny-rs.test.tls-pub-con")
            .subscribe(SubscribeOptions::manual_ack().prefetch(10))
            .await
            .unwrap();

        ch.basic_publish(
            "",
            "bunny-rs.test.tls-pub-con",
            &PublishOptions::default(),
            b"encrypted payload",
        )
        .await
        .unwrap();

        let delivery = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out")
            .expect("no delivery");

        assert_eq!(&delivery.body[..], b"encrypted payload");
        delivery.ack().await.unwrap();
        sub.cancel().await.unwrap();

        ch.queue_delete("bunny-rs.test.tls-pub-con", QueueDeleteOptions::default())
            .await
            .unwrap();
        ch.close().await.unwrap();
        conn.close().await.unwrap();
    }
}
