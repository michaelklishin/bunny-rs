// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

#[cfg(feature = "tls")]
mod tls {
    use std::path::PathBuf;

    use bunny_rs::connection::{Connection, ConnectionOptions};
    use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
    use bunny_rs::transport::tls::TlsOptions;

    fn tls_certs_dir() -> PathBuf {
        if let Ok(dir) = std::env::var("TLS_CERTS_DIR") {
            PathBuf::from(dir)
        } else {
            dirs_for_tls_gen()
        }
    }

    fn dirs_for_tls_gen() -> PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        PathBuf::from(home).join("Development/Opensource/tls-gen.git/basic/result")
    }

    #[tokio::test]
    #[ignore]
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
    #[ignore]
    async fn test_tls_connection_via_uri() {
        let certs = tls_certs_dir();
        let ca_pem = certs.join("ca_certificate.pem");

        if !ca_pem.exists() {
            eprintln!("skipping TLS test: {} not found", ca_pem.display());
            return;
        }

        // amqps:// URI triggers TLS with system roots by default,
        // but we need the custom CA for self-signed certs
        let tls = TlsOptions::with_ca_pem("localhost", &ca_pem).unwrap();
        let mut opts = ConnectionOptions::from_uri("amqps://guest:guest@localhost:5671/").unwrap();
        opts.tls = Some(tls);

        let conn = Connection::open(opts).await.unwrap();
        assert!(conn.is_open());
        conn.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
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
}
