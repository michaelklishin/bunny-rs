// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

#![cfg(feature = "tls")]

use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use super::Transport;

#[derive(Clone)]
pub struct TlsOptions {
    pub server_name: ServerName<'static>,
    pub connector: TlsConnector,
}

impl TlsOptions {
    /// TLS with platform-native certificate verification.
    pub fn new(server_name: &str) -> Result<Self, io::Error> {
        use rustls_platform_verifier::ConfigVerifierExt;

        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let config = rustls::ClientConfig::with_platform_verifier();

        Ok(Self {
            server_name,
            connector: TlsConnector::from(Arc::new(config)),
        })
    }

    /// TLS with custom CA (PEM).
    pub fn with_ca_pem(server_name: &str, ca_pem: &Path) -> Result<Self, io::Error> {
        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let ca_data = fs::read(ca_pem)?;
        let certs = rustls_pemfile::certs(&mut &ca_data[..])
            .collect::<Result<Vec<CertificateDer<'_>>, _>>()?;

        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(Self {
            server_name,
            connector: TlsConnector::from(Arc::new(config)),
        })
    }

    /// Mutual TLS using a CA bundle, public and private key files in the PEM format.
    pub fn mutual(
        server_name: &str,
        ca_pem: &Path,
        client_cert_pem: &Path,
        client_key_pem: &Path,
    ) -> Result<Self, io::Error> {
        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let ca_data = fs::read(ca_pem)?;
        let ca_certs = rustls_pemfile::certs(&mut &ca_data[..])
            .collect::<Result<Vec<CertificateDer<'_>>, _>>()?;

        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }

        let cert_data = fs::read(client_cert_pem)?;
        let client_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &cert_data[..])
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|c| c.into_owned())
            .collect();

        let key_data = fs::read(client_key_pem)?;
        let client_key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut &key_data[..])?
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no private key found"))?
            .clone_key();

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            server_name,
            connector: TlsConnector::from(Arc::new(config)),
        })
    }

    pub(crate) async fn connect(&self, tcp: TcpStream) -> Result<Transport, io::Error> {
        let tls_stream = self
            .connector
            .connect(self.server_name.clone(), tcp)
            .await?;
        Ok(Transport::Tls(Box::new(tls_stream)))
    }
}
