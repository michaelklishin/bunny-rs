// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::fmt;
use std::net::SocketAddr;

/// A host-port pair for connecting to a RabbitMQ node.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Endpoint with the default AMQP port (5672).
    pub fn from_host(host: impl Into<String>) -> Self {
        Self::new(host, 5672)
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// Resolves [`Endpoint`]s for connection attempts.
///
/// Modeled after the Java RabbitMQ client's `AddressResolver`:
/// - `List`: a fixed set of endpoints, returned as-is.
/// - `Dns`: expands a single hostname to all its A/AAAA records.
#[derive(Clone, Debug)]
pub enum AddressResolver {
    /// A fixed list of endpoints, returned as-is.
    List(Vec<Endpoint>),
    /// A single endpoint whose hostname is resolved via DNS to potentially
    /// multiple IP addresses.
    Dns(Endpoint),
}

impl AddressResolver {
    /// One endpoint becomes `Dns` (expanded via DNS); multiple become `List`.
    pub fn from_endpoints(mut endpoints: Vec<Endpoint>) -> Self {
        if endpoints.len() == 1 {
            AddressResolver::Dns(endpoints.pop().expect("len checked"))
        } else {
            AddressResolver::List(endpoints)
        }
    }

    /// Resolve without shuffling. Used for initial connection.
    pub async fn resolve(&self) -> Vec<Endpoint> {
        match self {
            AddressResolver::List(eps) => eps.clone(),
            AddressResolver::Dns(ep) => dns_resolve(ep).await,
        }
    }

    /// Resolve and shuffle. Used for recovery to spread load across nodes.
    pub async fn resolve_and_shuffle(&self) -> Vec<Endpoint> {
        let mut eps = self.resolve().await;
        shuffle(&mut eps);
        eps
    }
}

/// Resolves a hostname to all its IP addresses via `tokio::net::lookup_host`.
/// Falls back to the original endpoint if DNS resolution fails.
async fn dns_resolve(ep: &Endpoint) -> Vec<Endpoint> {
    match tokio::net::lookup_host(ep.to_string()).await {
        Ok(addrs) => {
            let resolved: Vec<Endpoint> = addrs
                .map(|addr: SocketAddr| Endpoint::new(addr.ip().to_string(), addr.port()))
                .collect();
            if resolved.is_empty() {
                vec![ep.clone()]
            } else {
                resolved
            }
        }
        Err(_) => vec![ep.clone()],
    }
}

/// Fisher-Yates shuffle using `fastrand`.
fn shuffle(endpoints: &mut [Endpoint]) {
    let len = endpoints.len();
    if len <= 1 {
        return;
    }
    for i in (1..len).rev() {
        let j = fastrand::usize(..=i);
        endpoints.swap(i, j);
    }
}
