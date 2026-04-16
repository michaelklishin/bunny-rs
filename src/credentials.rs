// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::errors::BoxError;

/// String wrapper that zeroes memory on drop when the `zeroize` feature is enabled.
///
/// ```
/// use bunny_rs::credentials::Password;
///
/// let password = Password::from("s3cret");
/// assert_eq!(password.as_str(), "s3cret");
/// ```
#[cfg(feature = "zeroize")]
#[derive(Clone, zeroize::Zeroize, zeroize::ZeroizeOnDrop)]
pub struct Password(String);

#[cfg(not(feature = "zeroize"))]
#[derive(Clone)]
pub struct Password(String);

impl Password {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Password([REDACTED])")
    }
}

impl From<String> for Password {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Password {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Credentials returned by a [`CredentialsProvider`].
pub struct Credentials {
    pub username: String,
    pub password: Password,
    /// Time until these credentials expire. `None` means they never expire.
    pub valid_until: Option<Duration>,
}

/// Async source of credentials that may expire.
///
/// Implementations must be `Send + Sync + 'static` so the provider can be
/// shared across the connection's background tasks via `Arc`.
///
/// Called once at connection time (and during recovery) to obtain credentials
/// for the AMQP handshake. When `valid_until` is `Some`, a background loop
/// periodically calls the provider again and sends `connection.update-secret`
/// to the broker before the token expires.
pub trait CredentialsProvider: Send + Sync + 'static {
    /// Fetch current or refreshed credentials.
    fn credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, BoxError>> + Send + '_>>;
}

/// A [`CredentialsProvider`] that always returns the same credentials.
pub struct StaticCredentialsProvider {
    username: String,
    password: Password,
}

impl StaticCredentialsProvider {
    pub fn new(username: impl Into<String>, password: impl Into<Password>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl CredentialsProvider for StaticCredentialsProvider {
    fn credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, BoxError>> + Send + '_>> {
        Box::pin(std::future::ready(Ok(Credentials {
            username: self.username.clone(),
            password: self.password.clone(),
            valid_until: None,
        })))
    }
}
