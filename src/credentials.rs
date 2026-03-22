// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::fmt;

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
