//! Lease token and mode types.

use std::fmt;

/// Lease mode: shared (read) or exclusive (write).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseMode {
    /// Read shared — multiple holders allowed on non-overlapping conflicts.
    Shared,
    /// Write exclusive — no other holder allowed on conflicting keys.
    Exclusive,
}

impl LeaseMode {
    pub fn is_exclusive(self) -> bool {
        matches!(self, LeaseMode::Exclusive)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LeaseMode::Shared => "shared",
            LeaseMode::Exclusive => "exclusive",
        }
    }
}

impl fmt::Display for LeaseMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Strongly typed lease token, avoids confusion with plain strings.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseToken(String);

impl LeaseToken {
    pub fn new(s: String) -> Self {
        Self(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for LeaseToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for LeaseToken {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for LeaseToken {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
