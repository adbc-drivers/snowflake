// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// src/driver.rs
use std::sync::Arc;

use adbc_core::{
    error::{Error, Result, Status},
    options::{OptionDatabase, OptionValue},
    Optionable,
};
use arrow_schema::TimeUnit;
use sf_core::apis::database_driver_v1::DatabaseDriverV1;
use tokio::runtime::Runtime;

use crate::database::Database;

/// Controls the Arrow time unit used for Snowflake TIMESTAMP columns.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum TimestampPrecision {
    /// Nanosecond precision (default). May overflow for dates outside 1677–2262.
    #[default]
    Nanoseconds,
    /// Microsecond precision. Safe for all Snowflake-representable dates.
    Microseconds,
    /// Nanosecond precision; returns an error when a value would overflow.
    NanosecondsErrorOnOverflow,
}

impl TimestampPrecision {
    pub(crate) fn time_unit(self) -> TimeUnit {
        match self {
            Self::Microseconds => TimeUnit::Microsecond,
            _ => TimeUnit::Nanosecond,
        }
    }
}

pub(crate) struct Inner {
    pub runtime: Runtime,
    pub sf: DatabaseDriverV1,
}

impl Inner {
    fn new() -> Result<Self> {
        let runtime = Runtime::new().map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create tokio runtime: {e}"),
                Status::IO,
            )
        })?;
        Ok(Self {
            runtime,
            sf: DatabaseDriverV1::new(),
        })
    }
}

/// Snowflake ADBC Driver.
pub struct Driver {
    pub(crate) inner: Arc<Inner>,
}

impl Default for Driver {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::new().expect("failed to initialize driver")),
        }
    }
}

impl adbc_core::Driver for Driver {
    type DatabaseType = Database;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.new_database_with_opts(std::iter::empty())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let db_handle = self.inner.sf.database_new();
        let sf_settings: std::collections::HashMap<String, sf_core::config::settings::Setting> =
            Default::default();
        let mut db = Database {
            inner: self.inner.clone(),
            db_handle,
            sf_settings,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::default(),
        };
        for (key, value) in opts {
            db.set_option(key, value)?;
        }
        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Driver as _;

    #[test]
    fn driver_default_creates_successfully() {
        let _driver = Driver::default();
    }

    #[test]
    fn new_database_succeeds_with_no_options() {
        let mut driver = Driver::default();
        let _db = driver.new_database().expect("new_database failed");
    }
}
