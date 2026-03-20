// Copyright (c) 2026 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// src/driver.rs
use std::sync::Arc;

use adbc_core::{
    Optionable,
    error::{Error, Result, Status},
    options::{OptionDatabase, OptionValue},
};
use sf_core::apis::database_driver_v1::DatabaseDriverV1;
use tokio::runtime::Runtime;

use crate::database::Database;

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
        let mut sf_settings: std::collections::HashMap<String, sf_core::config::settings::Setting> =
            Default::default();
        sf_settings.insert(
            "client_app_id".to_string(),
            sf_core::config::settings::Setting::String(
                concat!("[ADBC][Rust] Snowflake Driver/", env!("CARGO_PKG_VERSION")).to_string(),
            ),
        );
        let mut db = Database {
            inner: self.inner.clone(),
            db_handle,
            sf_settings,
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
