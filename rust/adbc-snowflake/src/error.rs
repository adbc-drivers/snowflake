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

// src/error.rs
use adbc_core::error::{Error, Status};
use sf_core::apis::database_driver_v1::ApiError;

pub(crate) fn api_error_to_adbc_error(err: ApiError) -> Error {
    let status = match &err {
        ApiError::InvalidArgument { .. } => Status::InvalidArguments,
        ApiError::Configuration { .. } => Status::InvalidArguments,
        ApiError::ConnectionNotInitialized { .. } => Status::InvalidState,
        ApiError::ConnectionLocking { .. } => Status::InvalidState,
        ApiError::StatementLocking { .. } => Status::InvalidState,
        ApiError::DatabaseLocking { .. } => Status::InvalidState,
        ApiError::InvalidRefreshState { .. } => Status::InvalidState,
        ApiError::Login { .. } => Status::Unauthenticated,
        ApiError::SessionRefresh { .. } => Status::Unauthenticated,
        ApiError::MasterTokenExpired { .. } => Status::Unauthenticated,
        ApiError::TlsClientCreation { .. } => Status::IO,
        ApiError::Query { .. } => Status::IO,
        ApiError::QueryResponseProcessing { .. } => Status::IO,
        ApiError::Statement { .. } => Status::IO,
        ApiError::RuntimeCreation { .. } => Status::IO,
        ApiError::GenericError { .. } => Status::IO,
    };
    Error::with_message_and_status(err.to_string(), status)
}

pub(crate) fn not_implemented(msg: &str) -> Error {
    Error::with_message_and_status(msg, Status::NotImplemented)
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    #[test]
    fn invalid_argument_maps_to_invalid_arguments() {
        use sf_core::apis::database_driver_v1::{DatabaseDriverV1, Handle};
        // Releasing a non-existent handle produces an InvalidArgument error
        let driver = DatabaseDriverV1::new();
        let bogus_handle = Handle { id: 999, magic: 0 };
        let err = driver.database_init(bogus_handle).unwrap_err();
        let adbc_err = api_error_to_adbc_error(err);
        assert_eq!(adbc_err.status, Status::InvalidArguments);
    }

    #[test]
    fn not_implemented_returns_correct_status() {
        let err = not_implemented("foo");
        assert_eq!(err.status, adbc_core::error::Status::NotImplemented);
        assert!(err.message.contains("foo"));
    }
}
