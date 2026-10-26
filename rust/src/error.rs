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

// src/error.rs
use std::os::raw::c_char;

use adbc_core::error::{Error, Status};
use sf_core::apis::database_driver_v1::ApiError;
use sf_core::rest::snowflake::{QUERY_CANCELED, RestError};

fn sqlstate_to_adbc(sqlstate: &str) -> Option<[c_char; 5]> {
    let bytes: [u8; 5] = sqlstate.as_bytes().try_into().ok()?;
    Some(bytes.map(|byte| byte as c_char))
}

fn is_snowflake_statement_timeout(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("statement or warehouse timeout") || message.contains("statement timeout")
}

/// Classify a server-side query failure into an ADBC status plus the vendor
/// code and SQLSTATE forwarded to the caller.
fn classify_query_failure(
    message: &str,
    code: Option<i32>,
    sql_state: Option<&str>,
) -> (Status, i32, [c_char; 5]) {
    let sqlstate = sql_state.and_then(sqlstate_to_adbc).unwrap_or([0; 5]);
    let server_cancel = code == Some(QUERY_CANCELED) || sql_state == Some("57014");
    let status = if server_cancel && is_snowflake_statement_timeout(message) {
        Status::Timeout
    } else if server_cancel {
        Status::Cancelled
    } else {
        Status::IO
    };
    (status, code.unwrap_or_default(), sqlstate)
}

pub(crate) fn api_error_to_adbc_error(err: ApiError) -> Error {
    let mut vendor_code = 0;
    let mut sqlstate = [0; 5];
    let status = match &err {
        ApiError::InvalidArgument { .. } => Status::InvalidArguments,
        ApiError::Configuration { .. } => Status::InvalidArguments,
        ApiError::ConnectionNotInitialized { .. } => Status::InvalidState,
        ApiError::ConnectionClosed { .. } => Status::InvalidState,
        ApiError::ConnectionLock { .. } => Status::InvalidState,
        ApiError::StatementLocking { .. } => Status::InvalidState,
        ApiError::DatabaseLocking { .. } => Status::InvalidState,
        ApiError::InvalidRefreshState { .. } => Status::InvalidState,
        ApiError::Login { .. } => Status::Unauthenticated,
        ApiError::SessionRefresh { .. } => Status::Unauthenticated,
        ApiError::MasterTokenTerminal { .. } => Status::Unauthenticated,
        ApiError::TlsClientCreation { .. } => Status::IO,
        ApiError::Query { source, .. } => {
            if let RestError::QueryFailed {
                message,
                code,
                sql_state,
                ..
            } = source.as_ref()
            {
                let (status, code, state) =
                    classify_query_failure(message, *code, sql_state.as_deref());
                vendor_code = code;
                sqlstate = state;
                status
            } else {
                Status::IO
            }
        }
        ApiError::QueryResponseProcess { .. } => Status::IO,
        ApiError::Statement { .. } => Status::IO,
        ApiError::RuntimeCreation { .. } => Status::IO,
        ApiError::TokenCacheInitialization { .. } => Status::IO,
        ApiError::ArrowParse { .. } => Status::IO,
        ApiError::ChunkFetch { .. } => Status::IO,
        ApiError::HttpRequest { .. } => Status::IO,
        ApiError::TokenRequest { .. } => Status::Unauthenticated,
        ApiError::QueryTimeout { .. } | ApiError::CancelTimeout { .. } => Status::Timeout,
        ApiError::Cancelled { .. } => Status::Cancelled,
        _ => Status::IO,
    };
    let mut error = Error::with_message_and_status(err.to_string(), status);
    error.vendor_code = vendor_code;
    error.sqlstate = sqlstate;
    error
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
    fn server_cancellation_preserves_vendor_code_and_sqlstate() {
        let (status, vendor_code, sqlstate) = classify_query_failure(
            "SQL execution canceled",
            Some(QUERY_CANCELED),
            Some("57014"),
        );

        assert_eq!(status, Status::Cancelled);
        assert_eq!(vendor_code, QUERY_CANCELED);
        assert_eq!(sqlstate, sqlstate_to_adbc("57014").unwrap());
    }

    #[test]
    fn cancellation_sqlstate_is_sufficient_without_vendor_code() {
        let (status, vendor_code, sqlstate) =
            classify_query_failure("query canceled by request", None, Some("57014"));

        assert_eq!(status, Status::Cancelled);
        assert_eq!(vendor_code, 0);
        assert_eq!(sqlstate, sqlstate_to_adbc("57014").unwrap());
    }

    #[test]
    fn snowflake_statement_timeout_is_distinguished_from_other_cancellation() {
        let (status, vendor_code, sqlstate) = classify_query_failure(
            "SQL execution canceled: Statement reached its statement or warehouse timeout of 1 second(s) and was canceled.",
            Some(QUERY_CANCELED),
            Some("57014"),
        );

        assert_eq!(status, Status::Timeout);
        assert_eq!(vendor_code, QUERY_CANCELED);
        assert_eq!(sqlstate, sqlstate_to_adbc("57014").unwrap());
    }

    #[test]
    fn other_server_query_failure_keeps_io_status_and_diagnostics() {
        let (status, vendor_code, sqlstate) =
            classify_query_failure("object does not exist", Some(2003), Some("42S02"));

        assert_eq!(status, Status::IO);
        assert_eq!(vendor_code, 2003);
        assert_eq!(sqlstate, sqlstate_to_adbc("42S02").unwrap());
    }

    #[test]
    fn malformed_server_sqlstate_is_not_forwarded() {
        let (_, _, sqlstate) = classify_query_failure("failure", Some(1), Some("TOO-LONG"));

        assert_eq!(sqlstate, [0; 5]);
    }

    #[test]
    fn not_implemented_returns_correct_status() {
        let err = not_implemented("foo");
        assert_eq!(err.status, adbc_core::error::Status::NotImplemented);
        assert!(err.message.contains("foo"));
    }
}
