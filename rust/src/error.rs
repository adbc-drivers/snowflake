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
    // Go's SQLSTATE mapping is applied first; vendor codes override it.
    let mut status = match sql_state {
        Some("42S02") => Status::NotFound,
        Some("42710") => Status::AlreadyExists,
        Some("08004") => Status::Unauthorized,
        _ => Status::IO,
    };
    status = match code {
        Some(1003 | 100383) => Status::InvalidArguments,
        Some(2003) => Status::NotFound,
        _ => status,
    };
    // Server cancellation and statement timeout are distinguished by the
    // server message, even when another diagnostic code is also present.
    if server_cancel {
        status = if is_snowflake_statement_timeout(message) {
            Status::Timeout
        } else {
            Status::Cancelled
        };
    }
    (status, code.unwrap_or_default(), sqlstate)
}

fn format_query_failure_message(message: &str, query_id: Option<&str>) -> String {
    match query_id.filter(|id| !id.is_empty()) {
        Some(id) => format!("[snowflake] {message} (query ID: {id})"),
        None => format!("[snowflake] {message}"),
    }
}

pub(crate) fn api_error_to_adbc_error(err: ApiError) -> Error {
    let mut vendor_code = 0;
    let mut sqlstate = [0; 5];
    let mut query_message = None;
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
                ids,
                ..
            } = source.as_ref()
            {
                let (status, code, state) =
                    classify_query_failure(message, *code, sql_state.as_deref());
                vendor_code = code;
                sqlstate = state;
                query_message = Some(format_query_failure_message(
                    message,
                    ids.query_id.as_deref(),
                ));
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
    let mut error = Error::with_message_and_status(
        query_message.unwrap_or_else(|| message_with_causes(&err)),
        status,
    );
    error.vendor_code = vendor_code;
    error.sqlstate = sqlstate;
    error
}

/// Render an error with its underlying causes. sf_core's messages often omit
/// their source (e.g. "HTTP request failed after retries: login request"
/// hides the transport error), so append each cause not already included.
fn message_with_causes(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut cause = err.source();
    while let Some(source) = cause {
        let text = source.to_string();
        if !text.is_empty() && !message.contains(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        cause = source.source();
    }
    message
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
    fn go_sqlstate_and_vendor_code_mappings() {
        let cases = [
            ("table not found", None, Some("42S02"), Status::NotFound),
            ("object exists", None, Some("42710"), Status::AlreadyExists),
            (
                "connection rejected",
                None,
                Some("08004"),
                Status::Unauthorized,
            ),
            ("syntax error", Some(1003), None, Status::InvalidArguments),
            ("object not found", Some(2003), None, Status::NotFound),
            (
                "invalid geometry",
                Some(100383),
                None,
                Status::InvalidArguments,
            ),
            // A vendor code wins over a conflicting SQLSTATE, as in Go.
            (
                "syntax error",
                Some(1003),
                Some("42710"),
                Status::InvalidArguments,
            ),
            ("other error", Some(1), Some("22000"), Status::IO),
        ];
        for (message, code, sql_state, expected) in cases {
            let (status, vendor_code, sqlstate) = classify_query_failure(message, code, sql_state);
            assert_eq!(status, expected, "{message}");
            assert_eq!(vendor_code, code.unwrap_or_default(), "{message}");
            assert_eq!(
                sqlstate,
                sql_state.and_then(sqlstate_to_adbc).unwrap_or([0; 5]),
                "{message}"
            );
        }
    }

    #[test]
    fn query_failure_message_includes_query_id_when_present() {
        assert_eq!(
            format_query_failure_message("syntax error", Some("01abcdef")),
            "[snowflake] syntax error (query ID: 01abcdef)"
        );
        assert_eq!(
            format_query_failure_message("syntax error", None),
            "[snowflake] syntax error"
        );
        assert_eq!(
            format_query_failure_message("syntax error", Some("")),
            "[snowflake] syntax error"
        );
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

    #[test]
    fn messages_include_causes_not_already_in_the_text() {
        #[derive(Debug)]
        struct Layer(&'static str, Option<Box<Layer>>);
        impl std::fmt::Display for Layer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Layer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1.as_deref().map(|e| e as _)
            }
        }
        // The middle layer already embeds its cause, so it is not repeated.
        let err = Layer(
            "Failed to login: HTTP request failed after retries: login request",
            Some(Box::new(Layer(
                "connect error: dns error",
                Some(Box::new(Layer("dns error", None))),
            ))),
        );
        assert_eq!(
            message_with_causes(&err),
            "Failed to login: HTTP request failed after retries: login request: \
             connect error: dns error"
        );
    }
}
