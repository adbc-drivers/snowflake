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
        use sf_core::apis::database_driver_v1::ApiError;
        // Build an InvalidArgument error via the snafu builder pattern
        let err: ApiError = sf_core::apis::database_driver_v1::error::InvalidArgumentSnafu {
            argument: "test".to_string(),
        }
        .build();
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
