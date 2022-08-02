use serde::Serialize;
use thiserror::Error;
use warp::{hyper::StatusCode, reject::Reject, Rejection, Reply};

use crate::query::compiler;

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug, PartialEq)]
pub enum Error {
    #[error("invalid credentials")]
    InvalidCredentialsError,
    #[error("Could not establish database connection")]
    DatabaseConnectionError,
    #[error("There has been an error executing a query: '{0}'")]
    QueryError(String),
    #[error("There has been an error running a transaction: '{0}'")]
    TransactionError(diesel::result::Error),
    #[error("There has been an error creating the JWT token")]
    JwtCreationError,
    #[error("There has been an error encrypting / decrypting a password")]
    EncryptionError,
    #[error("There already exists a principal with the given identifier: '{0}'")]
    UserExistsError(String),
    #[error("Failed to decode request header as valid utf8")]
    UtfEncodingError,
    #[error("The auth header is not formatted correctly (expected JWT 'Bearer ' header)")]
    InvalidAuthHeaderError,
    #[error("No auth header provided")]
    MissingAuthHeaderError,
    #[error("The request is not formatted correctly")]
    BadRequestError,
    #[error("The JWT is not or no longer valid")]
    InvalidJwtError,
    #[error("Failed to serialise data")]
    SerialisationError,
    #[error("The provided refresh token is invalid")]
    InvalidRefreshTokenError,
    #[error("The request input could not be validated: '{0}'")]
    InvalidRequestInputError(String),
    #[error("Query could not be compiled due to error in phase '{0}'")]
    QueryCompilationError(String, Vec<compiler::Error>),
    #[error("The provided query is invalid: {0}")]
    IllegalQueryInputError(String),
    #[error("Cannot access broker with provided pk {0}")]
    InaccessibleBrokerError(i32),
    #[error("Cannot access object with provided pk {0}")]
    InaccessibleObjectError(i32),
    #[error("The provided S3 bucket is invalid. Error '{0}'.")]
    InvalidBucketError(String),
    #[error("The file upload form is invalid. {0}.")]
    InvalidFileError(String),
    #[error("An error occured connecting to S3: {0}")]
    S3Error(String),
    #[error("Received error response code from S3: {0}")]
    S3ResponseError(u16),
    #[error("Received error response code from S3: {0}, Message: '{1}'")]
    S3ResponseErrorMsg(u16, String),
    #[error("The provided byte range is invalid: {0}")]
    IllegalRangeError(String),
    #[error("Error occurred in hyper: {0}")]
    HyperError(String),
}

impl Reject for Error {}

impl From<diesel::result::Error> for Error {
    fn from(e: diesel::result::Error) -> Self {
        Self::TransactionError(e)
    }
}

impl From<s3::error::S3Error> for Error {
    fn from(e: s3::error::S3Error) -> Self {
        match e {
            s3::error::S3Error::Http(code, msg) => Error::S3ResponseErrorMsg(code, msg),
            _ => Error::S3Error(e.to_string()),
        }
    }
}

impl From<warp::hyper::Error> for Error {
    fn from(e: warp::hyper::Error) -> Self {
        Error::HyperError(e.to_string())
    }
}

pub enum TransactionRuntimeError {
    Retry(Error),
    Rollback(Error),
}

impl From<Error> for TransactionRuntimeError {
    fn from(e: Error) -> Self {
        TransactionRuntimeError::Rollback(e)
    }
}

impl From<diesel::result::Error> for TransactionRuntimeError {
    fn from(e: diesel::result::Error) -> Self {
        match e {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::SerializationFailure,
                _,
            ) => TransactionRuntimeError::Retry(e.into()),
            _ => TransactionRuntimeError::Rollback(e.into()),
        }
    }
}

#[derive(Serialize, Debug)]
struct ErrorResponse {
    message: String,
    status: String,
    compilation_errors: Option<Vec<compiler::Error>>,
}

/// Creates a Rejection response for the given error and logs internal server errors.
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
    if let Some(e) = err.find::<Error>() {
        let (code, message, compilation_errors) = match e {
            Error::InaccessibleBrokerError(_) | Error::InaccessibleObjectError(_) => {
                (StatusCode::FORBIDDEN, e.to_string(), None)
            }
            Error::InvalidCredentialsError
            | Error::MissingAuthHeaderError
            | Error::InvalidJwtError
            | Error::InvalidRefreshTokenError => (StatusCode::UNAUTHORIZED, e.to_string(), None),
            Error::UserExistsError(_)
            | Error::UtfEncodingError
            | Error::InvalidAuthHeaderError
            | Error::BadRequestError
            | Error::InvalidRequestInputError(_)
            | Error::IllegalQueryInputError(_)
            | Error::InvalidBucketError(_)
            | Error::InvalidFileError(_) => (StatusCode::BAD_REQUEST, e.to_string(), None),
            Error::DatabaseConnectionError
            | Error::QueryError(_)
            | Error::TransactionError(_)
            | Error::JwtCreationError
            | Error::EncryptionError
            | Error::SerialisationError
            | Error::S3Error(_)
            | Error::HyperError(_) => {
                log::error!("Encountered internal server error: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string(), None)
            }
            Error::IllegalRangeError(_) => (StatusCode::RANGE_NOT_SATISFIABLE, e.to_string(), None),
            Error::QueryCompilationError(_, errors) => {
                let errors = errors
                    .iter()
                    .map(compiler::Error::clone)
                    .take(5)
                    .collect::<Vec<_>>();
                (StatusCode::BAD_REQUEST, e.to_string(), Some(errors))
            }
            Error::S3ResponseError(code) | Error::S3ResponseErrorMsg(code, _) => (
                StatusCode::from_u16(*code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
                None,
            ),
        };

        let err_response = ErrorResponse {
            message,
            status: code.to_string(),
            compilation_errors,
        };

        let json = warp::reply::json(&err_response);

        Ok(warp::reply::with_status(json, code))
    } else {
        Err(err)
    }
}
