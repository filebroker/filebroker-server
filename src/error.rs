use std::fmt;

use serde::Serialize;
use thiserror::Error;
use warp::{
    http::{header, response, Response},
    hyper::StatusCode,
    reject::Reject,
    Rejection, Reply,
};

use crate::query::compiler;

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug, PartialEq)]
pub enum Error {
    // 400
    #[error("The request is invalid: {0}")]
    BadRequestError(String),
    #[error("The given value is not unique: '{0}'")]
    UniqueValueError(String),
    #[error("Failed to decode request header as valid utf8")]
    UtfEncodingError,
    #[error("The auth header is not formatted correctly (expected JWT 'Bearer ' header)")]
    InvalidAuthHeaderError,
    #[error("The request input could not be validated: '{0}'")]
    InvalidRequestInputError(String),
    #[error("The provided query is invalid: {0}")]
    IllegalQueryInputError(String),
    #[error("The provided S3 bucket is invalid. Error '{0}'.")]
    InvalidBucketError(String),
    #[error("The file upload form is invalid. {0}.")]
    InvalidFileError(String),
    #[error("No entity found for key: {0}")]
    InvalidEntityReferenceError(String),
    #[error("Query could not be compiled due to error in phase '{0}'")]
    QueryCompilationError(String, Vec<compiler::Error>),
    #[error("Captcha token is missing or malformed")]
    InvalidCaptchaError,
    #[error("Failed to validate captcha: {0}")]
    CaptchaValidationError(String),
    #[error("Password is too weak")]
    WeakPasswordError,
    #[error("Invalid username")]
    InvalidUserNameError,
    #[error("The provided token is invalid: {0}")]
    InvalidTokenError(String),
    #[error("This email address is already confirmed for this user")]
    EmailAlreadyConfirmedError,
    #[error("The query returned {0} results, exceeding the limit of {1}")]
    TooManyResultsError(u32, u32),
    #[error("Detected duplicate post in collection {0} and duplicate_mode set to reject")]
    DuplicatePostCollectionItemError(i64, Vec<i64>),
    #[error("Exceeded quota for broker, available quota: {0} bytes, remaining: {1} bytes")]
    QuotaExceededError(i64, i64),
    #[error("The Filebroker-Upload-Size header value does not match the actual upload size")]
    InvalidUploadSizeError,

    // 401
    #[error("invalid credentials")]
    InvalidCredentialsError,
    #[error("No auth header provided")]
    MissingAuthHeaderError,
    #[error("The JWT is not or no longer valid")]
    InvalidJwtError,
    #[error("The provided refresh token is invalid")]
    InvalidRefreshTokenError,

    // 403
    #[error("Cannot access object with provided pk {0}")]
    InaccessibleObjectError(i64),
    #[error("Cannot access object with provided key {0}")]
    InaccessibleS3ObjectError(String),
    #[error("Cannot access objects with provided pks {0:?}")]
    InaccessibleObjectsError(Vec<i64>),

    // 404
    #[error("The requested entity was not found")]
    NotFoundError,

    // 416
    #[error("The provided byte range is invalid: {1}")]
    IllegalRangeError(u64, String),

    // 500
    #[error("Could not establish database connection: {0}")]
    DatabaseConnectionError(String),
    #[error("There has been an error executing a query: '{0}'")]
    QueryError(String),
    #[error("There has been an error running a transaction: '{0}'")]
    TransactionError(diesel::result::Error),
    #[error("There has been an error creating the JWT token")]
    JwtCreationError,
    #[error("There has been an error encrypting / decrypting a password")]
    EncryptionError,
    #[error("Failed to serialise data: {0}")]
    SerialisationError(String),
    #[error("An error occurred connecting to S3: {0}")]
    S3Error(String),
    #[error("Error occurred in hyper: {0}")]
    HyperError(String),
    #[error("Error in ffmpeg process: {0}")]
    FfmpegProcessError(String),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Submitted task was aborted")]
    CancellationError,
    #[error("An IO Error occurred: {0}")]
    IoError(String),
    #[error("Invalid URL: {0}")]
    InvalidUrlError(String),
    #[error("Reqwest error occurred: {0}")]
    ReqwestError(String),
    #[error("Error occurred in child process: {0}")]
    ChildProcessError(String),

    #[error("Received error response code from S3: {0}")]
    S3ResponseError(u16),
    #[error("Received error response code from S3: {0}, Message: '{1}'")]
    S3ResponseErrorMsg(u16, String),
}

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::NotFoundError => StatusCode::NOT_FOUND,
            Error::InaccessibleObjectError(_)
            | Error::InaccessibleS3ObjectError(_)
            | Error::InaccessibleObjectsError(_) => StatusCode::FORBIDDEN,
            Error::InvalidCredentialsError
            | Error::MissingAuthHeaderError
            | Error::InvalidJwtError
            | Error::InvalidRefreshTokenError => StatusCode::UNAUTHORIZED,
            Error::BadRequestError(_)
            | Error::UniqueValueError(_)
            | Error::UtfEncodingError
            | Error::InvalidAuthHeaderError
            | Error::InvalidRequestInputError(_)
            | Error::IllegalQueryInputError(_)
            | Error::InvalidBucketError(_)
            | Error::InvalidFileError(_)
            | Error::InvalidEntityReferenceError(_)
            | Error::QueryCompilationError(..)
            | Error::InvalidCaptchaError
            | Error::CaptchaValidationError(_)
            | Error::WeakPasswordError
            | Error::InvalidUserNameError
            | Error::InvalidTokenError(_)
            | Error::EmailAlreadyConfirmedError
            | Error::TooManyResultsError(..)
            | Error::DuplicatePostCollectionItemError(..)
            | Error::QuotaExceededError(..)
            | Error::InvalidUploadSizeError => StatusCode::BAD_REQUEST,
            Error::DatabaseConnectionError(_)
            | Error::QueryError(_)
            | Error::TransactionError(_)
            | Error::JwtCreationError
            | Error::EncryptionError
            | Error::SerialisationError(_)
            | Error::S3Error(_)
            | Error::HyperError(_)
            | Error::FfmpegProcessError(_)
            | Error::InternalError(_)
            | Error::CancellationError
            | Error::IoError(_)
            | Error::InvalidUrlError(_)
            | Error::ReqwestError(_)
            | Error::ChildProcessError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::IllegalRangeError(..) => StatusCode::RANGE_NOT_SATISFIABLE,
            Error::S3ResponseError(code) | Error::S3ResponseErrorMsg(code, _) => {
                StatusCode::from_u16(*code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    pub fn error_code(&self) -> u32 {
        match self {
            Self::BadRequestError(_) => 400_001,
            Self::UniqueValueError(_) => 400_002,
            Self::UtfEncodingError => 400_003,
            Self::InvalidAuthHeaderError => 400_004,
            Self::InvalidRequestInputError(_) => 400_005,
            Self::IllegalQueryInputError(_) => 400_006,
            Self::InvalidBucketError(_) => 400_007,
            Self::InvalidFileError(_) => 400_008,
            Self::InvalidEntityReferenceError(_) => 400_009,
            Self::QueryCompilationError(..) => 400_010,
            Self::InvalidCaptchaError => 400_011,
            Self::CaptchaValidationError(_) => 400_012,
            Self::WeakPasswordError => 400_013,
            Self::InvalidUserNameError => 400_014,
            Self::InvalidTokenError(_) => 400_015,
            Self::EmailAlreadyConfirmedError => 400_016,
            Self::TooManyResultsError(..) => 400_017,
            Self::DuplicatePostCollectionItemError(..) => 400_018,
            Self::QuotaExceededError(..) => 400_019,
            Self::InvalidUploadSizeError => 400_020,

            Self::InvalidCredentialsError => 401_001,
            Self::MissingAuthHeaderError => 401_002,
            Self::InvalidJwtError => 401_003,
            Self::InvalidRefreshTokenError => 401_004,

            Self::InaccessibleObjectError(_) => 403_001,
            Self::InaccessibleS3ObjectError(_) => 403_002,
            Self::InaccessibleObjectsError(_) => 403_003,

            Self::NotFoundError => 404_001,

            Self::IllegalRangeError(..) => 416_001,

            Self::DatabaseConnectionError(_) => 500_001,
            Self::QueryError(_) => 500_002,
            Self::TransactionError(_) => 500_003,
            Self::JwtCreationError => 500_004,
            Self::EncryptionError => 500_005,
            Self::SerialisationError(_) => 500_006,
            Self::S3Error(_) => 500_007,
            Self::HyperError(_) => 500_008,
            Self::FfmpegProcessError(_) => 500_009,
            Self::InternalError(_) => 500_010,
            Self::CancellationError => 500_011,
            Self::IoError(_) => 500_012,
            Self::InvalidUrlError(_) => 500_013,
            Self::ReqwestError(_) => 500_014,
            Self::ChildProcessError(_) => 500_015,

            Self::S3ResponseError(_) => 600_001,
            Self::S3ResponseErrorMsg(..) => 600_002,
        }
    }

    pub fn modify_response(&self, builder: response::Builder) -> response::Builder {
        match self {
            Self::IllegalRangeError(size, _) => {
                builder.header(header::CONTENT_RANGE, format!("bytes */{size}"))
            }
            _ => builder,
        }
    }
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

impl From<warp::hyper::header::ToStrError> for Error {
    fn from(e: warp::hyper::header::ToStrError) -> Self {
        Error::HyperError(e.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Error::ReqwestError(value.without_url().to_string())
    }
}

#[derive(Debug)]
pub enum TransactionRuntimeError {
    Retry(Error),
    Rollback(Error),
}

impl fmt::Display for TransactionRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retry(e) => e.fmt(f),
            Self::Rollback(e) => e.fmt(f),
        }
    }
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
    error_code: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    compilation_errors: Option<Vec<compiler::Error>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duplicate_post_collection_items: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inaccessible_object_keys: Option<Vec<i64>>,
}

/// Creates a Rejection response for the given error and logs internal server errors.
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
    if let Some(e) = err.find::<Error>() {
        let status_code = e.status_code();
        let message = e.to_string();
        let error_code = e.error_code();

        if let StatusCode::INTERNAL_SERVER_ERROR = status_code {
            log::error!("Encountered internal server error: {}", e);
        }

        let compilation_errors = if let Error::QueryCompilationError(_, errors) = e {
            Some(
                errors
                    .iter()
                    .map(compiler::Error::clone)
                    .take(5)
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };

        let duplicate_post_collection_items =
            if let Error::DuplicatePostCollectionItemError(_, duplicates) = e {
                Some(duplicates.clone())
            } else {
                None
            };

        let inaccessible_object_keys = if let Error::InaccessibleObjectsError(keys) = e {
            Some(keys.clone())
        } else {
            None
        };

        let err_response = ErrorResponse {
            message,
            status: status_code.to_string(),
            error_code,
            compilation_errors,
            duplicate_post_collection_items,
            inaccessible_object_keys,
        };

        let response_builder = Response::builder()
            .status(status_code)
            .header(header::CONTENT_TYPE, "application/json");
        let response_builder = e.modify_response(response_builder);
        let response = response_builder
            .body(
                serde_json::to_vec(&err_response)
                    .map_err(|e| Error::SerialisationError(e.to_string()))?,
            )
            .map_err(|e| Error::SerialisationError(e.to_string()))?;

        Ok(response)
    } else {
        Err(err)
    }
}
