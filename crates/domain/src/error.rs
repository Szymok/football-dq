use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Extraction error: {0}")]
    ExtractionError(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, AppError>;
