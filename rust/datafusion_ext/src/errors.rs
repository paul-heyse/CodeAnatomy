use std::error::Error;
use std::fmt::{self, Debug};

use datafusion::arrow::error::ArrowError;
use datafusion_common::DataFusionError;

#[derive(Debug)]
pub enum ExtError {
    DataFusion(Box<DataFusionError>),
    Arrow(ArrowError),
    Generic(String),
    Delta(String),
    Plugin(String),
}

impl fmt::Display for ExtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtError::DataFusion(err) => write!(f, "DataFusion error: {err:?}"),
            ExtError::Arrow(err) => write!(f, "Arrow error: {err:?}"),
            ExtError::Generic(message) => write!(f, "{message}"),
            ExtError::Delta(message) => write!(f, "Delta error: {message}"),
            ExtError::Plugin(message) => write!(f, "Plugin error: {message}"),
        }
    }
}

impl Error for ExtError {}

crate::impl_error_from!(ExtError, ArrowError, Arrow);
crate::impl_error_from!(ExtError, DataFusionError, DataFusion, |err| Box::new(err));
crate::impl_error_from!(ExtError, deltalake::errors::DeltaTableError, Delta, |err| {
    err.to_string()
});

pub type ExtResult<T> = std::result::Result<T, ExtError>;

pub fn to_datafusion_err(error: impl Debug) -> DataFusionError {
    DataFusionError::Execution(format!("{error:?}"))
}
