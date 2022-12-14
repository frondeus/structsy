use persy::{PersyError, PE};
use std::fmt::{Display, Formatter};
use std::{error::Error, io::Error as IOError, sync::PoisonError};
/// All the possible Structsy errors
#[derive(Debug)]
pub enum StructsyError {
    PersyError(PersyError),
    StructAlreadyDefined(String),
    StructNotDefined(String),
    IOError,
    PoisonedLock,
    MigrationNotSupported(String),
    InvalidId,
    ValueChangeError(String),
    TypeError(String),
}

impl<T: Into<PersyError>> From<PE<T>> for StructsyError {
    fn from(err: PE<T>) -> StructsyError {
        StructsyError::PersyError(err.error().into())
    }
}
impl<T> From<PoisonError<T>> for StructsyError {
    fn from(_err: PoisonError<T>) -> StructsyError {
        StructsyError::PoisonedLock
    }
}

impl From<IOError> for StructsyError {
    fn from(_err: IOError) -> StructsyError {
        StructsyError::IOError
    }
}

pub type SRes<T> = Result<T, StructsyError>;

impl Error for StructsyError {}

impl Display for StructsyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StructsyError::PersyError(pe) => write!(f, "Persy Error: {}", pe),
            StructsyError::StructAlreadyDefined(name) => write!(f, "Struct with name '{}' already defined ", name),
            StructsyError::StructNotDefined(name) => writeln!(f, "Struct with name '{}', already defined", name),
            StructsyError::IOError => writeln!(f, "IOError"),
            StructsyError::PoisonedLock => writeln!(f, "PoisonedLock"),
            StructsyError::MigrationNotSupported(name) => writeln!(f, "Migration of Struct '{}' not supported", name),
            StructsyError::InvalidId => writeln!(f, "Invalid ID"),
            StructsyError::ValueChangeError(message) => writeln!(f, "Value change: {}", message),
            StructsyError::TypeError(message) => writeln!(f, "Type Error : {}", message),
        }
    }
}
