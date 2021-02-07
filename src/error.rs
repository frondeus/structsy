use persy::PersyError;
use std::fmt::{Display, Formatter};
use std::{error::Error, io::Error as IOError, sync::PoisonError};

#[derive(Debug)]
pub enum StructsyError {
    PersyError(PersyError),
    StructAlreadyDefined(String),
    StructNotDefined(String),
    IOError,
    PoisonedLock,
    MigrationNotSupported(String),
    InvalidId,
}

impl From<PersyError> for StructsyError {
    fn from(err: PersyError) -> StructsyError {
        StructsyError::PersyError(err)
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
        }
    }
}
