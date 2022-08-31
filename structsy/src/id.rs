use crate::{Persistent, SRes, StructsyError};
use persy::PersyId;
use std::{hash::Hash, marker::PhantomData};
/// Reference to a record, can be used to load a record or to refer a record from another one.
#[derive(Eq, Ord)]
pub struct Ref<T> {
    pub(crate) type_name: String,
    pub(crate) raw_id: PersyId,
    pub(crate) ph: PhantomData<T>,
}

impl<T> Hash for Ref<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.type_name.hash(state);
        self.raw_id.hash(state);
    }
}

impl<T: Persistent> Ref<T> {
    pub(crate) fn new(persy_id: PersyId) -> Ref<T> {
        Ref {
            type_name: T::get_description().get_name(),
            raw_id: persy_id,
            ph: PhantomData,
        }
    }
}
impl<T> PartialEq for Ref<T> {
    fn eq(&self, other: &Self) -> bool {
        self.type_name == other.type_name && self.raw_id == other.raw_id
    }
}
impl<T> PartialOrd<Self> for Ref<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let c1 = self.type_name.cmp(&other.type_name);
        if c1 == std::cmp::Ordering::Equal {
            Some(self.raw_id.cmp(&other.raw_id))
        } else {
            Some(c1)
        }
    }
}
impl<T> Clone for Ref<T> {
    fn clone(&self) -> Self {
        Ref {
            type_name: self.type_name.clone(),
            raw_id: self.raw_id.clone(),
            ph: PhantomData,
        }
    }
}
impl<T> std::fmt::Debug for Ref<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "type: {} id :{:?}", self.type_name, self.raw_id)
    }
}

impl<T: Persistent> std::fmt::Display for Ref<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.type_name, self.raw_id)
    }
}

pub(crate) fn raw_format(type_name: &str, id: &PersyId) -> String {
    format!("{}@{}", type_name, id)
}

pub(crate) fn raw_parse(s: &str) -> SRes<(&str, &str)> {
    let mut split = s.split_terminator('@');
    let sty = split.next();
    let sid = split.next();
    if let (Some(ty), Some(id)) = (sty, sid) {
        Ok((ty, id))
    } else {
        Err(StructsyError::InvalidId)
    }
}

impl<T: Persistent> std::str::FromStr for Ref<T> {
    type Err = StructsyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (ty, id) = raw_parse(s)?;
        if ty != T::get_name() {
            Err(StructsyError::InvalidId)
        } else {
            Ok(Ref {
                type_name: T::get_name().to_string(),
                raw_id: id.parse().or(Err(StructsyError::InvalidId))?,
                ph: PhantomData,
            })
        }
    }
}
