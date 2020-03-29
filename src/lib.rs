//! Simple single file struct persistence manger
//!
//!
//! # Example
//!
//! ```
//! use structsy::{Structsy, StructsyError, StructsyTx};
//! use structsy_derive::Persistent;
//! # use structsy::SRes;
//! # fn foo() -> SRes<()> {
//! #[derive(Persistent)]
//! struct MyData {
//!     name: String,
//!     address: String,
//! }
//! // ......
//! let db = Structsy::open("my_data.db")?;
//! db.define::<MyData>()?;
//!
//!  let my_data = MyData {
//!    name: "Structsy".to_string(),
//!    address: "https://gitlab.com/tglman/structsy".to_string(),
//! };
//! let mut tx = db.begin()?;
//! tx.insert(&my_data)?;
//! db.commit(tx)?;
//! # Ok(())
//! # }
//!```
//!
//!
pub use persy::ValueMode;
use persy::{Config, IndexType, Persy, PersyError, PersyId, Transaction};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Cursor, Error as IOError, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
mod format;
pub use format::PersistentEmbedded;
mod desc;
use desc::InternalDescription;
pub use desc::{FieldDescription, StructDescription};
mod index;
pub use index::{
    find, find_range, find_range_tx, find_tx, find_unique, find_unique_range, find_unique_range_tx, find_unique_tx,
    IndexableValue, RangeIterator, UniqueRangeIterator,
};
mod filter;
pub use filter::{FieldConditionType, FilterBuilder};

const INTERNAL_SEGMENT_NAME: &str = "__#internal";

pub struct StructsyIter<T: Persistent> {
    iterator: Box<dyn Iterator<Item = (Ref<T>, T)>>,
}

impl<T: Persistent> StructsyIter<T> {
    pub fn new<I>(iterator: I) -> StructsyIter<T>
    where
        I: Iterator<Item = (Ref<T>, T)>,
        I: 'static,
    {
        StructsyIter {
            iterator: Box::new(iterator),
        }
    }
}

impl<T: Persistent> Iterator for StructsyIter<T> {
    type Item = (Ref<T>, T);

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}

pub struct StructsyIntoIter<T: Persistent + 'static> {
    structsy: Structsy,
    builder: FilterBuilder<T>,
}
impl<T: Persistent> StructsyIntoIter<T> {
    pub fn new(structsy: Structsy, builder: FilterBuilder<T>) -> StructsyIntoIter<T> {
        StructsyIntoIter { builder, structsy }
    }
}

impl<T: Persistent> IntoIterator for StructsyIntoIter<T> {
    type Item = (Ref<T>, T);
    type IntoIter = StructsyIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        StructsyIter::new(self.builder.finish(&self.structsy))
    }
}

pub struct StructsyInto<T> {
    t: T,
}

impl<T> StructsyInto<T> {
    pub fn into(self) -> T {
        self.t
    }
}

pub type IterResult<T> = Result<StructsyIntoIter<T>, StructsyError>;
pub type FirstResult<T> = Result<StructsyInto<T>, StructsyError>;

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

struct StructsyImpl {
    persy: Persy,
    definitions: Mutex<HashMap<String, InternalDescription>>,
}

/// Main API to persist structs with structsy.
///
///
#[derive(Clone)]
pub struct Structsy {
    structsy_impl: Arc<StructsyImpl>,
}

/// Trait for description of embedded structs, automatically generated by structsy_derive.
pub trait EmbeddedDescription: PersistentEmbedded {
    fn get_description() -> StructDescription;
}

/// Trait implemented by persistent struct, implementation automatically generated by
/// structsy_derive.
pub trait Persistent {
    fn get_name() -> &'static str;
    fn get_description() -> StructDescription;
    fn write(&self, write: &mut dyn Write) -> SRes<()>;
    fn read(read: &mut dyn Read) -> SRes<Self>
    where
        Self: std::marker::Sized;
    fn declare(db: &mut dyn Sytx) -> SRes<()>;
    fn put_indexes(&self, tx: &mut dyn Sytx, id: &Ref<Self>) -> SRes<()>
    where
        Self: std::marker::Sized;
    fn remove_indexes(&self, tx: &mut dyn Sytx, id: &Ref<Self>) -> SRes<()>
    where
        Self: std::marker::Sized;
}

pub fn declare_index<T: IndexType>(db: &mut dyn Sytx, name: &str, mode: ValueMode) -> SRes<()> {
    db.tx().trans.create_index::<T, PersyId>(name, mode)?;
    Ok(())
}

/// Reference to a record, can be used to load a record or to refer a record from another one.
#[derive(Eq, Ord)]
pub struct Ref<T> {
    type_name: String,
    raw_id: PersyId,
    ph: PhantomData<T>,
}

impl<T: Persistent> Ref<T> {
    fn new(persy_id: PersyId) -> Ref<T> {
        Ref {
            type_name: T::get_description().name.clone(),
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
impl<T: Persistent> std::str::FromStr for Ref<T> {
    type Err = StructsyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split_terminator("@");
        let sty = split.next();
        let sid = split.next();
        if let (Some(ty), Some(id)) = (sty, sid) {
            if ty != T::get_name() {
                Err(StructsyError::InvalidId)
            } else {
                Ok(Ref {
                    type_name: T::get_name().to_string(),
                    raw_id: id.parse().or(Err(StructsyError::InvalidId))?,
                    ph: PhantomData,
                })
            }
        } else {
            Err(StructsyError::InvalidId)
        }
    }
}

/// Owned transation to use with [`StructsyTx`] trait
///
/// [`StructsyTx`]: trait.StructsyTx.html
pub struct OwnedSytx {
    structsy_impl: Arc<StructsyImpl>,
    trans: Transaction,
}

/// Reference transaction to use with [`StructsyTx`] trait
///
/// [`StructsyTx`]: trait.StructsyTx.html
pub struct RefSytx<'a> {
    structsy_impl: Arc<StructsyImpl>,
    trans: &'a mut Transaction,
}

/// Internal use transaction reference
pub struct TxRef<'a> {
    trans: &'a mut Transaction,
}

/// Internal use implementation reference
pub struct ImplRef {
    structsy_impl: Arc<StructsyImpl>,
}

/// Base transaction trait for internal use.
pub trait Sytx {
    fn tx(&mut self) -> TxRef;
    fn structsy(&self) -> ImplRef;
}

impl Sytx for OwnedSytx {
    fn tx(&mut self) -> TxRef {
        TxRef { trans: &mut self.trans }
    }
    fn structsy(&self) -> ImplRef {
        ImplRef {
            structsy_impl: self.structsy_impl.clone(),
        }
    }
}

impl<'a> Sytx for RefSytx<'a> {
    fn tx(&mut self) -> TxRef {
        TxRef { trans: self.trans }
    }
    fn structsy(&self) -> ImplRef {
        ImplRef {
            structsy_impl: self.structsy_impl.clone(),
        }
    }
}

/// Transaction behaviour trait.
pub trait StructsyTx: Sytx {
    /// Persist a new struct instance.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// tx.insert(&Example{value:10})?;
    /// structsy.commit(tx)?;
    /// # Ok(())
    /// # }
    /// ```
    fn insert<T: Persistent>(&mut self, sct: &T) -> SRes<Ref<T>>;

    /// Update a persistent instance with a new value.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// let id = tx.insert(&Example{value:10})?;
    /// tx.update(&id, &Example{value:20})?;
    /// structsy.commit(tx)?;
    /// # Ok(())
    /// # }
    /// ```
    fn update<T: Persistent>(&mut self, sref: &Ref<T>, sct: &T) -> SRes<()>;

    /// Delete a persistent instance.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// let id = tx.insert(&Example{value:10})?;
    /// tx.delete(&id)?;
    /// structsy.commit(tx)?;
    /// # Ok(())
    /// # }
    /// ```
    fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<()>;

    /// Read a persistent instance considering changes in transaction.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// let id = tx.insert(&Example{value:10})?;
    /// let read = tx.read(&id)?;
    /// assert_eq!(10,read.unwrap().value);
    /// structsy.commit(tx)?;
    /// # Ok(())
    /// # }
    /// ```
    fn read<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<Option<T>>;

    /// Scan persistent instances of a struct considering changes in transaction.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// for (id, inst) in tx.scan::<Example>()? {
    ///     // logic
    /// }
    /// structsy.commit(tx)?;
    /// # Ok(())
    /// # }
    /// ```
    fn scan<'a, T: Persistent>(&'a mut self) -> SRes<TxRecordIter<'a, T>>;
}

/// Iterator for record instances
pub struct RecordIter<T: Persistent> {
    iter: persy::SegmentIter,
    marker: PhantomData<T>,
}

impl<T: Persistent> Iterator for RecordIter<T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, buff)) = self.iter.next() {
            if let Ok(x) = T::read(&mut Cursor::new(buff)) {
                Some((Ref::new(id), x))
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Iterator for record instances aware of transactions changes
pub struct TxRecordIter<'a, T: Persistent> {
    iter: persy::TxSegmentIter<'a>,
    marker: PhantomData<T>,
    structsy_impl: Arc<StructsyImpl>,
}

impl<'a, T: Persistent> TxRecordIter<'a, T> {
    fn new(iter: persy::TxSegmentIter<'a>, structsy_impl: Arc<StructsyImpl>) -> TxRecordIter<'a, T> {
        TxRecordIter {
            iter,
            marker: PhantomData,
            structsy_impl,
        }
    }

    pub fn tx(&mut self) -> RefSytx {
        RefSytx {
            trans: self.iter.tx(),
            structsy_impl: self.structsy_impl.clone(),
        }
    }

    pub fn next_tx(&mut self) -> Option<(Ref<T>, T, RefSytx)> {
        if let Some((id, buff, tx)) = self.iter.next_tx() {
            if let Ok(x) = T::read(&mut Cursor::new(buff)) {
                let stx = RefSytx {
                    trans: tx,
                    structsy_impl: self.structsy_impl.clone(),
                };
                Some((Ref::new(id), x, stx))
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<'a, T: Persistent> Iterator for TxRecordIter<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, buff)) = self.iter.next() {
            if let Ok(x) = T::read(&mut Cursor::new(buff)) {
                Some((Ref::new(id), x))
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<TX> StructsyTx for TX
where
    TX: Sytx + Sized,
{
    fn insert<T: Persistent>(&mut self, sct: &T) -> SRes<Ref<T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        let segment = T::get_description().name;
        let id = self.tx().trans.insert_record(&segment, &buff)?;
        let id_ref = Ref {
            type_name: segment,
            raw_id: id,
            ph: PhantomData,
        };
        sct.put_indexes(self, &id_ref)?;
        Ok(id_ref)
    }

    fn update<T: Persistent>(&mut self, sref: &Ref<T>, sct: &T) -> SRes<()> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        self.tx().trans.update_record(&sref.type_name, &sref.raw_id, &buff)?;
        sct.put_indexes(self, &sref)?;
        Ok(())
    }

    fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<()> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        self.tx().trans.delete_record(&sref.type_name, &sref.raw_id)?;
        Ok(())
    }

    fn read<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<Option<T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        tx_read(&sref.type_name, &mut self.tx().trans, &sref.raw_id)
    }

    fn scan<'a, T: Persistent>(&'a mut self) -> SRes<TxRecordIter<'a, T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let name = T::get_description().name;
        let implc = self.structsy().structsy_impl.clone();
        let iter = self.tx().trans.scan(&name)?;
        Ok(TxRecordIter::new(iter, implc))
    }
}

fn tx_read<T: Persistent>(name: &str, tx: &mut Transaction, id: &PersyId) -> SRes<Option<T>> {
    if let Some(buff) = tx.read_record(name, id)? {
        Ok(Some(T::read(&mut Cursor::new(buff))?))
    } else {
        Ok(None)
    }
}

/// Configuration builder for open/create a Structsy file.
///
///
/// # Example
/// ```
/// use structsy::Structsy;
/// # use structsy::SRes;
/// # fn example() -> SRes<()> {
/// let config = Structsy::config("path/to/file.stry");
/// let config = config.create(false);
/// let stry = Structsy::open(config)?;
/// # Ok(())
/// # }
/// ```
pub struct StructsyConfig {
    create: bool,
    path: PathBuf,
}
impl StructsyConfig {
    /// Set flag to create file if it does not exist
    pub fn create(mut self, create: bool) -> StructsyConfig {
        self.create = create;
        self
    }
}
impl<T: AsRef<Path>> From<T> for StructsyConfig {
    fn from(path: T) -> StructsyConfig {
        StructsyConfig {
            create: true,
            path: path.as_ref().to_path_buf(),
        }
    }
}

impl Structsy {
    /// Config builder for open and/or create a structsy file.
    ///
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let config = Structsy::config("path/to/file.stry");
    /// let config = config.create(false);
    /// let stry = Structsy::open(config)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn config<C: AsRef<Path>>(path: C) -> StructsyConfig {
        let mut c = StructsyConfig::from(path);
        c.create = false;
        c
    }

    /// Open a Structsy file, following the configuration as parameter, if the parameter is just a
    /// path it will create the file if it does not exist.
    ///
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open<C: Into<StructsyConfig>>(config: C) -> SRes<Structsy> {
        Ok(Structsy {
            structsy_impl: Arc::new(StructsyImpl::open(config.into())?),
        })
    }

    /// Every struct before use must be 'defined' calling this method.
    ///
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Simple {
    ///     name:String,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// stry.define::<Simple>()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn define<T: Persistent>(&self) -> SRes<bool> {
        self.structsy_impl.define::<T>(&self)
    }

    /// Migrate an existing persistent struct to a new struct.
    ///
    /// In structsy the name and order of the fields matter for the persistence, so each change
    /// need to migrate existing data from existing struct layout to the new struct.
    ///
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct PersonV0 {
    ///     name:String,
    /// }
    ///
    /// #[derive(Persistent)]
    /// struct PersonV1 {
    ///     name:String,
    ///     surname:String,
    /// }
    ///
    /// impl From<PersonV0> for PersonV1 {
    ///     fn from(f: PersonV0)  -> Self {
    ///         PersonV1 {
    ///             name: f.name,
    ///             surname: "Doe".to_string(),
    ///         }
    ///     }
    /// }
    ///
    ///
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// stry.define::<PersonV0>()?;
    /// stry.define::<PersonV1>()?;
    /// stry.migrate::<PersonV0,PersonV1>()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```
    ///
    pub fn migrate<S, D>(&self) -> SRes<()>
    where
        S: Persistent,
        D: Persistent,
        D: From<S>,
    {
        self.structsy_impl.check_defined::<S>()?;
        self.structsy_impl.check_defined::<S>()?;
        if self.structsy_impl.is_referred_by_others::<S>()? {
            return Err(StructsyError::MigrationNotSupported(format!(
                "Struct referred with Ref<{}> by other struct, migration of referred struct is not supported yet",
                S::get_name()
            )));
        }
        // TODO: Handle update of references
        // TODO: Handle partial migration
        let batch = 1000;
        let mut tx = self.begin()?;
        let mut count = 0;
        for (id, record) in self.scan::<S>()? {
            tx.delete(&id)?;
            tx.insert(&D::from(record))?;
            count += 1;
            if count % batch == 0 {
                self.commit(tx)?;
                tx = self.begin()?;
            }
        }
        self.commit(tx)?;
        Ok(())
    }

    /// Begin a new transaction needed to manipulate data.
    ///
    /// Returns an instance of [`OwnedSytx`] to be used with the [`StructsyTx`] trait.
    ///
    /// [`OwnedSytx`]: struct.OwnedSytx.html
    /// [`StructsyTx`]: trait.StructsyTx.html
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// //....
    /// let mut tx = stry.begin()?;
    /// // ... operate on tx.
    /// stry.commit(tx)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn begin(&self) -> SRes<OwnedSytx> {
        Ok(OwnedSytx {
            structsy_impl: self.structsy_impl.clone(),
            trans: self.structsy_impl.begin()?,
        })
    }

    /// Read a persistent instance.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// let id = tx.insert(&Example{value:10})?;
    /// structsy.commit(tx)?;
    /// let read = structsy.read(&id)?;
    /// assert_eq!(10,read.unwrap().value);
    /// # Ok(())
    /// # }
    /// ```
    pub fn read<T: Persistent>(&self, sref: &Ref<T>) -> SRes<Option<T>> {
        self.structsy_impl.read(sref)
    }

    /// Scan records of a specific struct.
    ///
    ///
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Simple {
    ///     name:String,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// stry.define::<Simple>()?;
    /// for (id, inst) in stry.scan::<Simple>()? {
    ///     // logic here
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn scan<T: Persistent>(&self) -> SRes<RecordIter<T>> {
        self.structsy_impl.check_defined::<T>()?;
        let name = T::get_description().name;
        Ok(RecordIter {
            iter: self.structsy_impl.persy.scan(&name)?,
            marker: PhantomData,
        })
    }

    /// Commit a transaction
    ///
    ///
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// //....
    /// let mut tx = stry.begin()?;
    /// // ... operate on tx.
    /// stry.commit(tx)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn commit(&self, tx: OwnedSytx) -> SRes<()> {
        self.structsy_impl.commit(tx.trans)
    }

    /// Check if a struct is defined
    pub fn is_defined<T: Persistent>(&self) -> SRes<bool> {
        self.structsy_impl.is_defined::<T>()
    }
}

impl StructsyImpl {
    fn init_segment<P: AsRef<Path>>(path: P) -> SRes<()> {
        let persy = Persy::open(path, Config::new())?;
        let mut tx = persy.begin()?;
        tx.create_segment(INTERNAL_SEGMENT_NAME)?;
        let prep = tx.prepare_commit()?;
        prep.commit()?;
        Ok(())
    }

    pub fn open(config: StructsyConfig) -> SRes<StructsyImpl> {
        if config.create && !config.path.exists() {
            Persy::create(&config.path)?;
            StructsyImpl::init_segment(&config.path)?;
        }
        let persy = Persy::open(&config.path, Config::new())?;
        let definitions = persy
            .scan(INTERNAL_SEGMENT_NAME)?
            .filter_map(|(_, r)| StructDescription::read(&mut Cursor::new(r)).ok())
            .map(|d| {
                (
                    d.name.clone(),
                    InternalDescription {
                        desc: d,
                        checked: false,
                    },
                )
            })
            .collect();
        Ok(StructsyImpl {
            definitions: Mutex::new(definitions),
            persy: persy,
        })
    }

    pub fn check_defined<T: Persistent>(&self) -> SRes<()> {
        let mut lock = self.definitions.lock()?;
        let name = T::get_name();
        if let Some(x) = lock.get_mut(name) {
            if x.checked {
                Ok(())
            } else {
                let desc = T::get_description();
                if x.desc != desc {
                    Err(StructsyError::StructNotDefined(desc.name.clone()))
                } else {
                    x.checked = true;
                    Ok(())
                }
            }
        } else {
            Err(StructsyError::StructNotDefined(String::from(name)))
        }
    }

    pub fn is_defined<T: Persistent>(&self) -> SRes<bool> {
        let lock = self.definitions.lock()?;
        Ok(lock.contains_key(T::get_name()))
    }

    pub fn define<T: Persistent>(&self, structsy: &Structsy) -> SRes<bool> {
        let desc = T::get_description();
        let mut lock = self.definitions.lock()?;
        match lock.entry(desc.name.clone()) {
            Entry::Occupied(x) => {
                if x.get().desc != desc {
                    return Err(StructsyError::StructAlreadyDefined(desc.name.clone()));
                }
                Ok(false)
            }
            Entry::Vacant(x) => {
                let mut buff = Vec::new();
                desc.write(&mut buff)?;
                let mut tx = structsy.begin()?;
                tx.trans.insert_record(INTERNAL_SEGMENT_NAME, &buff)?;
                tx.trans.create_segment(&desc.name)?;
                T::declare(&mut tx)?;
                structsy.commit(tx)?;
                x.insert(InternalDescription { desc, checked: true });
                Ok(true)
            }
        }
    }

    pub fn begin(&self) -> SRes<Transaction> {
        Ok(self.persy.begin()?)
    }

    pub fn read<T: Persistent>(&self, sref: &Ref<T>) -> SRes<Option<T>> {
        self.check_defined::<T>()?;
        if let Some(buff) = self.persy.read_record(&sref.type_name, &sref.raw_id)? {
            Ok(Some(T::read(&mut Cursor::new(buff))?))
        } else {
            Ok(None)
        }
    }
    pub fn commit(&self, tx: Transaction) -> SRes<()> {
        let to_finalize = tx.prepare_commit()?;
        to_finalize.commit()?;
        Ok(())
    }
    pub fn is_referred_by_others<T: Persistent>(&self) -> SRes<bool> {
        let name = T::get_name();
        for def in self.definitions.lock()?.values() {
            if def.has_refer_to(name) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod test {
    use super::{
        find, find_range, find_range_tx, find_tx, FieldDescription, Persistent, RangeIterator, Ref, SRes,
        StructDescription, Structsy, StructsyTx, Sytx,
    };
    use persy::ValueMode;
    use std::fs;
    use std::io::{Read, Write};
    #[derive(Debug, PartialEq)]
    struct ToTest {
        name: String,
        length: u32,
    }
    impl Persistent for ToTest {
        fn get_name() -> &'static str {
            "ToTest"
        }
        fn get_description() -> StructDescription {
            let fields: [FieldDescription; 2] = [
                FieldDescription::new::<String>(0, "name", Some(ValueMode::CLUSTER)),
                FieldDescription::new::<u32>(1, "length", None),
            ];
            StructDescription::new("ToTest", &fields)
        }
        fn write(&self, write: &mut dyn Write) -> SRes<()> {
            use super::PersistentEmbedded;
            self.name.write(write)?;
            self.length.write(write)?;
            Ok(())
        }
        fn read(read: &mut dyn Read) -> SRes<Self>
        where
            Self: std::marker::Sized,
        {
            use super::PersistentEmbedded;
            Ok(ToTest {
                name: String::read(read)?,
                length: u32::read(read)?,
            })
        }

        fn declare(tx: &mut dyn Sytx) -> SRes<()> {
            use super::declare_index;
            declare_index::<String>(tx, "ToTest.name", ValueMode::EXCLUSIVE)?;
            Ok(())
        }
        fn put_indexes(&self, tx: &mut dyn Sytx, id: &Ref<Self>) -> SRes<()> {
            use super::IndexableValue;
            self.name.puts(tx, "ToTest.name", id)?;
            Ok(())
        }

        fn remove_indexes(&self, tx: &mut dyn Sytx, id: &Ref<Self>) -> SRes<()> {
            use super::IndexableValue;
            self.name.removes(tx, "ToTest.name", id)?;
            Ok(())
        }
    }
    impl ToTest {
        fn find_by_name(st: &Structsy, val: &String) -> SRes<Vec<(Ref<Self>, Self)>> {
            find(st, "ToTest.name", val)
        }
        fn find_by_name_tx(st: &mut dyn Sytx, val: &String) -> SRes<Vec<(Ref<Self>, Self)>> {
            find_tx(st, "ToTest.name", val)
        }
        fn find_by_name_range<R: std::ops::RangeBounds<String>>(
            st: &Structsy,
            range: R,
        ) -> SRes<impl Iterator<Item = (Ref<Self>, Self, String)>> {
            find_range(st, "ToTest.name", range)
        }
        fn find_by_name_range_tx<'a, R: std::ops::RangeBounds<String>>(
            st: &'a mut dyn Sytx,
            range: R,
        ) -> SRes<RangeIterator<'a, String, Self>> {
            find_range_tx(st, "ToTest.name", range)
        }
    }

    #[test]
    fn simple_basic_flow() {
        let db = Structsy::open("one.db").expect("can open the database");
        db.define::<ToTest>().expect("is define correctly");
        let mut tx = db.begin().expect("can start a transaction");
        let val = ToTest {
            name: "one".to_string(),
            length: 3,
        };
        let id = tx.insert(&val).expect("insert correctly");
        let mut read = tx.read(&id).expect("read correctly").expect("this should be some");
        assert_eq!(read.name, val.name);
        assert_eq!(read.length, val.length);
        let looked_up_tx = ToTest::find_by_name_tx(&mut tx, &"one".to_string())
            .map(|x| x.into_iter())
            .into_iter()
            .flatten()
            .map(|(_id, e)| e.name.clone())
            .next();
        assert_eq!(looked_up_tx, Some("one".to_string()));
        let looked_up = ToTest::find_by_name_range_tx(&mut tx, &"mne".to_string()..&"pne".to_string())
            .map(|x| x.into_iter())
            .into_iter()
            .flatten()
            .map(|(_id, e, _k)| e.name.clone())
            .next();
        assert_eq!(looked_up, Some("one".to_string()));
        read.name = "new".to_string();
        tx.update(&id, &read).expect("updated correctly");

        let mut count = 0;
        let mut iter = tx.scan::<ToTest>().expect("scan works");
        assert_eq!(iter.tx().read(&id).expect("transaction access works").is_some(), true);
        for (sid, rec) in iter {
            assert_eq!(rec.name, read.name);
            assert_eq!(rec.length, val.length);
            assert_eq!(sid, id);
            count += 1;
        }

        assert_eq!(count, 1);
        count = 0;
        let mut iter = tx.scan::<ToTest>().expect("scan works");
        while let Some((sid, rec, _tx)) = iter.next_tx() {
            assert_eq!(rec.name, read.name);
            assert_eq!(rec.length, val.length);
            assert_eq!(sid, id);
            count += 1;
        }
        assert_eq!(count, 1);
        db.commit(tx).expect("tx committed correctly");

        let looked_up = ToTest::find_by_name(&db, &"new".to_string())
            .map(|x| x.into_iter())
            .into_iter()
            .flatten()
            .map(|(_id, e)| e.name.clone())
            .next();
        assert_eq!(looked_up, Some("new".to_string()));
        let looked_up = ToTest::find_by_name_range(&db, &"mew".to_string()..&"oew".to_string())
            .map(|x| x.into_iter())
            .into_iter()
            .flatten()
            .map(|(_id, e, _k)| e.name.clone())
            .next();
        assert_eq!(looked_up, Some("new".to_string()));
        let read_persistent = db.read(&id).expect("read correctly").expect("this is some");
        assert_eq!(read_persistent.name, read.name);
        assert_eq!(read_persistent.length, val.length);
        let mut count = 0;
        for (sid, rec) in db.scan::<ToTest>().expect("scan works") {
            assert_eq!(rec.name, read.name);
            assert_eq!(rec.length, val.length);
            assert_eq!(sid, id);
            count += 1;
        }
        assert_eq!(count, 1);
        fs::remove_file("one.db").expect("remove file works");
    }
}

#[cfg(test)]
mod tests {
    use crate::{Persistent, Ref, SRes, StructDescription, Sytx};
    use std::io::{Read, Write};

    #[derive(Debug)]
    struct Pers {}

    impl Persistent for Pers {
        fn get_name() -> &'static str {
            "Pers"
        }
        fn get_description() -> StructDescription {
            StructDescription::new("Pers", &Vec::new())
        }
        fn write(&self, _write: &mut dyn Write) -> SRes<()> {
            Ok(())
        }
        fn read(_read: &mut dyn Read) -> SRes<Self>
        where
            Self: std::marker::Sized,
        {
            Ok(Pers {})
        }
        fn declare(_db: &mut dyn Sytx) -> SRes<()> {
            Ok(())
        }
        fn put_indexes(&self, _tx: &mut dyn Sytx, _id: &Ref<Self>) -> SRes<()>
        where
            Self: std::marker::Sized,
        {
            Ok(())
        }
        fn remove_indexes(&self, _tx: &mut dyn Sytx, _id: &Ref<Self>) -> SRes<()>
        where
            Self: std::marker::Sized,
        {
            Ok(())
        }
    }
    #[test]
    pub fn test_id_display_parse() {
        let id = Ref::<Pers>::new("s0c5a58".parse().unwrap());
        let read: Ref<Pers> = format!("{}", &id).parse().unwrap();
        assert_eq!(id, read);
    }
}
