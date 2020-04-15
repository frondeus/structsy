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
//! tx.commit()?;
//! # Ok(())
//! # }
//!```
//!
//!
pub use persy::ValueMode;
use persy::{IndexType, PersyError, PersyId, Transaction};
use std::io::{Cursor, Error as IOError, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};
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
mod structsy;
use crate::structsy::StructsyImpl;
mod id;
pub use crate::id::Ref;
mod embedded_filter;

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

pub struct StructsyInto<T> {
    t: T,
}

impl<T> StructsyInto<T> {
    pub fn into(self) -> T {
        self.t
    }
}

pub type IterResult<T> = Result<StructsyQuery<T>, StructsyError>;
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

pub trait Sytx {
    /// Internal Use Only
    ///
    #[doc(hidden)]
    fn tx(&mut self) -> TxRef;
    /// Internal Use Only
    ///
    #[doc(hidden)]
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
impl StructsyTx for OwnedSytx {
    fn commit(self) -> SRes<()> {
        let prepared = self.trans.prepare_commit()?;
        prepared.commit()?;
        Ok(())
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
impl<'a> StructsyTx for RefSytx<'a> {
    fn commit(self) -> SRes<()> {
        panic!("")
    }
}

pub struct EmbeddedFilter<T: PersistentEmbedded + 'static> {
    builder: embedded_filter::EmbeddedFilterBuilder<T>,
}

pub struct StructsyQuery<T: Persistent + 'static> {
    structsy: Structsy,
    builder: FilterBuilder<T>,
}

pub fn filter_builder<T: Persistent>(query: &mut StructsyQuery<T>) -> &mut FilterBuilder<T> {
    &mut query.builder
}

impl<T: Persistent> IntoIterator for StructsyQuery<T> {
    type Item = (Ref<T>, T);
    type IntoIter = StructsyIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        StructsyIter::new(self.builder.finish(&self.structsy))
    }
}

/// Transaction behaviour trait.
pub trait StructsyTx: Sytx + Sized {
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
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn insert<T: Persistent>(&mut self, sct: &T) -> SRes<Ref<T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        let segment = T::get_description().name;
        let id = self.tx().trans.insert_record(&segment, &buff)?;
        let id_ref = Ref::new(id);
        sct.put_indexes(self, &id_ref)?;
        Ok(id_ref)
    }

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
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<()> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        self.tx().trans.delete_record(&sref.type_name, &sref.raw_id)?;
        Ok(())
    }

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
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn read<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<Option<T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        structsy::tx_read(&sref.type_name, &mut self.tx().trans, &sref.raw_id)
    }

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
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn scan<'a, T: Persistent>(&'a mut self) -> SRes<TxRecordIter<'a, T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let name = T::get_description().name;
        let implc = self.structsy().structsy_impl.clone();
        let iter = self.tx().trans.scan(&name)?;
        Ok(TxRecordIter::new(iter, implc))
    }

    /// Commit a transaction
    ///
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// //....
    /// let mut tx = stry.begin()?;
    /// // ... operate on tx.
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn commit(self) -> SRes<()>;
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
                tx.commit()?;
                tx = self.begin()?;
            }
        }
        tx.commit()?;
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
    /// use structsy::{Structsy,StructsyTx};
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// //....
    /// let mut tx = stry.begin()?;
    /// // ... operate on tx.
    /// tx.commit()?;
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
    /// tx.commit()?;
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
        self.structsy_impl.scan::<T>()
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
    #[deprecated]
    pub fn commit(&self, tx: OwnedSytx) -> SRes<()> {
        self.structsy_impl.commit(tx.trans)
    }

    /// Check if a struct is defined
    pub fn is_defined<T: Persistent>(&self) -> SRes<bool> {
        self.structsy_impl.is_defined::<T>()
    }

    pub fn query<T: Persistent>(&self) -> StructsyQuery<T> {
        StructsyQuery {
            structsy: self.clone(),
            builder: FilterBuilder::new(),
        }
    }
    pub fn embedded_filter<T: PersistentEmbedded>() -> EmbeddedFilter<T> {
        EmbeddedFilter {
            builder: embedded_filter::EmbeddedFilterBuilder::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{
        find, find_range, find_range_tx, find_tx, FieldDescription, IterResult, Persistent, RangeIterator, Ref, SRes,
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
    trait ToTestQueries {
        fn all(self) -> IterResult<ToTest>;
    }

    impl ToTestQueries for super::StructsyQuery<ToTest> {
        fn all(mut self) -> IterResult<ToTest> {
            let _builder = super::filter_builder(&mut self);
            Ok(self)
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
        tx.commit().expect("tx committed correctly");

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
