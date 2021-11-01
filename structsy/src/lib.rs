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
use record::Record;
use std::path::{Path, PathBuf};
use std::sync::Arc;
mod desc;
mod format;
use desc::{Description, InternalDescription};
mod index;
pub use index::{RangeIterator, UniqueRangeIterator};
mod filter_builder;
mod structsy;
pub use crate::structsy::{RawIter, RawPrepare, RawTransaction};
use crate::structsy::{RecordIter, StructsyImpl};
mod id;
pub use crate::id::Ref;
mod error;
pub use crate::error::{SRes, StructsyError};
mod queries;
pub use crate::queries::{Operators, SnapshotQuery, StructsyIter, StructsyQuery, StructsyQueryTx};
mod transaction;
pub use crate::transaction::{OwnedSytx, Prepared, RefSytx, StructsyTx, Sytx};
use filter_builder::FilterBuilder;
pub mod internal;
pub use internal::{Persistent, PersistentEmbedded};
mod filter;
mod projection;
pub use filter::Filter;
mod actions;
pub mod record;

#[cfg(feature = "derive")]
pub mod derive {
    //! Re Export proc macros
    //!
    pub use structsy_derive::{embedded_queries, queries, Persistent, PersistentEmbedded, Projection};
}
mod snapshot;
pub use snapshot::Snapshot;

/// Main API to persist structs with structsy.
///
///
#[derive(Clone)]
pub struct Structsy {
    structsy_impl: Arc<StructsyImpl>,
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
/// Prepare open of a structsy file, with migrations possibilities
pub struct PrepareOpen {
    structsy_impl: Arc<StructsyImpl>,
}
impl PrepareOpen {
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
    /// let prepare = Structsy::prepare_open("path/to/file.stry")?;
    /// prepare.migrate::<PersonV0,PersonV1>()?;
    /// let stry = prepare.open()?;
    /// stry.define::<PersonV1>()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn migrate<S, D>(&self) -> SRes<()>
    where
        S: Persistent,
        D: Persistent,
        D: From<S>,
    {
        self.structsy_impl.migrate::<S, D>()
    }
    /// Open a structsy instance from a prepare context.
    ///
    ///
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let prepare = Structsy::prepare_open("path/to/file.stry")?;
    /// let stry = prepare.open()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(self) -> SRes<Structsy> {
        Ok(Structsy {
            structsy_impl: self.structsy_impl,
        })
    }
}
/// Execute a query on structsy or a structsy transaction
///
pub trait Fetch<T> {
    #[deprecated]
    fn into(self, structsy: &Structsy) -> StructsyIter<T>;
    fn fetch(self, structsy: &Structsy) -> StructsyIter<T>;
    #[deprecated]
    fn into_tx(self, tx: &mut OwnedSytx) -> StructsyIter<T>;
    fn fetch_tx(self, tx: &mut OwnedSytx) -> StructsyIter<T>;
    fn fetch_snapshot(self, structsy: &Snapshot) -> StructsyIter<T>;
}

#[deprecated]
pub trait IntoResult<T>: Fetch<T> {}
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
    /// Prepare the open of a stuctsy file with the possibility to do migrations
    /// of data of previous structs.
    ///
    pub fn prepare_open<C: Into<StructsyConfig>>(config: C) -> SRes<PrepareOpen> {
        Ok(PrepareOpen {
            structsy_impl: Arc::new(StructsyImpl::open(config.into())?),
        })
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

    /// Open a structsy instance with only in memory persistence.
    /// This instance will delete all the data when went out of scope.
    ///
    /// # Example
    /// ```
    /// use structsy::Structsy;
    /// # use structsy::SRes;
    /// # fn main() -> SRes<()> {
    /// let stry = Structsy::memory()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn memory() -> SRes<Structsy> {
        Ok(Structsy {
            structsy_impl: Arc::new(StructsyImpl::memory()?),
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
        self.structsy_impl.define::<T>()
    }

    /// Remove a defined struct deleting all the contained data.
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
    /// stry.undefine::<Simple>()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn undefine<T: Persistent>(&self) -> SRes<()> {
        self.structsy_impl.undefine::<T>()
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
        self.structsy_impl.begin()
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
    /// use structsy::{Structsy, StructsyTx};
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
    #[deprecated]
    pub fn commit(&self, tx: OwnedSytx) -> SRes<()> {
        self.structsy_impl.commit(tx)
    }

    /// Check if a struct is defined
    pub fn is_defined<T: Persistent>(&self) -> SRes<bool> {
        self.structsy_impl.is_defined::<T>()
    }
    ///
    /// Query for a persistent struct
    ///
    /// # Example
    /// ```
    /// use structsy::{ Structsy, StructsyTx, StructsyError};
    /// use structsy_derive::{queries, Persistent};
    /// #[derive(Persistent)]
    /// struct Basic {
    ///     name: String,
    /// }
    /// impl Basic {
    ///     fn new(name: &str) -> Basic {
    ///         Basic { name: name.to_string() }
    ///     }
    /// }
    ///
    /// #[queries(Basic)]
    /// trait BasicQuery {
    ///      fn by_name(self, name: String) -> Self;
    /// }
    ///
    /// fn basic_query() -> Result<(), StructsyError> {
    ///     let structsy = Structsy::open("file.structsy")?;
    ///     structsy.define::<Basic>()?;
    ///     let mut tx = structsy.begin()?;
    ///     tx.insert(&Basic::new("aaa"))?;
    ///     tx.commit()?;
    ///     let count = structsy.query::<Basic>().by_name("aaa".to_string()).fetch().count();
    ///     assert_eq!(count, 1);
    ///     Ok(())
    /// }
    /// ```
    pub fn query<T: Persistent>(&self) -> StructsyQuery<T> {
        StructsyQuery {
            structsy: self.clone(),
            builder: FilterBuilder::new(),
        }
    }

    /// Execute a filter query and return an iterator of results
    ///
    ///
    /// # Example
    /// ```
    /// use structsy::{ Structsy, StructsyTx, StructsyError, Filter};
    /// use structsy_derive::{queries, embedded_queries, Persistent, PersistentEmbedded};
    ///
    /// #[derive(Persistent)]
    /// struct WithEmbedded {
    ///     embedded: Embedded,
    /// }
    ///
    /// #[derive(PersistentEmbedded)]
    /// struct Embedded {
    ///     name: String,
    /// }
    /// impl WithEmbedded {
    ///     fn new(name: &str) -> WithEmbedded {
    ///         WithEmbedded {
    ///             embedded: Embedded { name: name.to_string() },
    ///         }
    ///     }
    /// }
    ///
    /// #[queries(WithEmbedded)]
    /// trait WithEmbeddedQuery {
    ///     fn embedded(self, embedded: Filter<Embedded>) -> Self;
    /// }
    ///
    /// #[embedded_queries(Embedded)]
    /// trait EmbeddedQuery {
    ///     fn by_name(self, name: String) -> Self;
    /// }
    ///
    /// fn embedded_query() -> Result<(), StructsyError> {
    ///     let structsy = Structsy::open("file.structsy")?;
    ///     structsy.define::<WithEmbedded>()?;
    ///     let mut tx = structsy.begin()?;
    ///     tx.insert(&WithEmbedded::new("aaa"))?;
    ///     tx.commit()?;
    ///     let embedded_filter = Filter::<Embedded>::new().by_name("aaa".to_string());
    ///     let filter = Filter::<WithEmbedded>::new().embedded(embedded_filter);
    ///     let count = structsy.fetch(filter).count();
    ///     assert_eq!(count, 1);
    ///     Ok(())
    /// }
    /// ```
    pub fn fetch<R: Fetch<T>, T>(&self, filter: R) -> StructsyIter<T> {
        filter.fetch(self)
    }

    #[deprecated]
    pub fn into_iter<R: Fetch<T>, T>(&self, filter: R) -> StructsyIter<T> {
        filter.fetch(self)
    }

    pub fn list_defined(&self) -> SRes<impl std::iter::Iterator<Item = desc::Description>> {
        self.structsy_impl.list_defined()
    }

    /// Create a new snapshot at this specific moment.
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
    /// let snapshot = structsy.snapshot()?;
    /// let read = snapshot.read(&id)?;
    /// assert_eq!(10,read.unwrap().value);
    /// # Ok(())
    /// # }
    /// ```
    pub fn snapshot(&self) -> SRes<Snapshot> {
        Ok(Snapshot {
            structsy_impl: self.structsy_impl.clone(),
            ps: self.structsy_impl.persy.snapshot()?,
        })
    }
}

/// Query ordering
#[derive(Debug, Eq, PartialEq)]
pub enum Order {
    Asc,
    Desc,
}

pub trait RawRead {
    /// Scan the records of a struct or enum in a raw format
    fn raw_scan(&self, ty_name: &str) -> SRes<RawIter>;
    /// read a single record in a raw formant from a string id
    fn raw_read(&self, id: &str) -> SRes<Option<Record>>;
}

/// Trait for data operations that do not require original structs and enums source code.
pub trait RawAccess: RawRead {
    /// Scan the records of a struct or enum in a raw format
    fn raw_scan(&self, ty_name: &str) -> SRes<RawIter> {
        RawRead::raw_scan(self, ty_name)
    }
    /// read a single record in a raw formant from a string id
    fn raw_read(&self, id: &str) -> SRes<Option<Record>> {
        RawRead::raw_read(self, id)
    }
    /// Declare a new struct or enum from the generic description
    fn raw_define(&self, desc: Description) -> SRes<bool>;
    /// Begin a new raw transaction
    fn raw_begin(&self) -> SRes<RawTransaction>;
}

#[cfg(test)]
mod test {
    use super::{
        internal::{find, find_range, find_range_tx, find_tx, Description, FieldDescription, Query, StructDescription},
        Persistent, RangeIterator, Ref, SRes, Structsy, StructsyTx, Sytx,
    };
    use persy::ValueMode;
    use std::fs;
    use std::io::{Read, Write};
    #[derive(Debug, PartialEq)]
    struct ToTest {
        name: String,
        length: u32,
    }
    impl crate::internal::FilterDefinition for ToTest {
        type Filter = crate::internal::FilterBuilder<Self>;
    }
    impl Persistent for ToTest {
        fn get_name() -> &'static str {
            "ToTest"
        }
        fn get_description() -> Description {
            let fields: [FieldDescription; 2] = [
                FieldDescription::new::<String>(0, "name", Some(ValueMode::Cluster)),
                FieldDescription::new::<u32>(1, "length", None),
            ];
            Description::Struct(StructDescription::new("ToTest", &fields))
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
            use super::internal::PersistentEmbedded;
            Ok(ToTest {
                name: String::read(read)?,
                length: u32::read(read)?,
            })
        }

        fn declare(tx: &mut dyn Sytx) -> SRes<()> {
            use super::internal::declare_index;
            declare_index::<String>(tx, "ToTest.name", ValueMode::Exclusive)?;
            Ok(())
        }
        fn put_indexes(&self, tx: &mut dyn Sytx, id: &Ref<Self>) -> SRes<()> {
            use super::internal::IndexableValue;
            self.name.puts(tx, "ToTest.name", id)?;
            Ok(())
        }

        fn remove_indexes(&self, tx: &mut dyn Sytx, id: &Ref<Self>) -> SRes<()> {
            use super::internal::IndexableValue;
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
        fn all(self) -> Self;
    }

    impl<Q: Query<ToTest>> ToTestQueries for Q {
        fn all(mut self) -> Self {
            let _builder = self.filter_builder();
            self
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

    impl crate::internal::FilterDefinition for Pers {
        type Filter = crate::internal::FilterBuilder<Self>;
    }
    impl Persistent for Pers {
        fn get_name() -> &'static str {
            "Pers"
        }
        fn get_description() -> Description {
            Description::Struct(StructDescription::new("Pers", &Vec::new()))
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
