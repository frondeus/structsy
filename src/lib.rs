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
use std::path::{Path, PathBuf};
use std::sync::Arc;
mod desc;
mod format;
use desc::InternalDescription;
mod index;
pub use index::{RangeIterator, UniqueRangeIterator};
mod filter;
pub use filter::Order;
mod structsy;
use crate::structsy::{RecordIter, StructsyImpl};
mod id;
pub use crate::id::Ref;
mod embedded_filter;
mod error;
pub use crate::error::{SRes, StructsyError};
mod queries;
pub use crate::queries::{EmbeddedFilter, Operators, StructsyIter, StructsyQuery, StructsyQueryTx};
mod transaction;
pub use crate::transaction::{OwnedSytx, RefSytx, StructsyTx, Sytx};
use filter::FilterBuilder;
pub mod internal;
pub use internal::{Persistent, PersistentEmbedded};
mod projection;
pub use queries::Filter;

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

pub trait IntoResult<T> {
    fn into(self, structsy: &Structsy) -> StructsyIter<T>;
    fn into_tx<'a>(self, tx: &'a mut OwnedSytx) -> StructsyIter<'a, T>;
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
        self.structsy_impl.commit(tx.trans)
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
    ///     let count = structsy.query::<Basic>().by_name("aaa".to_string()).into_iter().count();
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

    pub fn into_iter<R: IntoResult<T>, T>(&self, filter: R) -> StructsyIter<T> {
        //StructsyIter::new(filter.extract_filter().finish(&self))
        filter.into(&self)
    }

    /// Create a new filter for an embedded structure
    ///
    ///
    /// # Example
    /// ```
    /// use structsy::{ Structsy, StructsyTx, StructsyError, EmbeddedFilter};
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
    ///     fn embedded(self, embedded: EmbeddedFilter<Embedded>) -> Self;
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
    ///     let embedded_filter = Structsy::embedded_filter::<Embedded>().by_name("aaa".to_string());
    ///     let count = structsy.query::<WithEmbedded>().embedded(embedded_filter).into_iter().count();
    ///     assert_eq!(count, 1);
    ///     Ok(())
    /// }
    /// ```
    pub fn embedded_filter<T: PersistentEmbedded + 'static>() -> EmbeddedFilter<T> {
        EmbeddedFilter {
            builder: embedded_filter::EmbeddedFilterBuilder::new(),
        }
    }
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
                FieldDescription::new::<String>(0, "name", Some(ValueMode::CLUSTER)),
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
            declare_index::<String>(tx, "ToTest.name", ValueMode::EXCLUSIVE)?;
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
