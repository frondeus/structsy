use crate::error::{SRes, StructsyError};
use crate::filter_builder::FilterBuilder;
use crate::id::raw_parse;
use crate::queries::SnapshotQuery;
use crate::record::Record;
use crate::structsy::{SnapshotRecordIter, StructsyImpl};
use crate::{Fetch, Persistent, RawAccess, RawIter, RawRead, Ref, Structsy, StructsyIter};
use persy::PersyId;
use std::io::Cursor;
use std::sync::Arc;

/// Read data from a snapshot freezed in a specific moment ignoring all
/// the subsequent committed transactions.
///
#[derive(Clone)]
pub struct Snapshot {
    pub(crate) structsy_impl: Arc<StructsyImpl>,
    pub(crate) ps: persy::Snapshot,
}

impl Snapshot {
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
    /// let snapshot = structsy.snapshot()?;
    /// let read = snapshot.read(&id)?;
    /// assert_eq!(10,read.unwrap().value);
    /// # Ok(())
    /// # }
    /// ```
    pub fn read<T: Persistent>(&self, sref: &Ref<T>) -> SRes<Option<T>> {
        self.structsy_impl.read_snapshot(self, sref)
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
    /// let snapshot = stry.snapshot()?;
    /// for (id, inst) in snapshot.scan::<Simple>()? {
    ///     // logic here
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn scan<T: Persistent>(&self) -> SRes<SnapshotRecordIter<T>> {
        self.structsy_impl.scan_snapshot::<T>(self)
    }
    pub(crate) fn structsy(&self) -> Structsy {
        Structsy {
            structsy_impl: self.structsy_impl.clone(),
        }
    }

    /// Execute a filter query and return an iterator of results for the current snapshot
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
    ///     let snapshot = structsy.snapshot()?;
    ///     let embedded_filter = Filter::<Embedded>::new().by_name("aaa".to_string());
    ///     let filter = Filter::<WithEmbedded>::new().embedded(embedded_filter);
    ///     let count = snapshot.fetch(filter).count();
    ///     assert_eq!(count, 1);
    ///     Ok(())
    /// }
    /// ```
    pub fn fetch<R: Fetch<T>, T>(&self, filter: R) -> StructsyIter<T> {
        filter.fetch_snapshot(self)
    }

    ///
    /// Query for a persistent struct in the snapshot
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
    ///     let snapshot = structsy.snapshot()?;
    ///     let count = snapshot.query::<Basic>().by_name("aaa".to_string()).fetch().count();
    ///     assert_eq!(count, 1);
    ///     Ok(())
    /// }
    /// ```
    pub fn query<T: Persistent + 'static>(&self) -> SnapshotQuery<T> {
        SnapshotQuery {
            snapshot: self.clone(),
            builder: FilterBuilder::new(),
        }
    }

    pub fn list_defined(&self) -> SRes<impl std::iter::Iterator<Item = crate::desc::Description>> {
        self.structsy_impl.list_defined()
    }
}

impl RawRead for Snapshot {
    fn raw_scan(&self, strct_name: &str) -> SRes<RawIter> {
        let definition = self.structsy_impl.full_definition_by_name(strct_name)?;
        Ok(RawIter::new(
            self.structsy_impl.persy.scan(&definition.info().segment_name())?,
            definition,
        ))
    }
    fn raw_read(&self, id: &str) -> SRes<Option<Record>> {
        let (ty, pid) = raw_parse(id)?;
        let definition = self.structsy_impl.full_definition_by_name(ty)?;
        let rid: PersyId = pid.parse().or(Err(StructsyError::InvalidId))?;
        let raw = self.structsy_impl.persy.read(&definition.info().segment_name(), &rid)?;
        if let Some(data) = raw {
            Ok(Some(Record::read(&mut Cursor::new(data), &definition.desc)?))
        } else {
            Ok(None)
        }
    }
}
impl RawAccess for Snapshot {
    fn raw_begin(&self) -> SRes<crate::structsy::RawTransaction> {
        unimplemented!()
    }

    fn raw_define(&self, _desc: crate::Description) -> SRes<bool> {
        unimplemented!()
    }
}
