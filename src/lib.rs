pub use persy::ValueMode;
use persy::{Config, IndexType, Persy, PersyError, PersyId, Transaction};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Cursor, Error as IOError, Read, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
mod format;
pub use format::PersistentEmbedded;
mod desc;
pub use desc::{FieldDescription,StructDescription};
use desc::InternalDescription;
mod index;
pub use index::{
    find, find_range, find_range_tx, find_tx, find_unique, find_unique_range, find_unique_range_tx, find_unique_tx,
    IndexableValue, RangeIterator, UniqueRangeIterator,
};

const INTERNAL_SEGMENT_NAME: &str = "__#internal";

#[derive(Debug)]
pub enum StructsyError {
    PersyError(PersyError),
    StructAlreadyDefined(String),
    StructNotDefined(String),
    IOError,
    PoisonedLock,
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
#[derive(Clone)]
pub struct Structsy {
    tsdb_impl: Arc<StructsyImpl>,
}

pub trait EmbeddedDescription: PersistentEmbedded {
    fn get_description() -> StructDescription;
}
pub trait Persistent {
    fn get_name() -> &'static str;
    fn get_description() -> StructDescription;
    fn write(&self, write: &mut Write) -> SRes<()>;
    fn read(read: &mut Read) -> SRes<Self>
    where
        Self: std::marker::Sized;
    fn declare(db: &mut Sytx) -> SRes<()>;
    fn put_indexes(&self, tx: &mut Sytx, id: &Ref<Self>) -> SRes<()>
    where
        Self: std::marker::Sized;
    fn remove_indexes(&self, tx: &mut Sytx, id: &Ref<Self>) -> SRes<()>
    where
        Self: std::marker::Sized;
}

pub fn declare_index<T: IndexType>(db: &mut Sytx, name: &str, mode: ValueMode) -> SRes<()> {
    let persy = &db.structsy().structsy_impl.persy;
    persy.create_index::<T, PersyId>(&mut db.tx().trans, name, mode)?;
    Ok(())
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
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

pub struct OwnedSytx {
    tsdb_impl: Arc<StructsyImpl>,
    trans: Transaction,
}

pub struct RefSytx<'a> {
    tsdb_impl: Arc<StructsyImpl>,
    trans: &'a mut Transaction,
}

pub struct TxRef<'a> {
    trans: &'a mut Transaction,
}

pub struct ImplRef {
    structsy_impl: Arc<StructsyImpl>,
}

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
            structsy_impl: self.tsdb_impl.clone(),
        }
    }
}

impl<'a> Sytx for RefSytx<'a> {
    fn tx(&mut self) -> TxRef {
        TxRef { trans: self.trans }
    }
    fn structsy(&self) -> ImplRef {
        ImplRef {
            structsy_impl: self.tsdb_impl.clone(),
        }
    }
}

pub trait StructsyTx: Sytx {
    fn insert<T: Persistent>(&mut self, sct: &T) -> SRes<Ref<T>>;
    fn update<T: Persistent>(&mut self, sref: &Ref<T>, sct: &T) -> SRes<()>;
    fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<()>;
    fn read<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<Option<T>>;
    fn scan<'a, T: Persistent>(&'a mut self) -> SRes<TxRecordIter<'a, T>>;
}

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
            tsdb_impl: self.structsy_impl.clone(),
        }
    }

    pub fn next_tx(&mut self) -> Option<(Ref<T>, T, RefSytx)> {
        if let Some((id, buff, tx)) = self.iter.next_tx() {
            if let Ok(x) = T::read(&mut Cursor::new(buff)) {
                let stx = RefSytx {
                    trans: tx,
                    tsdb_impl: self.structsy_impl.clone(),
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
        let persy = &self.structsy().structsy_impl.persy;
        let id = persy.insert_record(self.tx().trans, &segment, &buff)?;
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
        let persy = &self.structsy().structsy_impl.persy;
        persy.update_record(&mut self.tx().trans, &sref.type_name, &sref.raw_id, &buff)?;
        sct.put_indexes(self, &sref)?;
        Ok(())
    }

    fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<()> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        let persy = &self.structsy().structsy_impl.persy;
        persy.delete_record(&mut self.tx().trans, &sref.type_name, &sref.raw_id)?;
        Ok(())
    }

    fn read<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<Option<T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let persy = &self.structsy().structsy_impl.persy;
        tx_read(&persy, &sref.type_name, &mut self.tx().trans, &sref.raw_id)
    }

    fn scan<'a, T: Persistent>(&'a mut self) -> SRes<TxRecordIter<'a, T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let name = T::get_description().name;
        let persy = &self.structsy().structsy_impl.persy;
        let implc = self.structsy().structsy_impl.clone();
        let iter = persy.scan_tx(self.tx().trans, &name)?;
        Ok(TxRecordIter::new(iter, implc))
    }
}

fn tx_read<T: Persistent>(persy: &Persy, name: &str, tx: &mut Transaction, id: &PersyId) -> SRes<Option<T>> {
    if let Some(buff) = persy.read_record_tx(tx, name, id)? {
        Ok(Some(T::read(&mut Cursor::new(buff))?))
    } else {
        Ok(None)
    }
}

impl Structsy {
    pub fn create_if_not_exists<P: AsRef<Path>>(path: P) -> SRes<bool> {
        StructsyImpl::create_if_not_exists(path)
    }

    pub fn create<P: AsRef<Path>>(path: P) -> SRes<()> {
        StructsyImpl::create(path)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> SRes<Structsy> {
        Ok(Structsy {
            tsdb_impl: Arc::new(StructsyImpl::open(path)?),
        })
    }

    pub fn define<T: Persistent>(&self) -> SRes<()> {
        self.tsdb_impl.define::<T>(&self)
    }

    pub fn migrate<S, D>(&self) -> SRes<()>
    where
        S: Persistent,
        D: Persistent,
        D: From<S>,
    {
        self.tsdb_impl.check_defined::<S>()?;
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

    pub fn begin(&self) -> SRes<OwnedSytx> {
        Ok(OwnedSytx {
            tsdb_impl: self.tsdb_impl.clone(),
            trans: self.tsdb_impl.begin()?,
        })
    }

    fn read<T: Persistent>(&self, sref: &Ref<T>) -> SRes<Option<T>> {
        self.tsdb_impl.read(sref)
    }

    pub fn scan<T: Persistent>(&self) -> SRes<RecordIter<T>> {
        self.tsdb_impl.check_defined::<T>()?;
        let name = T::get_description().name;
        Ok(RecordIter {
            iter: self.tsdb_impl.persy.scan(&name)?,
            marker: PhantomData,
        })
    }

    pub fn commit(&self, tx: OwnedSytx) -> SRes<()> {
        self.tsdb_impl.commit(tx.trans)
    }
    pub fn is_defined<T:Persistent> (&self) -> SRes<bool> {
        self.tsdb_impl.is_defined::<T>()
    }
}

impl StructsyImpl {
    pub fn create_if_not_exists<P: AsRef<Path>>(path: P) -> SRes<bool> {
        if !path.as_ref().exists() {
            Persy::create(path.as_ref())?;
            StructsyImpl::init_segment(path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    fn init_segment<P: AsRef<Path>>(path: P) -> SRes<()> {
        let persy = Persy::open(path, Config::new())?;
        let mut tx = persy.begin()?;
        persy.create_segment(&mut tx, INTERNAL_SEGMENT_NAME)?;
        let prep = persy.prepare_commit(tx)?;
        persy.commit(prep)?;
        Ok(())
    }
    pub fn create<P: AsRef<Path>>(path: P) -> SRes<()> {
        Persy::create(path.as_ref())?;
        StructsyImpl::init_segment(path)?;
        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> SRes<StructsyImpl> {
        let persy = Persy::open(path, Config::new())?;
        let definitions = persy
            .scan(INTERNAL_SEGMENT_NAME)?
            .filter_map(|(_, r)| StructDescription::read(&mut Cursor::new(r)).ok())
            .map(|d| (d.name.clone(), InternalDescription{desc:d,checked:false}))
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

    pub fn define<T: Persistent>(&self, tsdb: &Structsy) -> SRes<()> {
        let desc = T::get_description();
        let mut lock = self.definitions.lock()?;
        match lock.entry(desc.name.clone()) {
            Entry::Occupied(x) => {
                if x.get().desc != desc {
                    return Err(StructsyError::StructAlreadyDefined(desc.name.clone()));
                }
            }
            Entry::Vacant(x) => {
                let mut buff = Vec::new();
                desc.write(&mut buff)?;
                let mut tx = tsdb.begin()?;
                self.persy.insert_record(&mut tx.trans, INTERNAL_SEGMENT_NAME, &buff)?;
                self.persy.create_segment(&mut tx.trans, &desc.name)?;
                T::declare(&mut tx)?;
                tsdb.commit(tx)?;
                x.insert(InternalDescription{
                    desc,
                    checked: true,
                });
            }
        }
        Ok(())
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
        let to_finalize = self.persy.prepare_commit(tx)?;
        self.persy.commit(to_finalize)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{
        find, find_range, find_range_tx, find_tx, FieldDescription, Persistent,
        RangeIterator, Ref, SRes, StructDescription, Structsy, StructsyTx, Sytx,
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
            let mut fields = Vec::new();
            fields.push(FieldDescription::new::<String>(0,"name",Some(ValueMode::CLUSTER)));
            fields.push(FieldDescription::new::<u32>(1, "length", None));
            StructDescription {
                name: "ToTest".to_string(),
                hash_id: 10,
                fields,
            }
        }
        fn write(&self, write: &mut Write) -> SRes<()> {
            use super::PersistentEmbedded;
            self.name.write(write)?;
            self.length.write(write)?;
            Ok(())
        }
        fn read(read: &mut Read) -> SRes<Self>
        where
            Self: std::marker::Sized,
        {
            use super::PersistentEmbedded;
            Ok(ToTest {
                name: String::read(read)?,
                length: u32::read(read)?,
            })
        }

        fn declare(tx: &mut Sytx) -> SRes<()> {
            use super::declare_index;
            declare_index::<String>(tx, "ToTest.name", ValueMode::EXCLUSIVE)?;
            Ok(())
        }
        fn put_indexes(&self, tx: &mut Sytx, id: &Ref<Self>) -> SRes<()> {
            use super::IndexableValue;
            self.name.puts(tx, "ToTest.name", id)?;
            Ok(())
        }

        fn remove_indexes(&self, tx: &mut Sytx, id: &Ref<Self>) -> SRes<()> {
            use super::IndexableValue;
            self.name.removes(tx, "ToTest.name", id)?;
            Ok(())
        }
    }
    impl ToTest {
        fn find_by_name(st: &Structsy, val: &String) -> SRes<Vec<(Ref<Self>, Self)>> {
            find(st, "ToTest.name", val)
        }
        fn find_by_name_tx(st: &mut Sytx, val: &String) -> SRes<Vec<(Ref<Self>, Self)>> {
            find_tx(st, "ToTest.name", val)
        }
        fn find_by_name_range<R: std::ops::RangeBounds<String>>(
            st: &Structsy,
            range: R,
        ) -> SRes<impl Iterator<Item = (Ref<Self>, Self, String)>> {
            find_range(st, "ToTest.name", range)
        }
        fn find_by_name_range_tx<'a, R: std::ops::RangeBounds<String>>(
            st: &'a mut Sytx,
            range: R,
        ) -> SRes<RangeIterator<'a, String, Self>> {
            find_range_tx(st, "ToTest.name", range)
        }
    }

    #[test()]
    fn simple_basic_flow() {
        Structsy::create("one.db").expect("can create the database");
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
