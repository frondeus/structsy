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
pub use desc::{FieldDescription, FieldType, FieldValueType, StructDescription};

const INTERNAL_SEGMENT_NAME: &str = "__#internal";

#[derive(Debug)]
pub enum TsdbError {
    PersyError(PersyError),
    StructAlreadyDefined(String),
    StructNotDefined(String),
    IOError,
    PoisonedLock,
}

impl From<PersyError> for TsdbError {
    fn from(err: PersyError) -> TsdbError {
        TsdbError::PersyError(err)
    }
}
impl<T> From<PoisonError<T>> for TsdbError {
    fn from(_err: PoisonError<T>) -> TsdbError {
        TsdbError::PoisonedLock
    }
}

impl From<IOError> for TsdbError {
    fn from(_err: IOError) -> TsdbError {
        TsdbError::IOError
    }
}

pub type TRes<T> = Result<T, TsdbError>;

pub struct TsdbImpl {
    persy: Persy,
    definitions: Mutex<HashMap<String, StructDescription>>,
}

pub struct Tsdb {
    tsdb_impl: Arc<TsdbImpl>,
}

pub trait Persistent {
    fn get_description() -> StructDescription;
    fn write(&self, write: &mut Write) -> TRes<()>;
    fn read(read: &mut Read) -> TRes<Self>
    where
        Self: std::marker::Sized;
    fn declare(db: &mut Tstx) -> TRes<()>;
    fn put_indexes(&self, tx: &mut Tstx, id: &Ref<Self>) -> TRes<()>
    where
        Self: std::marker::Sized;
    fn remove_indexes(&self, tx: &mut Tstx, id: &Ref<Self>) -> TRes<()>
    where
        Self: std::marker::Sized;
}

pub fn declare_index<T: IndexType>(db: &mut Tstx, name: &str, mode: ValueMode) -> TRes<()> {
    db.tsdb_impl
        .persy
        .create_index::<T, PersyId>(&mut db.trans, name, mode)?;
    Ok(())
}

pub trait IndexableValue {
    fn puts<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()>;
    fn removes<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()>;
}

macro_rules! impl_indexable_value {
    ($t:ident) => {
        impl IndexableValue for $t {
            fn puts<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()> {
                put_index(tx, name, self, id)
            }
            fn removes<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()> {
                remove_index(tx, name, self, id)
            }
        }
    };
}
impl_indexable_value!(u8);
impl_indexable_value!(u16);
impl_indexable_value!(u32);
impl_indexable_value!(u64);
impl_indexable_value!(u128);
impl_indexable_value!(i8);
impl_indexable_value!(i16);
impl_indexable_value!(i32);
impl_indexable_value!(i64);
impl_indexable_value!(i128);
impl_indexable_value!(String);

impl<T: IndexableValue> IndexableValue for Option<T> {
    fn puts<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()> {
        if let Some(x) = self {
            x.puts(tx, name, id)?;
        }
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()> {
        if let Some(x) = self {
            x.removes(tx, name, id)?;
        }
        Ok(())
    }
}
impl<T: IndexableValue> IndexableValue for Vec<T> {
    fn puts<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()> {
        for x in self {
            x.puts(tx, name, id)?;
        }
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()> {
        for x in self {
            x.removes(tx, name, id)?;
        }
        Ok(())
    }
}
impl<T: Persistent> IndexableValue for Ref<T> {
    fn puts<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()> {
        put_index(tx, name, &self.raw_id, id)?;
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut Tstx, name: &str, id: &Ref<P>) -> TRes<()> {
        remove_index(tx, name, &self.raw_id, id)?;
        Ok(())
    }
}

pub fn put_index<T: IndexType, P: Persistent>(tx: &mut Tstx, name: &str, k: &T, id: &Ref<P>) -> TRes<()> {
    tx.tsdb_impl
        .persy
        .put::<T, PersyId>(&mut tx.trans, name, k.clone(), id.raw_id.clone())?;
    Ok(())
}

pub fn remove_index<T: IndexType, P: Persistent>(tx: &mut Tstx, name: &str, k: &T, id: &Ref<P>) -> TRes<()> {
    tx.tsdb_impl
        .persy
        .remove::<T, PersyId>(&mut tx.trans, name, k.clone(), Some(id.raw_id.clone()))?;
    Ok(())
}

pub struct Ref<T> {
    type_name: String,
    raw_id: PersyId,
    ph: PhantomData<T>,
}

pub struct Tstx {
    tsdb_impl: Arc<TsdbImpl>,
    trans: Transaction,
}

impl Tstx {
    pub fn insert<T: Persistent>(&mut self, sct: &T) -> TRes<Ref<T>> {
        self.tsdb_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        let segment = T::get_description().name;
        let id = self.tsdb_impl.persy.insert_record(&mut self.trans, &segment, &buff)?;
        let id_ref = Ref {
            type_name: segment,
            raw_id: id,
            ph: PhantomData,
        };
        sct.put_indexes(self, &id_ref)?;
        Ok(id_ref)
    }

    pub fn update<T: Persistent>(&mut self, sref: &Ref<T>, sct: &T) -> TRes<()> {
        self.tsdb_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        self.tsdb_impl
            .persy
            .update_record(&mut self.trans, &sref.type_name, &sref.raw_id, &buff)?;
        sct.put_indexes(self, &sref)?;
        Ok(())
    }

    pub fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> TRes<()> {
        self.tsdb_impl.check_defined::<T>()?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        self.tsdb_impl
            .persy
            .delete_record(&mut self.trans, &sref.type_name, &sref.raw_id)?;
        Ok(())
    }

    pub fn read<T: Persistent>(&mut self, sref: &Ref<T>) -> TRes<Option<T>> {
        self.tsdb_impl.check_defined::<T>()?;
        if let Some(buff) = self
            .tsdb_impl
            .persy
            .read_record_tx(&mut self.trans, &sref.type_name, &sref.raw_id)?
        {
            Ok(Some(T::read(&mut Cursor::new(buff))?))
        } else {
            Ok(None)
        }
    }
}

impl Tsdb {
    pub fn create_if_not_exists<P: AsRef<Path>>(path: P) -> TRes<bool> {
        TsdbImpl::create_if_not_exists(path)
    }

    pub fn create<P: AsRef<Path>>(path: P) -> TRes<()> {
        TsdbImpl::create(path)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> TRes<Tsdb> {
        Ok(Tsdb {
            tsdb_impl: Arc::new(TsdbImpl::open(path)?),
        })
    }

    pub fn define<T: Persistent>(&self) -> TRes<()> {
        self.tsdb_impl.define::<T>(&self)
    }

    pub fn begin(&self) -> TRes<Tstx> {
        Ok(Tstx {
            tsdb_impl: self.tsdb_impl.clone(),
            trans: self.tsdb_impl.begin()?,
        })
    }

    fn read<T: Persistent>(&self, sref: &Ref<T>) -> TRes<Option<T>> {
        self.tsdb_impl.read(sref)
    }

    pub fn commit(&self, tx: Tstx) -> TRes<()> {
        self.tsdb_impl.commit(tx.trans)
    }
}

impl TsdbImpl {
    pub fn create_if_not_exists<P: AsRef<Path>>(path: P) -> TRes<bool> {
        if !path.as_ref().exists() {
            Persy::create(path.as_ref())?;
            TsdbImpl::init_segment(path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    fn init_segment<P: AsRef<Path>>(path: P) -> TRes<()> {
        let persy = Persy::open(path, Config::new())?;
        let mut tx = persy.begin()?;
        persy.create_segment(&mut tx, INTERNAL_SEGMENT_NAME)?;
        let prep = persy.prepare_commit(tx)?;
        persy.commit(prep)?;
        Ok(())
    }
    pub fn create<P: AsRef<Path>>(path: P) -> TRes<()> {
        Persy::create(path.as_ref())?;
        TsdbImpl::init_segment(path)?;
        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> TRes<TsdbImpl> {
        let persy = Persy::open(path, Config::new())?;
        let definitions = persy
            .scan(INTERNAL_SEGMENT_NAME)?
            .filter_map(|(_, r)| StructDescription::read(&mut Cursor::new(r)).ok())
            .map(|d| (d.name.clone(), d))
            .collect();
        Ok(TsdbImpl {
            definitions: Mutex::new(definitions),
            persy: persy,
        })
    }

    pub fn check_defined<T: Persistent>(&self) -> TRes<()> {
        let desc = T::get_description();
        let lock = self.definitions.lock()?;
        if let Some(x) = lock.get(&desc.name) {
            if x.hash_id != desc.hash_id {
                return Err(TsdbError::StructNotDefined(desc.name.clone()));
            }
        }
        Ok(())
    }

    pub fn define<T: Persistent>(&self, tsdb: &Tsdb) -> TRes<()> {
        let desc = T::get_description();
        let mut lock = self.definitions.lock()?;
        match lock.entry(desc.name.clone()) {
            Entry::Occupied(x) => {
                if x.get().hash_id != desc.hash_id {
                    return Err(TsdbError::StructAlreadyDefined(desc.name.clone()));
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
                x.insert(desc);
            }
        }
        Ok(())
    }

    pub fn begin(&self) -> TRes<Transaction> {
        Ok(self.persy.begin()?)
    }

    pub fn read<T: Persistent>(&self, sref: &Ref<T>) -> TRes<Option<T>> {
        self.check_defined::<T>()?;
        if let Some(buff) = self.persy.read_record(&sref.type_name, &sref.raw_id)? {
            Ok(Some(T::read(&mut Cursor::new(buff))?))
        } else {
            Ok(None)
        }
    }
    pub fn commit(&self, tx: Transaction) -> TRes<()> {
        let to_finalize = self.persy.prepare_commit(tx)?;
        self.persy.commit(to_finalize)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{FieldDescription, FieldType, FieldValueType, Persistent, Ref, StructDescription, TRes, Tsdb, Tstx};
    use persy::ValueMode;
    use std::fs;
    use std::io::{Read, Write};
    struct ToTest {
        name: String,
        length: u32,
    }

    impl Persistent for ToTest {
        fn get_description() -> StructDescription {
            let mut fields = Vec::new();
            fields.push(FieldDescription {
                name: "name".to_string(),
                field_type: FieldType::Value(FieldValueType::String),
                indexed: Some(ValueMode::CLUSTER),
            });
            fields.push(FieldDescription {
                name: "length".to_string(),
                field_type: FieldType::Value(FieldValueType::U32),
                indexed: None,
            });
            StructDescription {
                name: "ToTest".to_string(),
                hash_id: 10,
                fields,
            }
        }
        fn write(&self, write: &mut Write) -> TRes<()> {
            use super::PersistentEmbedded;
            self.name.write(write)?;
            self.length.write(write)?;
            Ok(())
        }
        fn read(read: &mut Read) -> TRes<Self>
        where
            Self: std::marker::Sized,
        {
            use super::PersistentEmbedded;
            Ok(ToTest {
                name: String::read(read)?,
                length: u32::read(read)?,
            })
        }

        fn declare(tx: &mut Tstx) -> TRes<()> {
            use super::declare_index;
            declare_index::<String>(tx, "ToTest.name", ValueMode::EXCLUSIVE)?;
            Ok(())
        }
        fn put_indexes(&self, tx: &mut Tstx, id: &Ref<Self>) -> TRes<()> {
            use super::IndexableValue;
            self.name.puts(tx, "ToTest.name", id)?;
            Ok(())
        }

        fn remove_indexes(&self, tx: &mut Tstx, id: &Ref<Self>) -> TRes<()> {
            use super::IndexableValue;
            self.name.removes(tx, "ToTest.name", id)?;
            Ok(())
        }
    }

    #[test()]
    fn simple_basic_flow() {
        Tsdb::create("one.db").expect("can create the database");
        let db = Tsdb::open("one.db").expect("can open the database");
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
        read.name = "new".to_string();
        tx.update(&id, &read).expect("updated correctly");
        db.commit(tx).expect("tx committed correctly");
        let read_persistent = db.read(&id).expect("read correctly").expect("this is some");
        assert_eq!(read_persistent.name, read.name);
        assert_eq!(read_persistent.length, val.length);
        fs::remove_file("one.db").expect("remove file works");
    }

}
