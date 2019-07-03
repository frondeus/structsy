pub use persy::ValueMode;
use persy::{Config, IndexType, Persy, PersyError, PersyId, Transaction};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Cursor, Error as IOError, Read, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
mod format;
pub use format::PeristentEmbedded;

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

pub enum FieldValueType {
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    F32,
    F64,
    Bool,
    String,
    Ref(String),
}
pub enum FieldType {
    Value(FieldValueType),
    Option(FieldValueType),
    Array(FieldValueType),
    OptionArray(FieldValueType),
}

impl FieldValueType {
    fn read(read: &mut Read) -> TRes<FieldValueType> {
        let sv = u8::read(read)?;
        Ok(match sv {
            1 => FieldValueType::U8,
            2 => FieldValueType::U16,
            3 => FieldValueType::U32,
            4 => FieldValueType::U64,
            5 => FieldValueType::U128,
            6 => FieldValueType::I8,
            7 => FieldValueType::I16,
            8 => FieldValueType::I32,
            9 => FieldValueType::I64,
            10 => FieldValueType::I128,
            11 => FieldValueType::F32,
            12 => FieldValueType::F64,
            13 => FieldValueType::Bool,
            14 => FieldValueType::String,
            15 => {
                let s = String::read(read)?;
                FieldValueType::Ref(s)
            }
            _ => panic!("error on de-serialization"),
        })
    }
    fn write(&self, write: &mut Write) -> TRes<()> {
        match self {
            FieldValueType::U8 => u8::write(&1, write)?,
            FieldValueType::U16 => u8::write(&2, write)?,
            FieldValueType::U32 => u8::write(&3, write)?,
            FieldValueType::U64 => u8::write(&4, write)?,
            FieldValueType::U128 => u8::write(&5, write)?,
            FieldValueType::I8 => u8::write(&6, write)?,
            FieldValueType::I16 => u8::write(&7, write)?,
            FieldValueType::I32 => u8::write(&8, write)?,
            FieldValueType::I64 => u8::write(&9, write)?,
            FieldValueType::I128 => u8::write(&10, write)?,
            FieldValueType::F32 => u8::write(&11, write)?,
            FieldValueType::F64 => u8::write(&12, write)?,
            FieldValueType::Bool => u8::write(&13, write)?,
            FieldValueType::String => u8::write(&14, write)?,
            FieldValueType::Ref(t) => {
                u8::write(&15, write)?;
                String::write(&t, write)?;
            }
        }
        Ok(())
    }
}
pub trait SupportedType {
    fn resolve() -> FieldType;
}
pub trait SimpleType {
    fn resolve() -> FieldValueType;
}
macro_rules! impl_field_type {
    ($t:ident,$v1:expr) => {
        impl SimpleType for $t {
            fn resolve() -> FieldValueType {
                $v1
            }
        }
    };
}

impl_field_type!(u8, (FieldValueType::U8));
impl_field_type!(u16, (FieldValueType::U16));
impl_field_type!(u32, (FieldValueType::U32));
impl_field_type!(u64, (FieldValueType::U64));
impl_field_type!(u128, (FieldValueType::U128));
impl_field_type!(i8, (FieldValueType::I8));
impl_field_type!(i16, (FieldValueType::I16));
impl_field_type!(i32, (FieldValueType::I32));
impl_field_type!(i64, (FieldValueType::I64));
impl_field_type!(i128, (FieldValueType::I128));
impl_field_type!(f32, (FieldValueType::F32));
impl_field_type!(f64, (FieldValueType::F64));
impl_field_type!(bool, (FieldValueType::Bool));
impl_field_type!(String, (FieldValueType::String));

impl<T: Persistent> SimpleType for Ref<T> {
    fn resolve() -> FieldValueType {
        FieldValueType::Ref(T::get_description().name)
    }
}

impl<T: SimpleType> SupportedType for T {
    fn resolve() -> FieldType {
        FieldType::Value(T::resolve())
    }
}

impl<T: SimpleType> SupportedType for Option<T> {
    fn resolve() -> FieldType {
        FieldType::Option(T::resolve())
    }
}

impl<T: SimpleType> SupportedType for Option<Vec<T>> {
    fn resolve() -> FieldType {
        FieldType::OptionArray(T::resolve())
    }
}

impl<T: SimpleType> SupportedType for Vec<T> {
    fn resolve() -> FieldType {
        FieldType::Array(T::resolve())
    }
}

impl FieldType {
    pub fn resolve<T: SupportedType>() -> FieldType {
        T::resolve()
    }

    fn read(read: &mut Read) -> TRes<FieldType> {
        let t = u8::read(read)?;
        Ok(match t {
            1 => FieldType::Value(FieldValueType::read(read)?),
            2 => FieldType::Option(FieldValueType::read(read)?),
            3 => FieldType::Array(FieldValueType::read(read)?),
            4 => FieldType::OptionArray(FieldValueType::read(read)?),
            _ => panic!("invalid value"),
        })
    }
    fn write(&self, write: &mut Write) -> TRes<()> {
        match self {
            FieldType::Value(t) => {
                u8::write(&1, write)?;
                t.write(write)?;
            }
            FieldType::Option(t) => {
                u8::write(&2, write)?;
                t.write(write)?;
            }
            FieldType::Array(t) => {
                u8::write(&3, write)?;
                t.write(write)?;
            }
            FieldType::OptionArray(t) => {
                u8::write(&4, write)?;
                t.write(write)?;
            }
        }
        Ok(())
    }
}

pub struct FieldDescription {
    name: String,
    field_type: FieldType,
    indexed: Option<ValueMode>,
}

impl FieldDescription {
    pub fn new(name: &str, field_type: FieldType, indexed: Option<ValueMode>) -> FieldDescription {
        FieldDescription {
            name: name.to_string(),
            field_type,
            indexed,
        }
    }
    fn read(read: &mut Read) -> TRes<FieldDescription> {
        let name = String::read(read)?;
        let field_type = FieldType::read(read)?;
        let indexed_value = u8::read(read)?;
        let indexed = match indexed_value {
            0 => None,
            1 => Some(ValueMode::CLUSTER),
            2 => Some(ValueMode::EXCLUSIVE),
            3 => Some(ValueMode::REPLACE),
            _ => panic!("index type reading failure"),
        };
        Ok(FieldDescription {
            name,
            field_type,
            indexed,
        })
    }
    fn write(&self, write: &mut Write) -> TRes<()> {
        self.name.write(write)?;
        self.field_type.write(write)?;
        match self.indexed {
            None => u8::write(&0, write)?,
            Some(ValueMode::CLUSTER) => u8::write(&1, write)?,
            Some(ValueMode::EXCLUSIVE) => u8::write(&2, write)?,
            Some(ValueMode::REPLACE) => u8::write(&3, write)?,
        }
        Ok(())
    }
}

pub struct StructDescription {
    name: String,
    hash_id: String,
    fields: Vec<FieldDescription>,
}

impl StructDescription {
    pub fn new(name: &str, hash_id: &str, fields: Vec<FieldDescription>) -> StructDescription {
        StructDescription {
            name: name.to_string(),
            hash_id: hash_id.to_string(),
            fields,
        }
    }
    fn read(read: &mut Read) -> TRes<StructDescription> {
        let name = String::read(read)?;
        let hash_id = String::read(read)?;
        let n_fields = u32::read(read)?;
        let mut fields = Vec::new();
        for _ in 0..n_fields {
            fields.push(FieldDescription::read(read)?);
        }
        Ok(StructDescription { name, hash_id, fields })
    }
    fn write(&self, write: &mut Write) -> TRes<()> {
        self.name.write(write)?;
        self.hash_id.write(write)?;
        (self.fields.len() as u32).write(write)?;
        for f in &self.fields {
            f.write(write)?;
        }
        Ok(())
    }
}

pub trait Persistent {
    fn get_description() -> StructDescription;
    fn write(&self, write: &mut Write) -> TRes<()>;
    fn read(read: &mut Read) -> TRes<Self>
    where
        Self: std::marker::Sized;
    fn declare(&self, db: &Tsdb) -> TRes<()>;
    fn put_indexes(&self, tx: &mut Tstx, id: &Ref<Self>) -> TRes<()>
    where
        Self: std::marker::Sized;
    fn remove_indexes(&self, tx: &mut Tstx, id: &Ref<Self>) -> TRes<()>
    where
        Self: std::marker::Sized;
}

pub fn declare_index<T: IndexType>(db: &Tsdb, name: &str, mode: ValueMode) -> TRes<()> {
    let mut tx = db.tsdb_impl.persy.begin()?;
    db.tsdb_impl.persy.create_index::<T, PersyId>(&mut tx, name, mode)?;
    let prep = db.tsdb_impl.persy.prepare_commit(tx)?;
    db.tsdb_impl.persy.commit(prep)?;
    Ok(())
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
        self.tsdb_impl.define::<T>()
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

    pub fn define<T: Persistent>(&self) -> TRes<()> {
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
                let mut tx = self.persy.begin()?;
                self.persy.insert_record(&mut tx, INTERNAL_SEGMENT_NAME, &buff)?;
                self.persy.create_segment(&mut tx, &desc.name)?;
                let prep = self.persy.prepare_commit(tx)?;
                self.persy.commit(prep)?;
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
                hash_id: "Todo!!!".to_string(),
                fields,
            }
        }
        fn write(&self, write: &mut Write) -> TRes<()> {
            use super::PeristentEmbedded;
            self.name.write(write)?;
            self.length.write(write)?;
            Ok(())
        }
        fn read(read: &mut Read) -> TRes<Self>
        where
            Self: std::marker::Sized,
        {
            use super::PeristentEmbedded;
            Ok(ToTest {
                name: String::read(read)?,
                length: u32::read(read)?,
            })
        }

        fn declare(&self, _db: &Tsdb) -> TRes<()> {
            Ok(())
        }
        fn put_indexes(&self, _tx: &mut Tstx, _id: &Ref<Self>) -> TRes<()> {
            Ok(())
        }

        fn remove_indexes(&self, _tx: &mut Tstx, _id: &Ref<Self>) -> TRes<()> {
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
