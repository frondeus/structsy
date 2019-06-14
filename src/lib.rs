use persy::{Config, Persy, PersyError, PersyId, Transaction};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Cursor, Error as IOError, Read, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

mod format;
use format::{TRead, TWrite};

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

type TRes<T> = Result<T, TsdbError>;

pub struct TsdbImpl {
    persy: Persy,
    definitions: Mutex<HashMap<String, StructDescription>>,
}

pub struct Tsdb {
    tsdb_impl: Arc<TsdbImpl>,
}

enum FieldValueType {
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
enum FieldType {
    Value(FieldValueType),
    Option(FieldValueType),
    Array(FieldValueType),
    OptionArray(FieldValueType),
}

impl FieldValueType {
    fn read(read: &mut Read) -> TRes<FieldValueType> {
        let sv = read.read_u8()?;
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
                let s = read.read_string()?;
                FieldValueType::Ref(s)
            }
            _ => panic!("error on de-serialization"),
        })
    }
    fn write(&self, write: &mut Write) -> TRes<()> {
        match self {
            FieldValueType::U8 => write.write_u8(1)?,
            FieldValueType::U16 => write.write_u8(2)?,
            FieldValueType::U32 => write.write_u8(3)?,
            FieldValueType::U64 => write.write_u8(4)?,
            FieldValueType::U128 => write.write_u8(5)?,
            FieldValueType::I8 => write.write_u8(6)?,
            FieldValueType::I16 => write.write_u8(7)?,
            FieldValueType::I32 => write.write_u8(8)?,
            FieldValueType::I64 => write.write_u8(9)?,
            FieldValueType::I128 => write.write_u8(10)?,
            FieldValueType::F32 => write.write_u8(11)?,
            FieldValueType::F64 => write.write_u8(12)?,
            FieldValueType::Bool => write.write_u8(13)?,
            FieldValueType::String => write.write_u8(14)?,
            FieldValueType::Ref(t) => {
                write.write_u8(15)?;
                write.write_string(&t)?;
            }
        }
        Ok(())
    }
}

impl FieldType {
    fn read(read: &mut Read) -> TRes<FieldType> {
        let t = read.read_u8()?;
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
                write.write_u8(1)?;
                t.write(write)?;
            }
            FieldType::Option(t) => {
                write.write_u8(2)?;
                t.write(write)?;
            }
            FieldType::Array(t) => {
                write.write_u8(3)?;
                t.write(write)?;
            }
            FieldType::OptionArray(t) => {
                write.write_u8(4)?;
                t.write(write)?;
            }
        }
        Ok(())
    }
}

pub struct FieldDescription {
    name: String,
    field_type: FieldType,
    indexed: bool,
}

impl FieldDescription {
    fn read(read: &mut Read) -> TRes<FieldDescription> {
        let name = read.read_string()?;
        let field_type = FieldType::read(read)?;
        let indexed = read.read_bool()?;
        Ok(FieldDescription {
            name,
            field_type,
            indexed,
        })
    }
    fn write(&self, write: &mut Write) -> TRes<()> {
        write.write_string(&self.name)?;
        self.field_type.write(write)?;
        write.write_bool(self.indexed)?;
        Ok(())
    }
}

pub struct StructDescription {
    name: String,
    hash_id: String,
    fields: Vec<FieldDescription>,
}

impl StructDescription {
    fn read(read: &mut Read) -> TRes<StructDescription> {
        let name = read.read_string()?;
        let hash_id = read.read_string()?;
        let n_fields = read.read_u32()?;
        let mut fields = Vec::new();
        for _ in 0..n_fields {
            fields.push(FieldDescription::read(read)?);
        }
        Ok(StructDescription { name, hash_id, fields })
    }
    fn write(&self, write: &mut Write) -> TRes<()> {
        write.write_string(&self.name)?;
        write.write_string(&self.hash_id)?;
        write.write_u32(self.fields.len() as u32)?;
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
        Ok(Ref {
            type_name: segment,
            raw_id: id,
            ph: PhantomData,
        })
    }

    pub fn update<T: Persistent>(&mut self, sref: &Ref<T>, sct: &T) -> TRes<()> {
        self.tsdb_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        self.tsdb_impl
            .persy
            .update_record(&mut self.trans, &sref.type_name, &sref.raw_id, &buff)?;
        Ok(())
    }

    pub fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> TRes<()> {
        self.tsdb_impl.check_defined::<T>()?;
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

    pub fn read<T: Persistent>(&self, sref: &Ref<T>) -> TRes<Option<T>> {
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
    fn init_segment<P:AsRef<Path>>(path:P) -> TRes<()>{
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
    use super::format::{TRead, TWrite};
    use super::{FieldDescription, FieldType, FieldValueType, Persistent, StructDescription, TRes, Tsdb};
    use std::io::{Read, Write};
    use std::fs;
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
                indexed: false,
            });
            fields.push(FieldDescription {
                name: "length".to_string(),
                field_type: FieldType::Value(FieldValueType::U32),
                indexed: false,
            });
            StructDescription {
                name: "ToTest".to_string(),
                hash_id: "Todo!!!".to_string(),
                fields,
            }
        }
        fn write(&self, write: &mut Write) -> TRes<()> {
            write.write_string(&self.name)?;
            write.write_u32(self.length)?;
            Ok(())
        }
        fn read(read: &mut Read) -> TRes<Self>
        where
            Self: std::marker::Sized,
        {
            Ok(ToTest {
                name: read.read_string()?,
                length: read.read_u32()?,
            })
        }
    }

    #[test()]
    fn simple_basic_flow() {
        Tsdb::create("one.db").expect("can create the database");
        let db = Tsdb::open("one.db").expect("can open the database");
        db.define::<ToTest>().expect("is define correctly");
        let mut tx = db.begin().expect("can start a transaction");
        let val = ToTest {
            name:"one".to_string(),
            length:3,
        };
        let id = tx.insert(&val).expect("insert correctly");
        let mut read = tx.read(&id).expect("read correctly").expect("this should be some");
        assert_eq!(read.name, val.name);
        assert_eq!(read.length, val.length);
        read.name ="new".to_string();
        tx.update(&id,&read).expect("updated correctly");
        db.commit(tx).expect("tx committed correctly");
        let read_persistent =db.read(&id).expect("read correctly").expect("this is some");
        assert_eq!(read_persistent.name, read.name);
        assert_eq!(read_persistent.length, val.length);
        fs::remove_file("one.db").expect("remove file works");
    }

}
