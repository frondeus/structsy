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

pub struct FieldDescription {
    name: String,
    field_type: FieldType,
    indexed: bool,
}
impl FieldDescription {
    fn read(read: &mut Read) -> TRes<FieldDescription> {
        let name = read.read_string()?;
        //TODO: read This
        let field_type = FieldType::Value(FieldValueType::Bool);
        let indexed = read.read_bool()?;
        Ok(FieldDescription {
            name,
            field_type,
            indexed,
        })
    }
    fn write(&self, write: &mut Write) -> TRes<()> {
        write.write_string(&self.name)?;
        //TODO: write type
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
    fn write(&self, write: &Write) -> TRes<()>;
    fn read(read: &Read) -> TRes<Self>
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
        let buff = Vec::new();
        sct.write(&buff)?;
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
        let buff = Vec::new();
        sct.write(&buff)?;
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
            Ok(Some(T::read(&Cursor::new(buff))?))
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
            Persy::create(path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn create<P: AsRef<Path>>(path: P) -> TRes<()> {
        Persy::create(path)?;
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
        if !self.persy.exists_segment(INTERNAL_SEGMENT_NAME)? {
            let mut tx = self.persy.begin()?;
            self.persy.create_segment(&mut tx, INTERNAL_SEGMENT_NAME)?;
            let prep = self.persy.prepare_commit(tx)?;
            self.persy.commit(prep)?;
        }
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
            Ok(Some(T::read(&Cursor::new(buff))?))
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
