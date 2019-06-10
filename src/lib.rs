use persy::{Config, Persy, PersyError, PersyId, Transaction};
use std::io::{Cursor, Read, Write};
use std::path::Path;

pub enum TsdbError {
    PersyError(PersyError),
}

impl From<PersyError> for TsdbError {
    fn from(err: PersyError) -> TsdbError {
        TsdbError::PersyError(err)
    }
}

type TRes<T> = Result<T, TsdbError>;

pub struct Tsdb {
    persy: Persy,
}

enum FieldValueType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
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

pub trait Persistent {
    fn get_name() -> String;
    fn get_fields_definitions() -> Vec<FieldDescription>;
    fn write(&self, write: &Write) -> TRes<()>;
    fn read(read: &Read) -> TRes<Self>
    where
        Self: std::marker::Sized;
}

pub struct Ref {
    type_name: String,
    raw_id: PersyId,
}

pub struct Tstx {
    persy: Persy,
    trans: Transaction,
}

impl Tstx {
    pub fn insert<T: Persistent>(&mut self, sct: &T) -> TRes<Ref> {
        let buff = Vec::new();
        sct.write(&buff)?;
        let segment = T::get_name();
        let id = self.persy.insert_record(&mut self.trans, &segment, &buff)?;
        Ok(Ref {
            type_name: segment,
            raw_id: id,
        })
    }

    pub fn update<T: Persistent>(&mut self, sref: &Ref, sct: &T) -> TRes<()> {
        let buff = Vec::new();
        sct.write(&buff)?;
        self.persy
            .update_record(&mut self.trans, &sref.type_name, &sref.raw_id, &buff)?;
        Ok(())
    }

    pub fn delete(&mut self, sref: &Ref) -> TRes<()> {
        self.persy
            .delete_record(&mut self.trans, &sref.type_name, &sref.raw_id)?;
        Ok(())
    }

    pub fn read<T: Persistent>(&mut self, sref: &Ref) -> TRes<Option<T>> {
        if let Some(buff) =
            self.persy
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

    pub fn open<P: AsRef<Path>>(path: P) -> TRes<Tsdb> {
        Ok(Tsdb {
            persy: Persy::open(path, Config::new())?,
        })
    }

    pub fn define<T: Persistent>(&self) -> TRes<()> {
        Ok(())
    }

    pub fn begin(&self) -> TRes<Tstx> {
        Ok(Tstx {
            persy: self.persy.clone(),
            trans: self.persy.begin()?,
        })
    }

    pub fn read<T: Persistent>(&self, sref: &Ref) -> TRes<Option<T>> {
        if let Some(buff) = self.persy.read_record(&sref.type_name, &sref.raw_id)? {
            Ok(Some(T::read(&Cursor::new(buff))?))
        } else {
            Ok(None)
        }
    }
    pub fn commit(&self, tx: Tstx) -> TRes<()> {
        let to_finalize = self.persy.prepare_commit(tx.trans)?;
        self.persy.commit(to_finalize)?;
        Ok(())
    }
}
