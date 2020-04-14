use crate::{
    InternalDescription, Persistent, RecordIter, Ref, SRes, StructDescription, Structsy, StructsyConfig, StructsyError,
    StructsyTx,
};
use persy::{Config, Persy, PersyId, Transaction};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Cursor;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};

const INTERNAL_SEGMENT_NAME: &str = "__#internal";

struct Definitions {
    definitions: Mutex<HashMap<String, InternalDescription>>,
}

impl Definitions {
    fn new(definitions: HashMap<String, InternalDescription>) -> Definitions {
        Definitions {
            definitions: Mutex::new(definitions),
        }
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

    pub fn define<T: Persistent, F>(&self, create: F) -> SRes<bool>
    where
        F: Fn(&StructDescription) -> SRes<()>,
    {
        let desc = T::get_description();
        let mut lock = self.definitions.lock()?;
        match lock.entry(desc.name.clone()) {
            Entry::Occupied(x) => {
                if x.get().desc != desc {
                    return Err(StructsyError::StructAlreadyDefined(desc.name.clone()));
                }
                Ok(false)
            }
            Entry::Vacant(x) => {
                create(&desc)?;
                x.insert(InternalDescription { desc, checked: true });
                Ok(true)
            }
        }
    }

    pub fn is_referred_by_others<T: Persistent>(&self) -> SRes<bool> {
        let name = T::get_name();
        for def in self.definitions.lock()?.values() {
            if def.has_refer_to(name) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(crate) struct StructsyImpl {
    pub(crate) persy: Persy,
    definitions: Arc<Definitions>,
}

impl StructsyImpl {
    fn init_segment<P: AsRef<Path>>(path: P) -> SRes<()> {
        let persy = Persy::open(path, Config::new())?;
        let mut tx = persy.begin()?;
        tx.create_segment(INTERNAL_SEGMENT_NAME)?;
        let prep = tx.prepare_commit()?;
        prep.commit()?;
        Ok(())
    }

    pub fn open(config: StructsyConfig) -> SRes<StructsyImpl> {
        if config.create && !config.path.exists() {
            Persy::create(&config.path)?;
            StructsyImpl::init_segment(&config.path)?;
        }
        let persy = Persy::open(&config.path, Config::new())?;
        let definitions = persy
            .scan(INTERNAL_SEGMENT_NAME)?
            .filter_map(|(_, r)| StructDescription::read(&mut Cursor::new(r)).ok())
            .map(|d| {
                (
                    d.name.clone(),
                    InternalDescription {
                        desc: d,
                        checked: false,
                    },
                )
            })
            .collect();
        Ok(StructsyImpl {
            definitions: Arc::new(Definitions::new(definitions)),
            persy,
        })
    }

    pub fn check_defined<T: Persistent>(&self) -> SRes<()> {
        self.definitions.check_defined::<T>()
    }

    pub fn is_defined<T: Persistent>(&self) -> SRes<bool> {
        self.definitions.is_defined::<T>()
    }

    pub fn define<T: Persistent>(&self, structsy: &Structsy) -> SRes<bool> {
        self.definitions.define::<T, _>(|desc| {
            let mut buff = Vec::new();
            desc.write(&mut buff)?;
            let mut tx = structsy.begin()?;
            tx.trans.insert_record(INTERNAL_SEGMENT_NAME, &buff)?;
            tx.trans.create_segment(&desc.name)?;
            T::declare(&mut tx)?;
            tx.commit()?;
            Ok(())
        })
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
        let to_finalize = tx.prepare_commit()?;
        to_finalize.commit()?;
        Ok(())
    }
    pub fn is_referred_by_others<T: Persistent>(&self) -> SRes<bool> {
        self.definitions.is_referred_by_others::<T>()
    }
    pub fn scan<T: Persistent>(&self) -> SRes<RecordIter<T>> {
        self.check_defined::<T>()?;
        let name = T::get_description().name;
        Ok(RecordIter {
            iter: self.persy.scan(&name)?,
            marker: PhantomData,
        })
    }
}

pub(crate) fn tx_read<T: Persistent>(name: &str, tx: &mut Transaction, id: &PersyId) -> SRes<Option<T>> {
    if let Some(buff) = tx.read_record(name, id)? {
        Ok(Some(T::read(&mut Cursor::new(buff))?))
    } else {
        Ok(None)
    }
}
