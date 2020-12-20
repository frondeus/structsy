use crate::{
    desc::DefinitionInfo, internal::Description, InternalDescription, Persistent, Ref, SRes, Structsy, StructsyConfig,
    StructsyError,
    transaction::OwnedSytx,
};
use persy::{Config, Persy, PersyId, Transaction};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Cursor;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub(crate) const INTERNAL_SEGMENT_NAME: &str = "__#internal";

struct Definitions {
    definitions: Mutex<HashMap<String, InternalDescription>>,
}

impl Definitions {
    fn new(definitions: HashMap<String, InternalDescription>) -> Definitions {
        Definitions {
            definitions: Mutex::new(definitions),
        }
    }

    pub(crate) fn check_defined<T: Persistent>(&self) -> SRes<DefinitionInfo> {
        let mut lock = self.definitions.lock()?;
        let name = T::get_name();
        if let Some(x) = lock.get_mut(name) {
            if x.checked {
                Ok(x.info())
            } else {
                let desc = T::get_description();
                if x.desc != desc {
                    Err(StructsyError::StructNotDefined(desc.get_name()))
                } else {
                    x.checked = true;
                    Ok(x.info())
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
        F: Fn(Description) -> SRes<InternalDescription>,
    {
        let desc = T::get_description();
        let mut lock = self.definitions.lock()?;
        match lock.entry(desc.get_name()) {
            Entry::Occupied(x) => {
                if x.get().desc != desc {
                    return Err(StructsyError::StructAlreadyDefined(desc.get_name()));
                }
                Ok(false)
            }
            Entry::Vacant(x) => {
                let desc = create(desc)?;
                x.insert(desc);
                Ok(true)
            }
        }
    }

    pub fn drop_defined<T: Persistent>(&self) -> SRes<InternalDescription> {
        let desc = T::get_description();
        let mut lock = self.definitions.lock()?;
        let removed = lock.remove(&desc.get_name());
        if let Some(rem) = removed {
            Ok(rem)
        } else {
            Err(StructsyError::StructNotDefined(desc.get_name()))
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
        let prep = tx.prepare()?;
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
            .filter_map(|(id, r)| InternalDescription::read(id, &mut Cursor::new(r)).ok())
            .map(|d| (d.desc.get_name(), d))
            .collect();
        Ok(StructsyImpl {
            definitions: Arc::new(Definitions::new(definitions)),
            persy,
        })
    }

    pub fn check_defined<T: Persistent>(&self) -> SRes<DefinitionInfo> {
        self.definitions.check_defined::<T>()
    }

    pub fn is_defined<T: Persistent>(&self) -> SRes<bool> {
        self.definitions.is_defined::<T>()
    }

    pub fn define<T: Persistent>(&self, structsy: &Structsy) -> SRes<bool> {
        self.definitions
            .define::<T, _>(|desc| InternalDescription::create::<T>(desc, structsy))
    }

    pub fn drop_defined<T: Persistent>(&self) -> SRes<()> {
        let int_def = self.definitions.drop_defined::<T>()?;
        let mut tx = self.persy.begin()?;
        tx.delete(INTERNAL_SEGMENT_NAME, &int_def.id)?;
        tx.drop_segment(&int_def.info().segment_name())?;
        tx.prepare()?.commit()?;
        Ok(())
    }

    pub fn begin(self:&Arc<Self>) -> SRes<OwnedSytx> {
        Ok(OwnedSytx {
            structsy_impl: self.clone(),
            trans: self.persy.begin()?,
        })
    }

    pub fn read<T: Persistent>(&self, sref: &Ref<T>) -> SRes<Option<T>> {
        let def = self.check_defined::<T>()?;
        if let Some(buff) = self.persy.read(def.segment_name(), &sref.raw_id)? {
            Ok(Some(T::read(&mut Cursor::new(buff))?))
        } else {
            Ok(None)
        }
    }

    pub fn commit(&self, tx: OwnedSytx) -> SRes<()> {
        let to_finalize = tx.trans.prepare()?;
        to_finalize.commit()?;
        Ok(())
    }

    pub fn is_referred_by_others<T: Persistent>(&self) -> SRes<bool> {
        self.definitions.is_referred_by_others::<T>()
    }

    pub fn scan<T: Persistent>(&self) -> SRes<RecordIter<T>> {
        let def = self.check_defined::<T>()?;
        Ok(RecordIter {
            iter: self.persy.scan(def.segment_name())?,
            marker: PhantomData,
        })
    }
}

pub(crate) fn tx_read<T: Persistent>(name: &str, tx: &mut Transaction, id: &PersyId) -> SRes<Option<T>> {
    if let Some(buff) = tx.read(name, id)? {
        Ok(Some(T::read(&mut Cursor::new(buff))?))
    } else {
        Ok(None)
    }
}

/// Iterator for record instances
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
