use crate::{
    InternalDescription, Persistent, RecordIter, Ref, SRes, StructDescription, Structsy, StructsyConfig, StructsyError,
    StructsyTx, Sytx, TxRecordIter,
};
use persy::{Config, Persy, PersyId, Transaction};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Cursor;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Mutex;

const INTERNAL_SEGMENT_NAME: &str = "__#internal";

pub(crate) struct StructsyImpl {
    pub(crate) persy: Persy,
    definitions: Mutex<HashMap<String, InternalDescription>>,
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

    pub fn define<T: Persistent>(&self, structsy: &Structsy) -> SRes<bool> {
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
                let mut buff = Vec::new();
                desc.write(&mut buff)?;
                let mut tx = structsy.begin()?;
                tx.trans.insert_record(INTERNAL_SEGMENT_NAME, &buff)?;
                tx.trans.create_segment(&desc.name)?;
                T::declare(&mut tx)?;
                structsy.commit(tx)?;
                x.insert(InternalDescription { desc, checked: true });
                Ok(true)
            }
        }
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
        let name = T::get_name();
        for def in self.definitions.lock()?.values() {
            if def.has_refer_to(name) {
                return Ok(true);
            }
        }
        Ok(false)
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

impl<TX> StructsyTx for TX
where
    TX: Sytx + Sized,
{
    fn insert<T: Persistent>(&mut self, sct: &T) -> SRes<Ref<T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        let segment = T::get_description().name;
        let id = self.tx().trans.insert_record(&segment, &buff)?;
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
        self.tx().trans.update_record(&sref.type_name, &sref.raw_id, &buff)?;
        sct.put_indexes(self, &sref)?;
        Ok(())
    }

    fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<()> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        self.tx().trans.delete_record(&sref.type_name, &sref.raw_id)?;
        Ok(())
    }

    fn read<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<Option<T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        tx_read(&sref.type_name, &mut self.tx().trans, &sref.raw_id)
    }

    fn scan<'a, T: Persistent>(&'a mut self) -> SRes<TxRecordIter<'a, T>> {
        self.structsy().structsy_impl.check_defined::<T>()?;
        let name = T::get_description().name;
        let implc = self.structsy().structsy_impl.clone();
        let iter = self.tx().trans.scan(&name)?;
        Ok(TxRecordIter::new(iter, implc))
    }
}

pub(crate) fn tx_read<T: Persistent>(name: &str, tx: &mut Transaction, id: &PersyId) -> SRes<Option<T>> {
    if let Some(buff) = tx.read_record(name, id)? {
        Ok(Some(T::read(&mut Cursor::new(buff))?))
    } else {
        Ok(None)
    }
}
