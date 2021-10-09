use crate::{
    desc::DefinitionInfo,
    id::{raw_format, raw_parse},
    internal::Description,
    record::Record,
    transaction::OwnedSytx,
    InternalDescription, Persistent, PersistentEmbedded, RawAccess, RawRead, Ref, SRes, Snapshot, Structsy,
    StructsyConfig, StructsyError, StructsyTx,
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

    pub fn define_raw<F>(&self, desc: Description, create: F) -> SRes<bool>
    where
        F: Fn(Description) -> SRes<InternalDescription>,
    {
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

    pub fn define<T: Persistent, F>(&self, create: F) -> SRes<bool>
    where
        F: Fn(Description) -> SRes<InternalDescription>,
    {
        self.define_raw(T::get_description(), create)
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

    pub fn is_migration_started<T: Persistent>(&self) -> SRes<bool> {
        let name = T::get_name();
        let lock = self.definitions.lock()?;
        Ok(if let Some(to_check) = lock.get(name) {
            to_check.is_migration_started()
        } else {
            false
        })
    }

    pub fn start_migration<T: Persistent>(&self, st: &Arc<StructsyImpl>) -> SRes<()> {
        let name = T::get_name();
        let mut lock = self.definitions.lock()?;
        if let Some(to_change) = lock.get_mut(name) {
            to_change.start_migration();
            to_change.update(st)?;
        }

        Ok(())
    }

    pub fn finish_migration<S: Persistent, D: Persistent>(&self, st: &Arc<StructsyImpl>) -> SRes<()> {
        let name = S::get_name();
        let mut tx = st.begin()?;
        let mut lock = self.definitions.lock()?;
        if let Some(mut to_change) = lock.remove(name) {
            to_change.migrate::<D>(&mut tx)?;
            lock.insert(D::get_name().to_string(), to_change);
        }
        for (_, v) in lock.iter_mut() {
            if v.remap_refer(name, D::get_name()) {
                v.update_tx(&mut tx)?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list(&self) -> SRes<impl std::iter::Iterator<Item = Description>> {
        let values = {
            self.definitions
                .lock()?
                .values()
                .map(|v| v.desc.clone())
                .collect::<Vec<_>>()
        };
        Ok(values.into_iter())
    }

    pub(crate) fn full_definition_by_name(&self, name: &str) -> SRes<InternalDescription> {
        let lock = self.definitions.lock()?;
        if let Some(x) = lock.get(name) {
            Ok(x.clone())
        } else {
            Err(StructsyError::StructNotDefined(name.to_owned()))
        }
    }
}

pub(crate) struct StructsyImpl {
    pub(crate) persy: Persy,
    definitions: Arc<Definitions>,
}

impl StructsyImpl {
    pub fn migrate<S, D>(self: &Arc<Self>) -> SRes<()>
    where
        S: Persistent,
        D: Persistent,
        D: From<S>,
    {
        if !self.is_defined::<S>()? {
            return Ok(());
        }
        let info = self.check_defined::<S>()?;
        let migration_batches = format!("--migration-{}-{}", S::get_name(), D::get_name());
        if self.persy.exists_segment(&migration_batches)? && !self.definitions.is_migration_started::<S>()? {
            let mut tx = self.begin()?;
            tx.trans.drop_segment(&migration_batches)?;
            tx.commit()?;
        }
        if !self.persy.exists_segment(&migration_batches)? {
            let mut tx = self.begin()?;
            tx.trans.create_segment(&migration_batches)?;
            tx.commit()?;
            let batch_size = 1000;
            let batch_commit_size = batch_size * 1000;
            let mut count = 0;
            tx = self.begin()?;
            let mut mig_batch = Vec::new();
            for (id, _record) in self.scan::<S>()? {
                mig_batch.push(id);
                count += 1;
                if count % batch_size == 0 {
                    let mut buff = Vec::new();
                    PersistentEmbedded::write(&mig_batch, &mut buff)?;
                    tx.trans.insert(&migration_batches, &buff)?;
                    mig_batch.clear();
                }
                if count % batch_commit_size == 0 {
                    tx.commit()?;
                    tx = self.begin()?;
                }
            }
            if !mig_batch.is_empty() {
                let mut buff = Vec::new();
                PersistentEmbedded::write(&mig_batch, &mut buff)?;
                tx.trans.insert(&migration_batches, &buff)?;
                mig_batch.clear();
            }
            tx.commit()?;
        }
        self.definitions.start_migration::<S>(self)?;
        let mut tx = self.begin()?;
        for (batch_id, record) in self.persy.scan(&migration_batches)? {
            let batch: Vec<Ref<S>> = PersistentEmbedded::read(&mut Cursor::new(record))?;
            for id in batch {
                let mut buff = Vec::new();
                D::from(tx.read(&id)?.unwrap()).write(&mut buff)?;
                tx.trans.update(info.segment_name(), &id.raw_id, &buff)?;
            }
            tx.trans.delete(&migration_batches, &batch_id)?;
            tx.commit()?;
            tx = self.begin()?;
        }
        tx.commit()?;
        self.definitions.finish_migration::<S, D>(self)?;
        Ok(())
    }

    fn init_segment<P: AsRef<Path>>(path: P) -> SRes<()> {
        let persy = Persy::open(path, Config::new())?;
        let mut tx = persy.begin()?;
        tx.create_segment(INTERNAL_SEGMENT_NAME)?;
        tx.prepare()?.commit()?;
        Ok(())
    }

    pub fn memory() -> SRes<StructsyImpl> {
        let persy = persy::OpenOptions::new().memory()?;
        let mut tx = persy.begin()?;
        tx.create_segment(INTERNAL_SEGMENT_NAME)?;
        tx.prepare()?.commit()?;
        let definitions = HashMap::new();
        Ok(StructsyImpl {
            definitions: Arc::new(Definitions::new(definitions)),
            persy,
        })
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

    pub fn define<T: Persistent>(self: &Arc<StructsyImpl>) -> SRes<bool> {
        self.definitions
            .define::<T, _>(|desc| InternalDescription::create::<T>(desc, self))
    }

    pub fn undefine<T: Persistent>(&self) -> SRes<()> {
        let int_def = self.definitions.drop_defined::<T>()?;
        let mut tx = self.persy.begin()?;
        if let Description::Struct(s) = &int_def.desc {
            for field in &s.fields {
                if field.indexed.is_some() {
                    tx.drop_index(&format!("{}.{}", s.get_name(), field.name))?;
                }
            }
        }
        tx.delete(INTERNAL_SEGMENT_NAME, &int_def.id)?;
        tx.drop_segment(int_def.info().segment_name())?;
        tx.prepare()?.commit()?;
        Ok(())
    }

    pub fn begin(self: &Arc<Self>) -> SRes<OwnedSytx> {
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

    pub fn scan<T: Persistent>(&self) -> SRes<RecordIter<T>> {
        let def = self.check_defined::<T>()?;
        Ok(RecordIter {
            iter: self.persy.scan(def.segment_name())?,
            marker: PhantomData,
        })
    }

    pub fn read_snapshot<T: Persistent>(&self, snap: &Snapshot, sref: &Ref<T>) -> SRes<Option<T>> {
        let def = self.check_defined::<T>()?;
        if let Some(buff) = snap.ps.read(def.segment_name(), &sref.raw_id)? {
            Ok(Some(T::read(&mut Cursor::new(buff))?))
        } else {
            Ok(None)
        }
    }

    pub fn scan_snapshot<T: Persistent>(&self, snap: &Snapshot) -> SRes<SnapshotRecordIter<T>> {
        let def = self.check_defined::<T>()?;
        Ok(SnapshotRecordIter {
            iter: snap.ps.scan(def.segment_name())?,
            snapshot: snap.clone(),
            marker: PhantomData,
        })
    }

    pub fn list_defined(&self) -> SRes<impl std::iter::Iterator<Item = Description>> {
        self.definitions.list()
    }
    pub(crate) fn full_definition_by_name(&self, name: &str) -> SRes<InternalDescription> {
        self.definitions.full_definition_by_name(name)
    }
}

impl RawRead for Structsy {
    fn raw_scan(&self, strct_name: &str) -> SRes<RawIter> {
        let definition = self.structsy_impl.definitions.full_definition_by_name(strct_name)?;
        Ok(RawIter {
            iter: self.structsy_impl.persy.scan(&definition.info().segment_name())?,
            description: definition,
        })
    }
    fn raw_read(&self, id: &str) -> SRes<Option<Record>> {
        let (ty, pid) = raw_parse(id)?;
        let definition = self.structsy_impl.definitions.full_definition_by_name(ty)?;
        let rid: PersyId = pid.parse().or(Err(StructsyError::InvalidId))?;
        let raw = self.structsy_impl.persy.read(&definition.info().segment_name(), &rid)?;
        if let Some(data) = raw {
            Ok(Some(Record::read(&mut Cursor::new(data), &definition.desc)?))
        } else {
            Ok(None)
        }
    }
}

impl RawAccess for Structsy {
    fn raw_begin(&self) -> SRes<RawTransaction> {
        Ok(RawTransaction {
            tx: self.structsy_impl.persy.begin()?,
            structsy_impl: self.structsy_impl.clone(),
        })
    }

    fn raw_define(&self, desc: Description) -> SRes<bool> {
        self.structsy_impl
            .definitions
            .define_raw::<_>(desc, |desc| InternalDescription::create_raw(desc, &self.structsy_impl))
    }
}

/// Transaction for save data in a structsy database without the original source
/// code
pub struct RawTransaction {
    tx: persy::Transaction,
    structsy_impl: Arc<StructsyImpl>,
}
impl RawTransaction {
    pub fn raw_insert(&mut self, record: &Record) -> SRes<String> {
        let type_name = record.type_name();
        let definition = self.structsy_impl.definitions.full_definition_by_name(type_name)?;
        let mut data = Vec::new();
        record.write(&mut data, &definition.desc)?;
        let id = self.tx.insert(definition.info().segment_name(), &data)?;
        record.put_indexes(&mut self.tx, &id)?;
        Ok(raw_format(type_name, &id))
    }
    pub fn raw_update(&mut self, id: &str, record: &Record) -> SRes<()> {
        let (_, pid) = raw_parse(id)?;
        let type_name = record.type_name();
        let definition = self.structsy_impl.definitions.full_definition_by_name(type_name)?;
        let ppid = pid.parse()?;
        if let Some(record) = self.raw_read(id)? {
            record.remove_indexes(&mut self.tx, &ppid)?;
        }
        let mut data = Vec::new();
        record.write(&mut data, &definition.desc)?;
        self.tx.update(definition.info().segment_name(), &ppid, &data)?;
        record.put_indexes(&mut self.tx, &ppid)?;
        Ok(())
    }
    pub fn raw_delete(&mut self, id: &str) -> SRes<()> {
        let (type_name, pid) = raw_parse(id)?;
        let definition = self.structsy_impl.definitions.full_definition_by_name(type_name)?;
        let ppid = pid.parse()?;
        if let Some(record) = self.raw_read(id)? {
            record.remove_indexes(&mut self.tx, &ppid)?;
        }
        self.tx.delete(definition.info().segment_name(), &ppid)?;
        Ok(())
    }

    pub fn raw_read(&mut self, id: &str) -> SRes<Option<Record>> {
        let (ty, pid) = raw_parse(id)?;
        let definition = self.structsy_impl.definitions.full_definition_by_name(ty)?;
        let rid: PersyId = pid.parse().or(Err(StructsyError::InvalidId))?;
        let raw = self.tx.read(&definition.info().segment_name(), &rid)?;
        if let Some(data) = raw {
            Ok(Some(Record::read(&mut Cursor::new(data), &definition.desc)?))
        } else {
            Ok(None)
        }
    }

    pub fn prepare(self) -> SRes<RawPrepare> {
        Ok(RawPrepare {
            prepared: self.tx.prepare()?,
        })
    }
}
/// Prepared state of RawTransaction
pub struct RawPrepare {
    prepared: persy::TransactionFinalize,
}
impl RawPrepare {
    pub fn commit(self) -> SRes<()> {
        Ok(self.prepared.commit()?)
    }
}

/// Iterator of raw Records
pub struct RawIter {
    iter: persy::SegmentIter,
    description: InternalDescription,
}
impl RawIter {
    pub(crate) fn new(iter: persy::SegmentIter, description: InternalDescription) -> Self {
        Self { iter, description }
    }
}
impl Iterator for RawIter {
    type Item = (String, Record);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, data)) = self.iter.next() {
            let fid = format!("{}@{}", self.description.desc.get_name(), id);
            let rec = Record::read(&mut Cursor::new(data), &self.description.desc).unwrap();
            Some((fid, rec))
        } else {
            None
        }
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

pub trait SnapshotIterator: Iterator {
    fn snapshot(&self) -> &Snapshot;
}

/// Iterator for record instances
pub struct SnapshotRecordIter<T: Persistent> {
    iter: persy::SnapshotSegmentIter,
    snapshot: Snapshot,
    marker: PhantomData<T>,
}

impl<T: Persistent> Iterator for SnapshotRecordIter<T> {
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
impl<T: Persistent> SnapshotIterator for SnapshotRecordIter<T> {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
}
