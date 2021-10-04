use crate::transaction::TxIterator;
use crate::{
    structsy::tx_read, structsy::SnapshotIterator, Persistent, Ref, RefSytx, SRes, Snapshot, Structsy, StructsyImpl,
    Sytx,
};
use persy::{IndexType, PersyId, Transaction, ValueIter, ValueMode};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::vec::IntoIter;

/// Trait implemented by all the values that can be directly indexed.
pub trait IndexableValue {
    fn puts<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()>;
    fn removes<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()>;
}

macro_rules! impl_indexable_value {
    ($($t:ty),+) => {
        $(
        impl IndexableValue for $t {
            fn puts<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
                put_index(tx, name, self, id)
            }
            fn removes<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
                remove_index(tx, name, self, id)
            }
        }
        )+
    };
}
impl_indexable_value!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, String);

impl<T: IndexableValue> IndexableValue for Option<T> {
    fn puts<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        if let Some(x) = self {
            x.puts(tx, name, id)?;
        }
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        if let Some(x) = self {
            x.removes(tx, name, id)?;
        }
        Ok(())
    }
}
impl<T: IndexableValue> IndexableValue for Vec<T> {
    fn puts<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        for x in self {
            x.puts(tx, name, id)?;
        }
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        for x in self {
            x.removes(tx, name, id)?;
        }
        Ok(())
    }
}
impl<T: Persistent> IndexableValue for Ref<T> {
    fn puts<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        put_index(tx, name, &self.raw_id, id)?;
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut dyn Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        remove_index(tx, name, &self.raw_id, id)?;
        Ok(())
    }
}

fn put_index<T: IndexType, P: Persistent>(tx: &mut dyn Sytx, name: &str, k: &T, id: &Ref<P>) -> SRes<()> {
    tx.tx().trans.put::<T, PersyId>(name, k.clone(), id.raw_id.clone())?;
    Ok(())
}

fn remove_index<T: IndexType, P: Persistent>(tx: &mut dyn Sytx, name: &str, k: &T, id: &Ref<P>) -> SRes<()> {
    tx.tx()
        .trans
        .remove::<T, PersyId>(name, k.clone(), Some(id.raw_id.clone()))?;
    Ok(())
}

fn map_unique_entry<P: Persistent>(db: &Structsy, entry: ValueIter<PersyId>) -> Option<(Ref<P>, P)> {
    if let Some(id) = entry.into_iter().next() {
        let r = Ref::new(id);
        if let Ok(val) = db.read(&r) {
            val.map(|c| (r, c))
        } else {
            None
        }
    } else {
        None
    }
}

fn map_entry<P: Persistent>(db: &Structsy, entry: ValueIter<PersyId>) -> Vec<(Ref<P>, P)> {
    entry
        .into_iter()
        .filter_map(|id| {
            let r = Ref::new(id);
            if let Ok(x) = db.read(&r) {
                x.map(|c| (r, c))
            } else {
                None
            }
        })
        .collect()
}

fn map_entry_snapshot<P: Persistent>(snap: &Snapshot, entry: ValueIter<PersyId>) -> Vec<(Ref<P>, P)> {
    entry
        .into_iter()
        .filter_map(|id| {
            let r = Ref::new(id);
            if let Ok(x) = snap.read(&r) {
                x.map(|c| (r, c))
            } else {
                None
            }
        })
        .collect()
}

fn map_unique_entry_tx<P: Persistent>(
    tx: &mut Transaction,
    st: &StructsyImpl,
    entry: ValueIter<PersyId>,
) -> Option<(Ref<P>, P)> {
    let info = st.check_defined::<P>().expect("already checked here");
    if let Some(id) = entry.into_iter().next() {
        if let Ok(val) = tx_read::<P>(info.segment_name(), tx, &id) {
            let r = Ref::new(id);
            val.map(|c| (r, c))
        } else {
            None
        }
    } else {
        None
    }
}

fn map_entry_tx<P: Persistent>(tx: &mut Transaction, st: &StructsyImpl, entry: ValueIter<PersyId>) -> Vec<(Ref<P>, P)> {
    let info = st.check_defined::<P>().expect("already checked here");
    entry
        .into_iter()
        .filter_map(|id| {
            if let Ok(val) = tx_read::<P>(info.segment_name(), tx, &id) {
                let r = Ref::new(id);
                val.map(|x| (r, x))
            } else {
                None
            }
        })
        .collect()
}

pub fn find_unique<K: IndexType, P: Persistent>(db: &Structsy, name: &str, k: &K) -> SRes<Option<(Ref<P>, P)>> {
    let id_container = db.structsy_impl.persy.get::<K, PersyId>(name, k)?;
    Ok(map_unique_entry(db, id_container))
}

pub fn find_unique_range<K: IndexType, P: Persistent, R: RangeBounds<K>>(
    db: &Structsy,
    name: &str,
    range: R,
) -> SRes<impl Iterator<Item = (Ref<P>, P, K)>> {
    let db1: Structsy = db.clone();
    Ok(db
        .structsy_impl
        .persy
        .range::<K, PersyId, R>(name, range)?
        .filter_map(move |e| {
            let k = e.0;
            map_unique_entry(&db1, e.1).map(|(r, v)| (r, v, k))
        }))
}

pub fn find<K: IndexType, P: Persistent>(db: &Structsy, name: &str, k: &K) -> SRes<Vec<(Ref<P>, P)>> {
    let e = db.structsy_impl.persy.get::<K, PersyId>(name, k)?;
    Ok(map_entry(db, e))
}

pub fn find_range<K: IndexType, P: Persistent, R: RangeBounds<K>>(
    db: &Structsy,
    name: &str,
    range: R,
) -> SRes<impl DoubleEndedIterator<Item = (Ref<P>, P, K)>> {
    let db1: Structsy = db.clone();
    Ok(db
        .structsy_impl
        .persy
        .range::<K, PersyId, R>(name, range)?
        .map(move |(key, value)| {
            map_entry(&db1, value)
                .into_iter()
                .map(move |(id, val)| (id, val, key.clone()))
        })
        .flatten())
}

pub fn find_unique_tx<K: IndexType, P: Persistent>(db: &mut dyn Sytx, name: &str, k: &K) -> SRes<Option<(Ref<P>, P)>> {
    let id_container = db.tx().trans.get::<K, PersyId>(name, k)?;
    let st = db.structsy().structsy_impl;
    Ok(map_unique_entry_tx(&mut db.tx().trans, &st, id_container))
}

pub fn find_tx<K: IndexType, P: Persistent>(db: &mut dyn Sytx, name: &str, k: &K) -> SRes<Vec<(Ref<P>, P)>> {
    let e = db.tx().trans.get::<K, PersyId>(name, k)?;
    let st = db.structsy().structsy_impl;
    Ok(map_entry_tx(&mut db.tx().trans, &st, e))
}

pub fn find_snap<K: IndexType, P: Persistent>(snapshot: &Snapshot, name: &str, k: &K) -> SRes<Vec<(Ref<P>, P)>> {
    let e = snapshot.ps.get::<K, PersyId>(name, k)?;
    Ok(map_entry_snapshot(snapshot, e))
}

/// Iterator implementation for Range of indexed persistent types
pub struct RangeIterator<'a, K: IndexType, P: Persistent> {
    structsy: Arc<StructsyImpl>,
    persy_iter: persy::TxIndexIter<'a, K, PersyId>,
    iter: Option<IntoIter<(Ref<P>, P, K)>>,
}

impl<'a, K: IndexType, P: Persistent> TxIterator<'a> for RangeIterator<'a, K, P> {
    fn tx(&mut self) -> RefSytx {
        self.tx()
    }
}

impl<'a, K: IndexType, P: Persistent> RangeIterator<'a, K, P> {
    fn new(structsy: Arc<StructsyImpl>, iter: persy::TxIndexIter<'a, K, PersyId>) -> RangeIterator<'a, K, P> {
        RangeIterator {
            structsy,
            persy_iter: iter,
            iter: None,
        }
    }

    pub fn tx(&mut self) -> RefSytx {
        RefSytx {
            structsy_impl: self.structsy.clone(),
            trans: self.persy_iter.tx(),
        }
    }

    pub fn next_tx<'b: 'a>(&'b mut self) -> Option<(Vec<(Ref<P>, P)>, K, RefSytx<'a>)> {
        if let Some((k, v, tx)) = self.persy_iter.next_tx() {
            let info = self.structsy.check_defined::<P>().expect("already checked here");
            let mut pv = Vec::new();
            for id in v {
                if let Ok(Some(val)) = tx_read::<P>(info.segment_name(), tx, &id) {
                    let r = Ref::new(id);
                    pv.push((r, val));
                }
            }
            let ref_tx = RefSytx {
                structsy_impl: self.structsy.clone(),
                trans: tx,
            };
            Some((pv, k, ref_tx))
        } else {
            None
        }
    }
}

impl<'a, P: Persistent, K: IndexType> Iterator for RangeIterator<'a, K, P> {
    type Item = (Ref<P>, P, K);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(it) = &mut self.iter {
                let next = it.next();
                if next.is_some() {
                    return next;
                }
            }

            if let Some((k, v)) = self.persy_iter.next() {
                let info = self.structsy.check_defined::<P>().expect("already checked here");
                let mut pv = Vec::new();
                for id in v {
                    let tx = self.persy_iter.tx();
                    if let Ok(Some(val)) = tx_read::<P>(info.segment_name(), tx, &id) {
                        let r = Ref::new(id);
                        pv.push((r, val, k.clone()));
                    }
                }
                self.iter = Some(pv.into_iter());
            } else {
                return None;
            }
        }
    }
}

impl<'a, P: Persistent, K: IndexType> DoubleEndedIterator for RangeIterator<'a, K, P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(it) = &mut self.iter {
                let next = it.next_back();
                if next.is_some() {
                    return next;
                }
            }

            if let Some((k, v)) = self.persy_iter.next_back() {
                let info = self.structsy.check_defined::<P>().expect("already checked here");
                let mut pv = Vec::new();
                for id in v {
                    let tx = self.persy_iter.tx();
                    if let Ok(Some(val)) = tx_read::<P>(info.segment_name(), tx, &id) {
                        let r = Ref::new(id);
                        pv.push((r, val, k.clone()));
                    }
                }
                self.iter = Some(pv.into_iter());
            } else {
                return None;
            }
        }
    }
}

/// Iterator implementation for Range of unique indexed persistent types
pub struct UniqueRangeIterator<'a, K: IndexType, P: Persistent> {
    structsy: Arc<StructsyImpl>,
    persy_iter: persy::TxIndexIter<'a, K, PersyId>,
    phantom: PhantomData<P>,
}

impl<'a, K: IndexType, P: Persistent> UniqueRangeIterator<'a, K, P> {
    fn new(structsy: Arc<StructsyImpl>, iter: persy::TxIndexIter<'a, K, PersyId>) -> UniqueRangeIterator<'a, K, P> {
        UniqueRangeIterator {
            structsy,
            persy_iter: iter,
            phantom: PhantomData,
        }
    }
    pub fn tx(&'a mut self) -> RefSytx<'a> {
        RefSytx {
            structsy_impl: self.structsy.clone(),
            trans: self.persy_iter.tx(),
        }
    }

    pub fn next_tx(&'a mut self) -> Option<(Ref<P>, P, K, RefSytx<'a>)> {
        let info = self.structsy.check_defined::<P>().expect("already checked here");
        if let Some((k, v, tx)) = self.persy_iter.next_tx() {
            if let Some(id) = v.into_iter().next() {
                if let Ok(Some(val)) = tx_read::<P>(info.segment_name(), tx, &id) {
                    let r = Ref::new(id);
                    Some((r, val, k, self.tx()))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<'a, P: Persistent, K: IndexType> Iterator for UniqueRangeIterator<'a, K, P> {
    type Item = (Ref<P>, P, K);
    fn next(&mut self) -> Option<Self::Item> {
        let info = self.structsy.check_defined::<P>().expect("already checked here");
        if let Some((k, v, tx)) = self.persy_iter.next_tx() {
            if let Some(id) = v.into_iter().next() {
                if let Ok(Some(val)) = tx_read::<P>(info.segment_name(), tx, &id) {
                    let r = Ref::new(id);
                    Some((r, val, k))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

pub fn find_unique_range_tx<'a, K: IndexType, P: Persistent, R: RangeBounds<K>>(
    db: &'a mut dyn Sytx,
    name: &str,
    r: R,
) -> SRes<UniqueRangeIterator<'a, K, P>> {
    let p1 = db.structsy().structsy_impl;
    let iter = db.tx().trans.range::<K, PersyId, R>(name, r)?;
    Ok(UniqueRangeIterator::new(p1, iter))
}

pub fn find_range_tx<'a, K: IndexType, P: Persistent, R: RangeBounds<K>>(
    db: &'a mut dyn Sytx,
    name: &str,
    r: R,
) -> SRes<RangeIterator<'a, K, P>> {
    let p1 = db.structsy().structsy_impl;
    let iter = db.tx().trans.range::<K, PersyId, R>(name, r)?;
    Ok(RangeIterator::new(p1, iter))
}

struct SnapshotRangeIterator<K, P> {
    snap: Snapshot,
    iter: Box<dyn DoubleEndedIterator<Item = (Ref<P>, P, K)>>,
}

impl<K: IndexType, P: Persistent> Iterator for SnapshotRangeIterator<K, P> {
    type Item = (Ref<P>, P, K);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
impl<K: IndexType, P: Persistent> DoubleEndedIterator for SnapshotRangeIterator<K, P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}
impl<K: IndexType, P: Persistent> SnapshotIterator for SnapshotRangeIterator<K, P> {
    fn snapshot(&self) -> &Snapshot {
        &self.snap
    }
}

pub fn find_range_snap<K: IndexType, P: Persistent, R: RangeBounds<K>>(
    snap: &Snapshot,
    name: &str,
    r: R,
) -> SRes<impl DoubleEndedIterator<Item = (Ref<P>, P, K)>> {
    let ms = snap.clone();
    Ok(snap
        .ps
        .range::<K, PersyId, R>(name, r)?
        .map(move |(key, value)| {
            map_entry_snapshot(&ms, value)
                .into_iter()
                .map(move |(id, val)| (id, val, key.clone()))
        })
        .flatten())
}

pub fn declare_index<T: IndexType>(db: &mut dyn Sytx, name: &str, mode: ValueMode) -> SRes<()> {
    db.tx().trans.create_index::<T, PersyId>(name, mode)?;
    Ok(())
}
