use crate::transaction::TxIterator;
use crate::{
    filter_builder::Reader, structsy::tx_read, structsy::SnapshotIterator, Persistent, Ref, RefSytx, SRes, Snapshot,
    Structsy, StructsyImpl, Sytx,
};
use persy::{IndexType, PersyId, Transaction, ValueIter, ValueMode};
use std::ops::{Bound, RangeBounds};
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

pub(crate) fn map_entry<P: Persistent>(db: &Structsy, entry: impl Iterator<Item = PersyId>) -> Vec<(Ref<P>, P)> {
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

pub(crate) fn map_entry_snapshot<P: Persistent>(
    snap: &Snapshot,
    entry: impl Iterator<Item = PersyId>,
) -> Vec<(Ref<P>, P)> {
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

pub(crate) fn map_entry_tx<P: Persistent>(
    tx: &mut Transaction,
    st: &StructsyImpl,
    entry: impl Iterator<Item = PersyId>,
) -> Vec<(Ref<P>, P)> {
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

pub trait Finder<K> {
    fn find(&self, reader: &mut Reader, name: &str, k: &K) -> SRes<ValueIter<PersyId>>;
    fn find_range_first(&self, reader: &mut Reader, name: &str, range: (Bound<K>, Bound<K>)) -> SRes<Vec<PersyId>>;
}

pub struct IndexFinder<K> {
    p: std::marker::PhantomData<K>,
}

impl<K> Default for IndexFinder<K> {
    fn default() -> Self {
        Self {
            p: std::marker::PhantomData,
        }
    }
}

impl<K: IndexType> Finder<K> for IndexFinder<K> {
    fn find(&self, reader: &mut Reader, name: &str, k: &K) -> SRes<ValueIter<PersyId>> {
        Ok(match reader {
            Reader::Structsy(st) => st.structsy_impl.persy.get::<K, PersyId>(name, k)?,
            Reader::Snapshot(st) => st.ps.get::<K, PersyId>(name, k)?,
            Reader::Tx(tx) => tx.tx().trans.get::<K, PersyId>(name, k)?,
        })
    }
    fn find_range_first(&self, reader: &mut Reader, name: &str, range: (Bound<K>, Bound<K>)) -> SRes<Vec<PersyId>> {
        Ok(match reader {
            Reader::Structsy(st) => st
                .structsy_impl
                .persy
                .range::<K, PersyId, _>(name, range)?
                .map(|(_, v)| v.into_iter())
                .flatten()
                .take(1000)
                .collect(),
            Reader::Snapshot(snap) => snap
                .ps
                .range::<K, PersyId, _>(name, range)?
                .map(|(_, v)| v.into_iter())
                .flatten()
                .take(1000)
                .collect(),
            Reader::Tx(tx) => tx
                .tx()
                .trans
                .range::<K, PersyId, _>(name, range)?
                .map(|(_, v)| v.into_iter())
                .flatten()
                .take(1000)
                .collect(),
        })
    }
}

pub struct NoneFinder<K> {
    p: std::marker::PhantomData<K>,
}
impl<K> Default for NoneFinder<K> {
    fn default() -> Self {
        Self {
            p: std::marker::PhantomData,
        }
    }
}

impl<K> Finder<K> for NoneFinder<K> {
    fn find(&self, _reader: &mut Reader, _name: &str, _k: &K) -> SRes<ValueIter<PersyId>> {
        unreachable!();
    }

    fn find_range_first(&self, _reader: &mut Reader, _name: &str, _range: (Bound<K>, Bound<K>)) -> SRes<Vec<PersyId>> {
        unreachable!()
    }
}
