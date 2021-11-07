use crate::transaction::TxIterator;
use crate::{
    filter_builder::{Reader, ReaderIterator},
    structsy::tx_read,
    Persistent, Ref, RefSytx, SRes, Snapshot, Structsy, StructsyImpl, Sytx,
};
use persy::{IndexType, PersyId, Transaction, ValueIter, ValueMode};
use std::ops::Bound;
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

pub fn declare_index<T: IndexType>(db: &mut dyn Sytx, name: &str, mode: ValueMode) -> SRes<()> {
    db.tx().trans.create_index::<T, PersyId>(name, mode)?;
    Ok(())
}

/// Iterator implementation for Range of indexed persistent types
pub struct IdRangeIteratorTx<'a, K: IndexType> {
    structsy: Arc<StructsyImpl>,
    persy_iter: persy::TxIndexIter<'a, K, PersyId>,
    front_key: Option<K>,
    iter: Option<IntoIter<(K, PersyId)>>,
    back_key: Option<K>,
    back_iter: Option<IntoIter<(K, PersyId)>>,
}

impl<'a, K: IndexType + PartialEq> TxIterator<'a> for IdRangeIteratorTx<'a, K> {
    fn tx(&mut self) -> RefSytx {
        self.tx()
    }
}

impl<'a, K: IndexType> IdRangeIteratorTx<'a, K> {
    fn new(structsy: Arc<StructsyImpl>, iter: persy::TxIndexIter<'a, K, PersyId>) -> IdRangeIteratorTx<'a, K> {
        IdRangeIteratorTx {
            structsy,
            persy_iter: iter,
            front_key: None,
            iter: None,
            back_key: None,
            back_iter: None,
        }
    }

    pub fn tx(&mut self) -> RefSytx {
        RefSytx {
            structsy_impl: self.structsy.clone(),
            trans: self.persy_iter.tx(),
        }
    }
}

impl<'a, K: IndexType + PartialEq> Iterator for IdRangeIteratorTx<'a, K> {
    type Item = (K, PersyId);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(it) = &mut self.iter {
                let next = it.next();
                if next.is_some() {
                    return next;
                } else if self.back_key == self.front_key {
                    return None;
                }
            }

            if let Some((k, v)) = self.persy_iter.next() {
                self.front_key = Some(k.clone());
                if self.front_key != self.back_key {
                    self.iter = Some(
                        v.into_iter()
                            .map(|val| (k.clone(), val))
                            .collect::<Vec<_>>()
                            .into_iter(),
                    );
                } else {
                    // If the front key and the back key arrived to the same level, use
                    // fill the front iterator with the back iterator that may be already in
                    // progress.
                    self.iter = self.back_iter.take();
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a, K: IndexType + PartialEq> DoubleEndedIterator for IdRangeIteratorTx<'a, K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            // if the front and the end key are the same use the front interator
            if self.front_key == self.back_key && self.front_key.is_some() {
                if let Some(it) = &mut self.iter {
                    return it.next_back();
                } else {
                    return None;
                }
            } else {
                if let Some(it) = &mut self.back_iter {
                    let next = it.next_back();
                    if next.is_some() {
                        return next;
                    }
                }
            }

            if let Some((k, v)) = self.persy_iter.next_back() {
                self.back_key = Some(k.clone());
                //If the back arrived to the same key of the front do nothing, next loop iteration
                // will use the front iterator with next back
                if self.front_key != self.back_key {
                    self.back_iter = Some(
                        v.into_iter()
                            .map(|val| (k.clone(), val))
                            .collect::<Vec<_>>()
                            .into_iter(),
                    );
                }
            } else {
                return None;
            }
        }
    }
}
/// Iterator implementation for Range of indexed persistent types
pub struct IdRangeIterator<K: IndexType> {
    persy_iter: persy::IndexIter<K, PersyId>,
    front_key: Option<K>,
    iter: Option<IntoIter<(K, PersyId)>>,
    back_key: Option<K>,
    back_iter: Option<IntoIter<(K, PersyId)>>,
}

impl<'a, K: IndexType> IdRangeIterator<K> {
    fn new(iter: persy::IndexIter<K, PersyId>) -> IdRangeIterator<K> {
        IdRangeIterator {
            persy_iter: iter,
            front_key: None,
            iter: None,
            back_key: None,
            back_iter: None,
        }
    }
}

impl<K: IndexType + PartialEq> Iterator for IdRangeIterator<K> {
    type Item = (K, PersyId);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(it) = &mut self.iter {
                let next = it.next();
                if next.is_some() {
                    return next;
                } else if self.back_key == self.front_key {
                    return None;
                }
            }

            if let Some((k, v)) = self.persy_iter.next() {
                self.front_key = Some(k.clone());
                if self.front_key != self.back_key {
                    self.iter = Some(
                        v.into_iter()
                            .map(|val| (k.clone(), val))
                            .collect::<Vec<_>>()
                            .into_iter(),
                    );
                } else {
                    // If the front key and the back key arrived to the same level, use
                    // fill the front iterator with the back iterator that may be already in
                    // progress.
                    self.iter = self.back_iter.take();
                }
            } else {
                return None;
            }
        }
    }
}

impl<K: IndexType + PartialEq> DoubleEndedIterator for IdRangeIterator<K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            // if the front and the end key are the same use the front interator
            if self.front_key == self.back_key && self.front_key.is_some() {
                if let Some(it) = &mut self.iter {
                    return it.next_back();
                } else {
                    return None;
                }
            } else {
                if let Some(it) = &mut self.back_iter {
                    let next = it.next_back();
                    if next.is_some() {
                        return next;
                    }
                }
            }

            if let Some((k, v)) = self.persy_iter.next_back() {
                self.back_key = Some(k.clone());
                //If the back arrived to the same key of the front do nothing, next loop iteration
                // will use the front iterator with next back
                if self.front_key != self.back_key {
                    self.back_iter = Some(
                        v.into_iter()
                            .map(|val| (k.clone(), val))
                            .collect::<Vec<_>>()
                            .into_iter(),
                    );
                }
            } else {
                return None;
            }
        }
    }
}

pub trait MyTrait<'a, K>: DoubleEndedIterator<Item = (K, PersyId)> + TxIterator<'a> {}
impl<'a, K: IndexType + PartialEq> MyTrait<'a, K> for IdRangeIteratorTx<'a, K> {}

pub enum RangeIter<'a, K> {
    Structsy((Box<dyn DoubleEndedIterator<Item = (K, PersyId)>>, Structsy)),
    Snapshot((Box<dyn DoubleEndedIterator<Item = (K, PersyId)>>, Snapshot)),
    Tx(Box<dyn MyTrait<'a, K> + 'a>),
}
impl<'a, K: IndexType + PartialEq + 'static> RangeIter<'a, K> {
    fn new_tx(structsy: Arc<StructsyImpl>, p: persy::TxIndexIter<'a, K, PersyId>) -> RangeIter<'a, K> {
        RangeIter::Tx(Box::new(IdRangeIteratorTx::<'a, K>::new(structsy, p)))
    }
    fn new_snap(snapshot: Snapshot, p: persy::IndexIter<K, PersyId>) -> RangeIter<'a, K> {
        RangeIter::Snapshot((Box::new(IdRangeIterator::new(p)), snapshot))
    }
    fn new(structsy: Structsy, p: persy::IndexIter<K, PersyId>) -> RangeIter<'a, K> {
        RangeIter::Structsy((Box::new(IdRangeIterator::new(p)), structsy))
    }
}
impl<'a, K> RangeIter<'a, K> {
    pub fn reader<'r>(&'r mut self) -> Reader<'r> {
        match self {
            Self::Structsy((_it, st)) => Reader::Structsy(st.clone()),
            Self::Snapshot((_it, snap)) => Reader::Snapshot(snap.clone()),
            Self::Tx(tx) => Reader::Tx(tx.tx()),
        }
    }
}
impl<'a, K: 'static> Iterator for RangeIter<'a, K> {
    type Item = (K, PersyId);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Structsy((it, _st)) => it.next(),
            Self::Snapshot((it, _snap)) => it.next(),
            Self::Tx(tx) => tx.next(),
        }
    }
}

impl<'a, K: 'static> DoubleEndedIterator for RangeIter<'a, K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Structsy((it, _st)) => it.next_back(),
            Self::Snapshot((it, _snap)) => it.next_back(),
            Self::Tx(tx) => tx.next_back(),
        }
    }
}

pub(crate) struct RangeInstanceIter<'a, K, P> {
    iter: RangeIter<'a, K>,
    marker: std::marker::PhantomData<P>,
}
impl<'a, K, P> RangeInstanceIter<'a, K, P> {
    pub(crate) fn new(iter: RangeIter<'a, K>) -> Self {
        Self {
            iter,
            marker: std::marker::PhantomData,
        }
    }
}

impl<'a, K: 'static, P: Persistent + 'static> Iterator for RangeInstanceIter<'a, K, P> {
    type Item = (Ref<P>, P);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((_, id)) = self.iter.next() {
            let mut reader = self.iter.reader();
            let rid = Ref::new(id);
            if let Ok(Some(rec)) = reader.read(&rid) {
                Some((rid, rec))
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<'a, K: 'static, P: Persistent + 'static> DoubleEndedIterator for RangeInstanceIter<'a, K, P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some((_, id)) = self.iter.next_back() {
            let mut reader = self.iter.reader();
            let rid = Ref::new(id);
            if let Ok(Some(rec)) = reader.read(&rid) {
                Some((rid, rec))
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<'a, K: 'static, P: Persistent + 'static> ReaderIterator for RangeInstanceIter<'a, K, P> {
    fn reader<'b>(&'b mut self) -> Reader<'b> {
        self.iter.reader()
    }
}

pub trait Finder<K> {
    fn find(&self, reader: &mut Reader, name: &str, k: &K) -> SRes<ValueIter<PersyId>>;
    fn find_range_first(&self, reader: &mut Reader, name: &str, range: (Bound<K>, Bound<K>)) -> SRes<Vec<PersyId>>;
    fn find_range<'a>(&self, reader: Reader<'a>, name: &str, range: (Bound<K>, Bound<K>)) -> SRes<RangeIter<'a, K>>;
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

impl<K: IndexType + PartialEq + 'static> Finder<K> for IndexFinder<K> {
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

    fn find_range<'a>(&self, reader: Reader<'a>, name: &str, range: (Bound<K>, Bound<K>)) -> SRes<RangeIter<'a, K>> {
        Ok(match reader {
            Reader::Structsy(st) => {
                RangeIter::new(st.clone(), st.structsy_impl.persy.range::<K, PersyId, _>(name, range)?)
            }
            Reader::Snapshot(snap) => RangeIter::new_snap(snap.clone(), snap.ps.range::<K, PersyId, _>(name, range)?),
            Reader::Tx(RefSytx { structsy_impl, trans }) => {
                RangeIter::new_tx(structsy_impl, trans.range::<K, PersyId, _>(name, range)?)
            }
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
    fn find_range<'a>(&self, _reader: Reader<'a>, _name: &str, _range: (Bound<K>, Bound<K>)) -> SRes<RangeIter<'a, K>> {
        unreachable!()
    }
}
