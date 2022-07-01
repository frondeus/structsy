use crate::{
    filter_builder::{desc_info_finder::index_find_range, plan_model::IndexInfo},
    index::{map_entry, map_entry_snapshot, map_entry_tx, RangeInstanceIter},
    snapshot::{SnapshotIterator, SnapshotRecordIter},
    structsy::RecordIter,
    transaction::{raw_tx_scan, TxRecordIter},
    Persistent, PersistentEmbedded, Ref, RefSytx, SRes, Snapshot, Structsy, StructsyTx, Sytx,
};
use std::ops::{Bound, RangeBounds};

fn clone_bound_ref<X: Clone>(bound: &Bound<&X>) -> Bound<X> {
    match bound {
        Bound::Included(x) => Bound::Included((*x).clone()),
        Bound::Excluded(x) => Bound::Excluded((*x).clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
pub trait ReaderIterator: Iterator {
    fn reader<'a>(&'a mut self) -> Reader<'a>;
}

pub(crate) enum ScanIter<'a, T> {
    Structsy((RecordIter<T>, Structsy)),
    Snapshot(SnapshotRecordIter<T>),
    Tx(TxRecordIter<'a, T>),
}
impl<'a, T: Persistent> Iterator for ScanIter<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Structsy((it, _)) => it.next(),
            Self::Snapshot(sn) => sn.next(),
            Self::Tx(tx) => tx.next(),
        }
    }
}
impl<'a, T: Persistent> ReaderIterator for ScanIter<'a, T> {
    fn reader<'b>(&'b mut self) -> Reader<'b> {
        match self {
            Self::Structsy((_, s)) => Reader::Structsy(s.clone()),
            Self::Snapshot(sn) => Reader::Snapshot(sn.snapshot().clone()),
            Self::Tx(tx) => Reader::Tx(tx.tx()),
        }
    }
}

pub enum Reader<'a> {
    Structsy(Structsy),
    Snapshot(Snapshot),
    Tx(RefSytx<'a>),
}
impl<'a> Reader<'a> {
    pub(crate) fn read<T: Persistent>(&mut self, id: &Ref<T>) -> SRes<Option<T>> {
        match self {
            Reader::Structsy(st) => st.read(id),
            Reader::Snapshot(snap) => snap.read(id),
            Reader::Tx(tx) => tx.read(id),
        }
    }

    pub(crate) fn scan<T: Persistent>(self) -> SRes<ScanIter<'a, T>> {
        match self {
            Reader::Structsy(st) => Ok(ScanIter::Structsy((st.scan::<T>()?, st.clone()))),
            Reader::Snapshot(snap) => Ok(ScanIter::Snapshot(snap.scan::<T>()?)),
            Reader::Tx(RefSytx { structsy_impl, trans }) => Ok(ScanIter::Tx(raw_tx_scan(structsy_impl, trans)?)),
        }
    }

    pub(crate) fn find<K: PersistentEmbedded + 'static, P: Persistent>(
        &mut self,
        name: &str,
        k: &K,
    ) -> SRes<Vec<(Ref<P>, P)>> {
        let iter = K::finder().find(self, name, k)?;
        Ok(match self {
            Reader::Structsy(st) => map_entry(st, iter),
            Reader::Snapshot(st) => map_entry_snapshot(st, iter),
            Reader::Tx(tx) => {
                let st = tx.structsy().structsy_impl.clone();
                map_entry_tx(tx.tx().trans, &st, iter)
            }
        })
    }

    pub(crate) fn find_range_first<
        K: PersistentEmbedded + Clone + 'static,
        P: Persistent + 'static,
        R: RangeBounds<K> + 'static,
    >(
        &mut self,
        name: &str,
        range: R,
    ) -> SRes<Option<Vec<(Ref<P>, P)>>> {
        let iter = K::finder().find_range_first(
            self,
            name,
            (
                clone_bound_ref(&range.start_bound()),
                clone_bound_ref(&range.end_bound()),
            ),
        )?;

        let vec = match self {
            Reader::Structsy(st) => map_entry(st, iter.into_iter()),
            Reader::Snapshot(st) => map_entry_snapshot(st, iter.into_iter()),
            Reader::Tx(tx) => {
                let st = tx.structsy().structsy_impl.clone();
                map_entry_tx(tx.tx().trans, &st, iter.into_iter())
            }
        };
        if vec.len() < 1000 {
            Ok(Some(vec))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn find_range<
        K: PersistentEmbedded + Clone + 'static,
        P: Persistent + 'static,
        R: RangeBounds<K> + 'static,
    >(
        self,
        name: &str,
        range: R,
    ) -> SRes<RangeInstanceIter<'a, K, P>> {
        let iter = K::finder().find_range(
            self,
            name,
            (
                clone_bound_ref(&range.start_bound()),
                clone_bound_ref(&range.end_bound()),
            ),
        )?;
        Ok(RangeInstanceIter::new(iter))
    }

    pub(crate) fn find_range_from_info<P: Persistent + 'static>(
        self,
        info: IndexInfo,
    ) -> SRes<Box<dyn ReaderIterator<Item = (Ref<P>, P)> + 'a>> {
        index_find_range(
            self,
            &info.index_name,
            info.index_range.unwrap_or((Bound::Unbounded, Bound::Unbounded)),
            info.ordering_mode,
        )
    }

    pub(crate) fn structsy(&self) -> Structsy {
        match self {
            Reader::Structsy(st) => st.clone(),
            Reader::Snapshot(st) => Structsy {
                structsy_impl: st.structsy_impl.clone(),
            },
            Reader::Tx(tx) => Structsy {
                structsy_impl: tx.structsy_impl.clone(),
            },
        }
    }
}
