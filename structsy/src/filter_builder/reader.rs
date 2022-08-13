use crate::{
    filter_builder::{desc_info_finder::index_find_range, plan_model::IndexInfo},
    snapshot::{SnapshotIterator, SnapshotRecordIter},
    structsy::RecordIter,
    transaction::{raw_tx_scan, TxRecordIter},
    Persistent, Ref, RefSytx, SRes, Snapshot, Structsy, StructsyTx,
};

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

    pub(crate) fn find_range_from_info<P: Persistent + 'static>(
        self,
        info: IndexInfo,
    ) -> SRes<Box<dyn ReaderIterator<Item = (Ref<P>, P)> + 'a>> {
        index_find_range(
            self,
            &info.index_name,
            info.index_range.unwrap_or(info.value_type.default_range()),
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
