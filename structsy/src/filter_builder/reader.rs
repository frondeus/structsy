use crate::{
    index::{map_entry, map_entry_snapshot, map_entry_tx},
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
}
