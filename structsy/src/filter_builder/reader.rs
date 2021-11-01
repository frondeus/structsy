use crate::{
    index::{find, find_range, find_range_snap, find_range_tx, find_snap, find_tx},
    Persistent, Ref, RefSytx, SRes, Snapshot, Structsy, StructsyTx,
};
use persy::IndexType;
use std::ops::RangeBounds;

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
    pub(crate) fn find<K: IndexType, P: Persistent>(&mut self, name: &str, k: &K) -> SRes<Vec<(Ref<P>, P)>> {
        Ok(match self {
            Reader::Structsy(st) => find(st, name, k),
            Reader::Snapshot(st) => find_snap(st, name, k),
            Reader::Tx(tx) => find_tx(tx, name, k),
        }?
        .into_iter()
        .collect())
    }

    pub(crate) fn find_range_first<K: IndexType + 'static, P: Persistent + 'static, R: RangeBounds<K> + 'static>(
        &mut self,
        name: &str,
        range: R,
    ) -> SRes<Option<Vec<(Ref<P>, P)>>> {
        let mut vec = Vec::new();
        match self {
            Reader::Structsy(st) => {
                let iter = find_range(st, name, range)?;
                let no_key = iter.map(|(r, e, _)| (r, e));
                for el in no_key {
                    vec.push(el);
                    if vec.len() == 1000 {
                        break;
                    }
                }
            }
            Reader::Snapshot(snap) => {
                let iter = find_range_snap(snap, name, range)?;
                let no_key = iter.map(|(r, e, _)| (r, e));
                for el in no_key {
                    vec.push(el);
                    if vec.len() == 1000 {
                        break;
                    }
                }
            }
            Reader::Tx(tx) => {
                let iter = find_range_tx(tx, name, range)?;
                let no_key = iter.map(|(r, e, _)| (r, e));
                for el in no_key {
                    vec.push(el);
                    if vec.len() == 1000 {
                        break;
                    }
                }
            }
        };
        if vec.len() < 1000 {
            Ok(Some(vec))
        } else {
            Ok(None)
        }
    }
}
