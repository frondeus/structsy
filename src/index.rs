use crate::{Persistent, Ref, SRes, Structsy, Sytx};
use persy::{IndexType, PersyId, Value};
use std::ops::RangeBounds;

pub trait IndexableValue {
    fn puts<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()>;
    fn removes<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()>;
}

macro_rules! impl_indexable_value {
    ($t:ident) => {
        impl IndexableValue for $t {
            fn puts<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
                put_index(tx, name, self, id)
            }
            fn removes<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
                remove_index(tx, name, self, id)
            }
        }
    };
}
impl_indexable_value!(u8);
impl_indexable_value!(u16);
impl_indexable_value!(u32);
impl_indexable_value!(u64);
impl_indexable_value!(u128);
impl_indexable_value!(i8);
impl_indexable_value!(i16);
impl_indexable_value!(i32);
impl_indexable_value!(i64);
impl_indexable_value!(i128);
impl_indexable_value!(String);

impl<T: IndexableValue> IndexableValue for Option<T> {
    fn puts<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        if let Some(x) = self {
            x.puts(tx, name, id)?;
        }
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        if let Some(x) = self {
            x.removes(tx, name, id)?;
        }
        Ok(())
    }
}
impl<T: IndexableValue> IndexableValue for Vec<T> {
    fn puts<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        for x in self {
            x.puts(tx, name, id)?;
        }
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        for x in self {
            x.removes(tx, name, id)?;
        }
        Ok(())
    }
}
impl<T: Persistent> IndexableValue for Ref<T> {
    fn puts<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        put_index(tx, name, &self.raw_id, id)?;
        Ok(())
    }
    fn removes<P: Persistent>(&self, tx: &mut Sytx, name: &str, id: &Ref<P>) -> SRes<()> {
        remove_index(tx, name, &self.raw_id, id)?;
        Ok(())
    }
}

fn put_index<T: IndexType, P: Persistent>(tx: &mut Sytx, name: &str, k: &T, id: &Ref<P>) -> SRes<()> {
    tx.tsdb_impl
        .persy
        .put::<T, PersyId>(&mut tx.trans, name, k.clone(), id.raw_id.clone())?;
    Ok(())
}

fn remove_index<T: IndexType, P: Persistent>(tx: &mut Sytx, name: &str, k: &T, id: &Ref<P>) -> SRes<()> {
    tx.tsdb_impl
        .persy
        .remove::<T, PersyId>(&mut tx.trans, name, k.clone(), Some(id.raw_id.clone()))?;
    Ok(())
}

pub(crate) fn map_unique_entry<P: Persistent>(db: &Structsy, entry: Value<PersyId>) -> Option<(Ref<P>, P)> {
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

pub(crate) fn map_entry<P: Persistent>(db: &Structsy, entry: Value<PersyId>) -> Vec<(Ref<P>, P)> {
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

pub(crate) fn map_unique_entry_tx<P: Persistent>(db: &mut Sytx, entry: Value<PersyId>) -> Option<(Ref<P>, P)> {
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

pub(crate) fn map_entry_tx<P: Persistent>(db: &mut Sytx, entry: Value<PersyId>) -> Vec<(Ref<P>, P)> {
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

pub fn find_unique<K: IndexType, P: Persistent>(db: &Structsy, name: &str, k: &K) -> SRes<Option<(Ref<P>, P)>> {
    if let Some(id_container) = db.tsdb_impl.persy.get::<K, PersyId>(name, k)? {
        Ok(map_unique_entry(db, id_container))
    } else {
        Ok(None)
    }
}

pub fn find_unique_range<K: IndexType, P: Persistent, R: RangeBounds<K>>(
    db: &Structsy,
    name: &str,
    range: R,
) -> SRes<impl Iterator<Item = (K, (Ref<P>, P))>> {
    let db1: Structsy = db.clone();
    Ok(db
        .tsdb_impl
        .persy
        .range::<K, PersyId, R>(name, range)?
        .filter_map(move |e| {
            let k = e.0;
            map_unique_entry(&db1, e.1).map(|x| (k, x))
        }))
}

pub fn find<K: IndexType, P: Persistent>(db: &Structsy, name: &str, k: &K) -> SRes<Vec<(Ref<P>, P)>> {
    if let Some(e) = db.tsdb_impl.persy.get::<K, PersyId>(name, k)? {
        Ok(map_entry(db, e))
    } else {
        Ok(Vec::new())
    }
}

pub fn find_range<K: IndexType, P: Persistent, R: RangeBounds<K>>(
    db: &Structsy,
    name: &str,
    range: R,
) -> SRes<impl Iterator<Item = (K, Vec<(Ref<P>, P)>)>> {
    let db1: Structsy = db.clone();
    Ok(db
        .tsdb_impl
        .persy
        .range::<K, PersyId, R>(name, range)?
        .map(move |e| (e.0, map_entry(&db1, e.1))))
}

pub fn find_unique_tx<K: IndexType, P: Persistent>(db: &mut Sytx, name: &str, k: &K) -> SRes<Option<(Ref<P>, P)>> {
    if let Some(id_container) = db.tsdb_impl.persy.get_tx::<K, PersyId>(&mut db.trans, name, k)? {
        Ok(map_unique_entry_tx(db, id_container))
    } else {
        Ok(None)
    }
}

pub fn find_tx<K: IndexType, P: Persistent>(db: &mut Sytx, name: &str, k: &K) -> SRes<Vec<(Ref<P>, P)>> {
    if let Some(e) = db.tsdb_impl.persy.get_tx::<K, PersyId>(&mut db.trans, name, k)? {
        Ok(map_entry_tx(db, e))
    } else {
        Ok(Vec::new())
    }
}
