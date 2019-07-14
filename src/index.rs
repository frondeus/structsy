use crate::{tx_read, Persistent, Ref, RefSytx, SRes, Structsy, StructsyImpl, Sytx};
use persy::{IndexType, Persy, PersyId, Transaction, Value};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::vec::IntoIter;

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
impl_indexable_value!(f32);
impl_indexable_value!(f64);
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
    let persy = &tx.structsy().structsy_impl.persy;
    persy.put::<T, PersyId>(&mut tx.tx().trans, name, k.clone(), id.raw_id.clone())?;
    Ok(())
}

fn remove_index<T: IndexType, P: Persistent>(tx: &mut Sytx, name: &str, k: &T, id: &Ref<P>) -> SRes<()> {
    let persy = &tx.structsy().structsy_impl.persy;
    persy.remove::<T, PersyId>(&mut tx.tx().trans, name, k.clone(), Some(id.raw_id.clone()))?;
    Ok(())
}

fn map_unique_entry<P: Persistent>(db: &Structsy, entry: Value<PersyId>) -> Option<(Ref<P>, P)> {
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

fn map_entry<P: Persistent>(db: &Structsy, entry: Value<PersyId>) -> Vec<(Ref<P>, P)> {
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

fn map_unique_entry_tx<P: Persistent>(
    persy: &Persy,
    tx: &mut Transaction,
    entry: Value<PersyId>,
) -> Option<(Ref<P>, P)> {
    let name = P::get_description().name;
    if let Some(id) = entry.into_iter().next() {
        if let Ok(val) = tx_read::<P>(&persy, &name, tx, &id) {
            let r = Ref::new(id);
            val.map(|c| (r, c))
        } else {
            None
        }
    } else {
        None
    }
}

fn map_entry_tx<P: Persistent>(persy: &Persy, tx: &mut Transaction, entry: Value<PersyId>) -> Vec<(Ref<P>, P)> {
    let name = P::get_description().name;
    entry
        .into_iter()
        .filter_map(|id| {
            if let Ok(val) = tx_read::<P>(&persy, &name, tx, &id) {
                let r = Ref::new(id);
                val.map(|x| (r, x))
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
) -> SRes<impl Iterator<Item = (Ref<P>, P, K)>> {
    let db1: Structsy = db.clone();
    Ok(db
        .tsdb_impl
        .persy
        .range::<K, PersyId, R>(name, range)?
        .filter_map(move |e| {
            let k = e.0;
            map_unique_entry(&db1, e.1).map(|(r, v)| (r, v, k))
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
) -> SRes<impl Iterator<Item = (Ref<P>, P, K)>> {
    let db1: Structsy = db.clone();
    Ok(db
        .tsdb_impl
        .persy
        .range::<K, PersyId, R>(name, range)?
        .map(move |e| {
            map_entry(&db1, e.1.clone())
                .into_iter()
                .map(move |(id, val)| (id, val, e.0.clone()))
        })
        .flatten())
}

pub fn find_unique_tx<K: IndexType, P: Persistent>(db: &mut Sytx, name: &str, k: &K) -> SRes<Option<(Ref<P>, P)>> {
    let persy = &db.structsy().structsy_impl.persy;
    if let Some(id_container) = persy.get_tx::<K, PersyId>(&mut db.tx().trans, name, k)? {
        Ok(map_unique_entry_tx(persy, &mut db.tx().trans, id_container))
    } else {
        Ok(None)
    }
}

pub fn find_tx<K: IndexType, P: Persistent>(db: &mut Sytx, name: &str, k: &K) -> SRes<Vec<(Ref<P>, P)>> {
    let persy = &db.structsy().structsy_impl.persy;
    if let Some(e) = persy.get_tx::<K, PersyId>(&mut db.tx().trans, name, k)? {
        Ok(map_entry_tx(persy, &mut db.tx().trans, e))
    } else {
        Ok(Vec::new())
    }
}

pub struct RangeIterator<'a, K: IndexType, P: Persistent> {
    structsy: Arc<StructsyImpl>,
    persy_iter: persy::TxIndexIter<'a, K, PersyId>,
    iter: Option<IntoIter<(Ref<P>, P, K)>>,
}
impl<'a, K: IndexType, P: Persistent> RangeIterator<'a, K, P> {
    fn new(structsy: Arc<StructsyImpl>, iter: persy::TxIndexIter<'a, K, PersyId>) -> RangeIterator<'a, K, P> {
        RangeIterator {
            structsy: structsy,
            persy_iter: iter,
            iter: None,
        }
    }
    pub fn tx(&'a mut self) -> RefSytx<'a> {
        RefSytx {
            tsdb_impl: self.structsy.clone(),
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
                let name = P::get_description().name;
                let mut pv = Vec::new();
                for id in v {
                    let tx = self.persy_iter.tx();
                    if let Ok(Some(val)) = tx_read::<P>(&self.structsy.persy, &name, tx, &id) {
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

pub struct UniqueRangeIterator<'a, K: IndexType, P: Persistent> {
    structsy: Arc<StructsyImpl>,
    persy_iter: persy::TxIndexIter<'a, K, PersyId>,
    phantom: PhantomData<P>,
}

impl<'a, K: IndexType, P: Persistent> UniqueRangeIterator<'a, K, P> {
    fn new(structsy: Arc<StructsyImpl>, iter: persy::TxIndexIter<'a, K, PersyId>) -> UniqueRangeIterator<'a, K, P> {
        UniqueRangeIterator {
            structsy: structsy,
            persy_iter: iter,
            phantom: PhantomData,
        }
    }
    pub fn tx(&'a mut self) -> RefSytx<'a> {
        RefSytx {
            tsdb_impl: self.structsy.clone(),
            trans: self.persy_iter.tx(),
        }
    }
}

impl<'a, P: Persistent, K: IndexType> Iterator for UniqueRangeIterator<'a, K, P> {
    type Item = (Ref<P>, P, K);
    fn next(&mut self) -> Option<Self::Item> {
        let name = P::get_description().name;
        if let Some((k, v)) = self.persy_iter.next() {
            if let Some(id) = v.into_iter().next() {
                let tx = self.persy_iter.tx();
                if let Ok(Some(val)) = tx_read::<P>(&self.structsy.persy.clone(), &name, tx, &id) {
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
    db: &'a mut Sytx,
    name: &str,
    r: R,
) -> SRes<UniqueRangeIterator<'a, K, P>> {
    let p = &db.structsy().structsy_impl.persy;
    let p1 = db.structsy().structsy_impl.clone();
    let iter = p.range_tx::<K, PersyId, R>(db.tx().trans, &name, r)?;
    Ok(UniqueRangeIterator::new(p1, iter))
}

pub fn find_range_tx<'a, K: IndexType, P: Persistent, R: RangeBounds<K>>(
    db: &'a mut Sytx,
    name: &str,
    r: R,
) -> SRes<RangeIterator<'a, K, P>> {
    let p = &db.structsy().structsy_impl.persy;
    let p1 = db.structsy().structsy_impl.clone();
    let iter = p.range_tx::<K, PersyId, R>(db.tx().trans, &name, r)?;
    Ok(RangeIterator::new(p1, iter))
}
