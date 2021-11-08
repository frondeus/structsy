use crate::{
    filter_builder::{
        execution_iterator::ExecutionIterator,
        filter_builder::{Conditions, Iter, Orders},
        reader::{Reader, ReaderIterator},
    },
    structsy::StructsyImpl,
    OwnedSytx, Persistent, Ref, RefSytx, Snapshot, Structsy, StructsyTx,
};
use persy::Transaction;
use std::sync::Arc;

enum Holder<'a> {
    Structsy(Structsy),
    Snapshot(Snapshot),
    Tx((Arc<StructsyImpl>, &'a mut Transaction)),
}
impl<'a> Holder<'a> {
    fn new(reader: Reader<'a>) -> Self {
        match reader {
            Reader::Structsy(st) => Self::Structsy(st),
            Reader::Snapshot(sn) => Self::Snapshot(sn),
            Reader::Tx(RefSytx { structsy_impl, trans }) => Self::Tx((structsy_impl, trans)),
        }
    }
    fn reader<'b>(&'b mut self) -> Reader<'b> {
        match self {
            Self::Structsy(st) => Reader::Structsy(st.clone()),
            Self::Snapshot(st) => Reader::Snapshot(st.clone()),
            Self::Tx((st, tx)) => Reader::Tx(RefSytx {
                structsy_impl: st.clone(),
                trans: tx,
            }),
        }
    }
}

struct HolderIter<'a, T> {
    iter: Box<dyn Iterator<Item = (Ref<T>, T)>>,
    h: Holder<'a>,
}
impl<'a, T> HolderIter<'a, T> {
    fn new(iter: Box<dyn Iterator<Item = (Ref<T>, T)>>, reader: Reader<'a>) -> Self {
        HolderIter {
            iter,
            h: Holder::new(reader),
        }
    }
}
impl<'a, T> Iterator for HolderIter<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, T> ReaderIterator for HolderIter<'a, T> {
    fn reader<'b>(&'b mut self) -> Reader<'b> {
        self.h.reader()
    }
}

struct EmptyIter<T> {
    mark: std::marker::PhantomData<T>,
    structsy: Structsy,
}
impl<T> EmptyIter<T> {
    fn new(structsy: Structsy) -> Self {
        Self {
            mark: std::marker::PhantomData,
            structsy,
        }
    }
}
impl<T> Iterator for EmptyIter<T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
impl<T> DoubleEndedIterator for EmptyIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        None
    }
}
impl<T> ReaderIterator for EmptyIter<T> {
    fn reader<'b>(&'b mut self) -> Reader<'b> {
        Reader::Structsy(self.structsy.clone())
    }
}

pub(crate) trait StartStep<'a, T> {
    fn start_reader(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        reader: Reader<'a>,
    ) -> ExecutionIterator<'a, T>;
    fn start(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        structsy: Structsy,
    ) -> ExecutionIterator<'static, T>;
    fn start_tx(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        tx: &'a mut OwnedSytx,
    ) -> ExecutionIterator<T>;
    fn start_snapshot(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        snapshot: &Snapshot,
    ) -> ExecutionIterator<'static, T>;
}

pub(crate) struct ScanStartStep {}
impl ScanStartStep {
    pub(crate) fn new() -> Self {
        ScanStartStep {}
    }
}
impl<'a, T: Persistent + 'static> StartStep<'a, T> for ScanStartStep {
    fn start_reader(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        reader: Reader<'a>,
    ) -> ExecutionIterator<'a, T> {
        let st = reader.structsy();
        if order.index_order() {
            let (buffered, iter) = order.scan(reader);
            ExecutionIterator::new_raw(iter.unwrap(), conditions, buffered)
        } else if let Ok(found) = reader.scan::<T>() {
            ExecutionIterator::new_raw(Iter::IterR(Box::new(found)), conditions, order.buffered())
        } else {
            ExecutionIterator::new_raw(
                Iter::IterR(Box::new(EmptyIter::new(st.clone()))),
                conditions,
                order.buffered(),
            )
        }
    }
    fn start(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        structsy: Structsy,
    ) -> ExecutionIterator<'static, T> {
        let (buffered, iter) = order.scan(Reader::Structsy(structsy.clone()));
        if let Some(it) = iter {
            ExecutionIterator::new_raw(it, conditions, buffered)
        } else if let Ok(found) = structsy.scan::<T>() {
            ExecutionIterator::new(Box::new(found), conditions, structsy, buffered)
        } else {
            ExecutionIterator::new_raw(
                Iter::Iter((Box::new(EmptyIter::new(structsy.clone())), structsy)),
                conditions,
                buffered,
            )
        }
    }
    fn start_tx(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        tx: &'a mut OwnedSytx,
    ) -> ExecutionIterator<'a, T> {
        let structsy = Structsy {
            structsy_impl: tx.structsy_impl.clone(),
        };
        if order.index_order() {
            let (buffered, iter) = order.scan(Reader::Tx(tx.reference()));
            ExecutionIterator::new_raw(iter.unwrap(), conditions, buffered)
        } else if let Ok(found) = tx.scan::<T>() {
            ExecutionIterator::new_raw(Iter::TxIter(Box::new(found)), conditions, order.buffered())
        } else {
            ExecutionIterator::new_raw(
                Iter::Iter((Box::new(EmptyIter::new(structsy.clone())), structsy)),
                conditions,
                order.buffered(),
            )
        }
    }
    fn start_snapshot(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        snapshot: &Snapshot,
    ) -> ExecutionIterator<'static, T> {
        let structsy = snapshot.structsy();
        let (buffered, iter) = order.scan(Reader::Snapshot(snapshot.clone()));
        if let Some(it) = iter {
            ExecutionIterator::new_raw(it, conditions, buffered)
        } else if let Ok(found) = snapshot.scan::<T>() {
            ExecutionIterator::new_raw(Iter::SnapshotIter(Box::new(found)), conditions, buffered)
        } else {
            ExecutionIterator::new_raw(
                Iter::Iter((Box::new(EmptyIter::new(structsy.clone())), structsy)),
                conditions,
                buffered,
            )
        }
    }
}

pub(crate) struct DataStartStep<T> {
    data: Box<dyn Iterator<Item = (Ref<T>, T)>>,
}
impl<'a, T> DataStartStep<T> {
    pub(crate) fn new(data: Box<dyn Iterator<Item = (Ref<T>, T)>>) -> Self {
        Self { data }
    }
}
impl<'a, T: 'static> StartStep<'a, T> for DataStartStep<T> {
    fn start_reader(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        reader: Reader<'a>,
    ) -> ExecutionIterator<'a, T> {
        ExecutionIterator::new_raw(
            Iter::IterR(Box::new(HolderIter::new(self.data, reader))),
            conditions,
            order.buffered(),
        )
    }
    fn start(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        structsy: Structsy,
    ) -> ExecutionIterator<'static, T> {
        ExecutionIterator::new(Box::new(self.data), conditions, structsy, order.buffered())
    }
    fn start_tx(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        tx: &'a mut OwnedSytx,
    ) -> ExecutionIterator<T> {
        let structsy = Structsy {
            structsy_impl: tx.structsy_impl.clone(),
        };
        ExecutionIterator::new(Box::new(self.data), conditions, structsy, order.buffered())
    }
    fn start_snapshot(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        snapshot: &Snapshot,
    ) -> ExecutionIterator<'static, T> {
        ExecutionIterator::new(Box::new(self.data), conditions, snapshot.structsy(), order.buffered())
    }
}
