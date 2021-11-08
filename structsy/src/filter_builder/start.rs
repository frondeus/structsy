use crate::{
    filter_builder::{
        execution_iterator::ExecutionIterator,
        filter_builder::{Conditions, Iter, Orders},
        reader::{Reader, ReaderIterator},
    },
    structsy::StructsyImpl,
    Persistent, Ref, RefSytx, Snapshot, Structsy,
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
}
