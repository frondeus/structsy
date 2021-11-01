use crate::{
    filter_builder::{
        execution_iterator::ExecutionIterator,
        filter_builder::{Conditions, Iter, Orders},
    },
    OwnedSytx, Persistent, Ref, Snapshot, Structsy, StructsyTx,
};

pub(crate) trait StartStep<'a, T> {
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
    fn start(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        structsy: Structsy,
    ) -> ExecutionIterator<'static, T> {
        let (buffered, iter) = order.scan(structsy.clone());
        if let Some(it) = iter {
            ExecutionIterator::new_raw(it, conditions, structsy, buffered)
        } else if let Ok(found) = structsy.scan::<T>() {
            ExecutionIterator::new(Box::new(found), conditions, structsy, buffered)
        } else {
            ExecutionIterator::new(Box::new(Vec::new().into_iter()), conditions, structsy, buffered)
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
            let (buffered, iter) = order.scan_tx(tx);
            ExecutionIterator::new_raw(iter.unwrap(), conditions, structsy, buffered)
        } else if let Ok(found) = tx.scan::<T>() {
            ExecutionIterator::new_raw(Iter::TxIter(Box::new(found)), conditions, structsy, order.buffered())
        } else {
            ExecutionIterator::new(Box::new(Vec::new().into_iter()), conditions, structsy, order.buffered())
        }
    }
    fn start_snapshot(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        snapshot: &Snapshot,
    ) -> ExecutionIterator<'static, T> {
        let (buffered, iter) = order.scan_snapshot(snapshot);
        if let Some(it) = iter {
            ExecutionIterator::new_raw(it, conditions, snapshot.structsy(), buffered)
        } else if let Ok(found) = snapshot.scan::<T>() {
            ExecutionIterator::new(Box::new(found), conditions, snapshot.structsy(), buffered)
        } else {
            ExecutionIterator::new(
                Box::new(Vec::new().into_iter()),
                conditions,
                snapshot.structsy(),
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
