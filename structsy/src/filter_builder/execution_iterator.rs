use crate::{
    filter_builder::{
        filter_builder::{BufferedExection, Conditions, Item, Iter},
        reader::Reader,
    },
    Persistent, Ref, Structsy,
};

pub(crate) struct ExecutionIterator<'a, P> {
    base: Iter<'a, P>,
    conditions: Conditions<P>,
    structsy: Structsy,
    buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
}
impl<'a, P: 'static> ExecutionIterator<'a, P> {
    pub(crate) fn new_raw(
        base: Iter<'a, P>,
        conditions: Conditions<P>,
        structsy: Structsy,
        buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
    ) -> Self {
        ExecutionIterator {
            base,
            conditions,
            structsy,
            buffered,
        }
    }
    pub(crate) fn new(
        base: Box<dyn Iterator<Item = (Ref<P>, P)>>,
        conditions: Conditions<P>,
        structsy: Structsy,
        buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
    ) -> Self {
        ExecutionIterator {
            base: Iter::Iter(base),
            conditions,
            structsy,
            buffered,
        }
    }
}

impl<'a, P: Persistent + 'static> ExecutionIterator<'a, P> {
    pub(crate) fn filtered_next(
        base: &mut Iter<P>,
        conditions: &mut Conditions<P>,
        structsy: &Structsy,
    ) -> Option<Item<P>> {
        while let Some(read) = match base {
            Iter::Iter(ref mut it) => it.next(),
            Iter::SnapshotIter(ref mut it) => it.next(),
            Iter::TxIter(ref mut it) => it.next(),
        } {
            let mut reader = match base {
                Iter::Iter(_) => Reader::Structsy(structsy.clone()),
                Iter::SnapshotIter(it) => Reader::Snapshot(it.snapshot().clone()),
                Iter::TxIter(ref mut it) => Reader::Tx(it.tx()),
            };
            let item = Item::new(read);
            if conditions.check(&item, &mut reader) {
                return Some(item);
            }
        }

        None
    }

    pub(crate) fn buffered_next(&mut self) -> Option<Item<P>> {
        let mut source = (&mut self.base, &mut self.conditions, &self.structsy);
        if let Some(buffered) = &mut self.buffered {
            buffered.next(&mut source)
        } else {
            ExecutionIterator::filtered_next(&mut self.base, &mut self.conditions, &self.structsy)
        }
    }
}

impl<'a, P: Persistent + 'static> Iterator for ExecutionIterator<'a, P> {
    type Item = (Ref<P>, P);
    fn next(&mut self) -> Option<Self::Item> {
        self.buffered_next().map(|i| (i.id, i.record))
    }
}
