use crate::{
    filter_builder::{
        filter_builder::{BufferedExection, Conditions, Item, Iter},
        reader::Reader,
    },
    Persistent, Ref,
};

pub(crate) struct ExecutionIterator<'a, P> {
    base: Iter<'a, P>,
    conditions: Conditions<P>,
    buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
}
impl<'a, P: 'static> ExecutionIterator<'a, P> {
    pub(crate) fn new_raw(
        base: Iter<'a, P>,
        conditions: Conditions<P>,
        buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
    ) -> Self {
        ExecutionIterator {
            base,
            conditions,
            buffered,
        }
    }
}

impl<'a, P: Persistent + 'static> ExecutionIterator<'a, P> {
    pub(crate) fn filtered_next(base: &mut Iter<P>, conditions: &mut Conditions<P>) -> Option<Item<P>> {
        while let Some(read) = match base {
            Iter::Iter((ref mut it, _)) => it.next(),
            Iter::SnapshotIter(ref mut it) => it.next(),
            Iter::TxIter(ref mut it) => it.next(),
            Iter::IterR(ref mut it) => it.next(),
        } {
            let mut reader = match base {
                Iter::Iter((_, structsy)) => Reader::Structsy(structsy.clone()),
                Iter::SnapshotIter(it) => Reader::Snapshot(it.snapshot().clone()),
                Iter::TxIter(ref mut it) => Reader::Tx(it.tx()),
                Iter::IterR(ref mut it) => it.reader(),
            };
            let item = Item::new(read);
            if conditions.check(&item, &mut reader) {
                return Some(item);
            }
        }

        None
    }

    pub(crate) fn buffered_next(&mut self) -> Option<Item<P>> {
        let mut source = (&mut self.base, &mut self.conditions);
        if let Some(buffered) = &mut self.buffered {
            buffered.next(&mut source)
        } else {
            ExecutionIterator::filtered_next(&mut self.base, &mut self.conditions)
        }
    }
}

impl<'a, P: Persistent + 'static> Iterator for ExecutionIterator<'a, P> {
    type Item = (Ref<P>, P);
    fn next(&mut self) -> Option<Self::Item> {
        self.buffered_next().map(|i| (i.id, i.record))
    }
}
