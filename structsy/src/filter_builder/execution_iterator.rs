use crate::{
    filter_builder::{
        filter_builder::{BufferedExection, Conditions, Item},
        reader::ReaderIterator,
    },
    Persistent, Ref,
};

pub(crate) type IterT<'a, P> = Box<dyn ReaderIterator<Item = (Ref<P>, P)> + 'a>;
pub(crate) trait Source<T> {
    fn next_item(&mut self) -> Option<Item<T>>;
}

impl<'a, T: Persistent + 'static> Source<T> for (&mut IterT<'a, T>, &mut Conditions<T>) {
    fn next_item(&mut self) -> Option<Item<T>> {
        ExecutionIterator::filtered_next(self.0, self.1)
    }
}
pub(crate) struct ExecutionIterator<'a, P> {
    base: IterT<'a, P>,
    conditions: Conditions<P>,
    buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
}
impl<'a, P: 'static> ExecutionIterator<'a, P> {
    pub(crate) fn new(
        iter: IterT<'a, P>,
        conditions: Conditions<P>,
        buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
    ) -> Self {
        ExecutionIterator {
            base: iter,
            conditions,
            buffered,
        }
    }
}

impl<'a, P: Persistent + 'static> ExecutionIterator<'a, P> {
    pub(crate) fn filtered_next(base: &mut IterT<'a, P>, conditions: &mut Conditions<P>) -> Option<Item<P>> {
        while let Some(read) = base.next() {
            let mut reader = base.reader();
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
