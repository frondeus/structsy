use crate::{
    embedded_filter::{EIter, EmbeddedFilterBuilder},
    FilterBuilder, OwnedSytx, Persistent, PersistentEmbedded, Ref, Structsy,
};

pub struct StructsyIter<'a, T: Persistent> {
    iterator: Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>,
}

impl<'a, T: Persistent> StructsyIter<'a, T> {
    pub fn new<I>(iterator: I) -> StructsyIter<'a, T>
    where
        I: Iterator<Item = (Ref<T>, T)>,
        I: 'a,
    {
        StructsyIter {
            iterator: Box::new(iterator),
        }
    }
}

impl<'a, T: Persistent> Iterator for StructsyIter<'a, T> {
    type Item = (Ref<T>, T);

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}

pub struct EmbeddedFilter<T: PersistentEmbedded> {
    pub(crate) builder: EmbeddedFilterBuilder<T>,
}

impl<T: PersistentEmbedded + 'static> EmbeddedFilter<T> {
    pub fn new() -> EmbeddedFilter<T> {
        EmbeddedFilter {
            builder: EmbeddedFilterBuilder::new(),
        }
    }
    pub fn filter_builder(&mut self) -> &mut EmbeddedFilterBuilder<T> {
        &mut self.builder
    }
    pub(crate) fn filter<'a>(self, i: EIter<'a, T>) -> EIter<'a, T> {
        self.builder.filter(i)
    }
}

pub trait Query<T: Persistent> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T>;
}

pub struct StructsyQuery<T: Persistent + 'static> {
    pub(crate) structsy: Structsy,
    pub(crate) builder: FilterBuilder<T>,
}

impl<T: Persistent + 'static> Query<T> for StructsyQuery<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.builder
    }
}
impl<T: Persistent + 'static> StructsyQuery<T> {
    pub(crate) fn builder(self) -> FilterBuilder<T> {
        self.builder
    }
}

impl<T: Persistent> IntoIterator for StructsyQuery<T> {
    type Item = (Ref<T>, T);
    type IntoIter = StructsyIter<'static, T>;
    fn into_iter(self) -> Self::IntoIter {
        StructsyIter::new(self.builder.finish(&self.structsy))
    }
}

pub struct StructsyQueryTx<'a, T: Persistent + 'static> {
    pub(crate) tx: &'a mut OwnedSytx,
    pub(crate) builder: FilterBuilder<T>,
}

impl<'a, T: Persistent + 'static> Query<T> for StructsyQueryTx<'a, T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.builder
    }
}

impl<'a, T: Persistent> IntoIterator for StructsyQueryTx<'a, T> {
    type Item = (Ref<T>, T);
    type IntoIter = StructsyIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        StructsyIter::new(self.builder.finish_tx(self.tx))
    }
}
