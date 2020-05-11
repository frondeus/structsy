use crate::{
    embedded_filter::{EIter, EmbeddedFilterBuilder},
    FilterBuilder, OwnedSytx, Persistent, PersistentEmbedded, Ref, Structsy,
};
/// Iterator for query results
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

/// Filter for an embedded structure
///
/// # Example
/// ```
/// use structsy::{ Structsy, StructsyTx, StructsyError, EmbeddedFilter};
/// use structsy_derive::{queries, embedded_queries, Persistent, PersistentEmbedded};
///
/// #[derive(Persistent)]
/// struct WithEmbedded {
///     embedded: Embedded,
/// }
///
/// #[derive(PersistentEmbedded)]
/// struct Embedded {
///     name: String,
/// }
/// impl WithEmbedded {
///     fn new(name: &str) -> WithEmbedded {
///         WithEmbedded {
///             embedded: Embedded { name: name.to_string() },
///         }
///     }
/// }
///
/// #[queries(WithEmbedded)]
/// trait WithEmbeddedQuery {
///     fn embedded(self, embedded: EmbeddedFilter<Embedded>) -> Self;
/// }
///
/// #[embedded_queries(Embedded)]
/// trait EmbeddedQuery {
///     fn by_name(self, name: String) -> Self;
/// }
///
/// fn embedded_query() -> Result<(), StructsyError> {
///     let structsy = Structsy::open("file.structsy")?;
///     structsy.define::<WithEmbedded>()?;
///     let mut tx = structsy.begin()?;
///     tx.insert(&WithEmbedded::new("aaa"))?;
///     tx.commit()?;
///     let embedded_filter = Structsy::embedded_filter::<Embedded>().by_name("aaa".to_string());
///     let count = structsy.query::<WithEmbedded>().embedded(embedded_filter).into_iter().count();
///     assert_eq!(count, 1);
///     Ok(())
/// }
/// ```
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

/// Base trait for all the query types
pub trait Query<T: Persistent + 'static>: Sized {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T>;

    fn or(mut self, builder: fn(StructsyOrFilter<T>) -> StructsyOrFilter<T>) -> Self {
        self.filter_builder().or(builder(StructsyOrFilter::<T>::new()));
        self
    }
    fn and(mut self, builder: fn(StructsyAndFilter<T>) -> StructsyAndFilter<T>) -> Self {
        self.filter_builder().and(builder(StructsyAndFilter::<T>::new()));
        self
    }
    fn not(mut self, builder: fn(StructsyNotFilter<T>) -> StructsyNotFilter<T>) -> Self {
        self.filter_builder().not(builder(StructsyNotFilter::<T>::new()));
        self
    }
}
/// Query for a persistent struct
///
/// # Example
/// ```
/// use structsy::{ Structsy, StructsyTx, StructsyError};
/// use structsy_derive::{queries, Persistent};
/// #[derive(Persistent)]
/// struct Basic {
///     name: String,
/// }
/// impl Basic {
///     fn new(name: &str) -> Basic {
///         Basic { name: name.to_string() }
///     }
/// }
///
/// #[queries(Basic)]
/// trait BasicQuery {
///      fn by_name(self, name: String) -> Self;
/// }
///
///
/// fn basic_query() -> Result<(), StructsyError> {
///     let structsy = Structsy::open("file.structsy")?;
///     structsy.define::<Basic>()?;
///     let mut tx = structsy.begin()?;
///     tx.insert(&Basic::new("aaa"))?;
///     tx.commit()?;
///     let count = structsy.query::<Basic>().by_name("aaa".to_string()).into_iter().count();
///     assert_eq!(count, 1);
///     Ok(())
/// }
/// ```
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

/// Query for a persistent struct considering in transaction changes.
///
/// # Example
/// ```
/// use structsy::{ Structsy, StructsyTx, StructsyError};
/// use structsy_derive::{queries, Persistent};
/// #[derive(Persistent)]
/// struct Basic {
///     name: String,
/// }
/// impl Basic {
///     fn new(name: &str) -> Basic {
///         Basic { name: name.to_string() }
///     }
/// }
///
/// #[queries(Basic)]
/// trait BasicQuery {
///     fn by_name(self, name: String) -> Self;
/// }
///
///
/// fn basic_query() -> Result<(), StructsyError> {
///     let structsy = Structsy::open("file.structsy")?;
///     structsy.define::<Basic>()?;
///     let mut tx = structsy.begin()?;
///     tx.insert(&Basic::new("aaa"))?;
///     let count = tx.query::<Basic>().by_name("aaa".to_string()).into_iter().count();
///     assert_eq!(count, 1);
///     tx.commit()?;
///     Ok(())
/// }
/// ```
///
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

pub struct StructsyOrFilter<T: Persistent> {
    pub(crate) builder: FilterBuilder<T>,
}

impl<T: Persistent + 'static> StructsyOrFilter<T> {
    pub fn new() -> StructsyOrFilter<T> {
        StructsyOrFilter {
            builder: FilterBuilder::new(),
        }
    }
}

impl<T: Persistent + 'static> Query<T> for StructsyOrFilter<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.builder
    }
}

pub struct StructsyAndFilter<T: Persistent> {
    pub(crate) builder: FilterBuilder<T>,
}

impl<T: Persistent + 'static> StructsyAndFilter<T> {
    pub fn new() -> StructsyAndFilter<T> {
        StructsyAndFilter {
            builder: FilterBuilder::new(),
        }
    }
}

impl<T: Persistent + 'static> Query<T> for StructsyAndFilter<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.builder
    }
}

pub struct StructsyNotFilter<T: Persistent> {
    pub(crate) builder: FilterBuilder<T>,
}

impl<T: Persistent + 'static> StructsyNotFilter<T> {
    pub fn new() -> StructsyNotFilter<T> {
        StructsyNotFilter {
            builder: FilterBuilder::new(),
        }
    }
}
impl<T: Persistent + 'static> Query<T> for StructsyNotFilter<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.builder
    }
}
