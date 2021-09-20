use crate::Snapshot;
#[allow(deprecated)]
use crate::{
    embedded_filter::{EmbeddedFilterBuilder, EmbeddedRangeCondition, SimpleEmbeddedCondition},
    filter::{RangeCondition, SimpleCondition},
    internal::{EmbeddedDescription, Field, FilterDefinition, Projection},
    Fetch, FilterBuilder, IntoResult, Order, OwnedSytx, Persistent, PersistentEmbedded, Ref, Structsy,
};
use std::ops::RangeBounds;
/// Iterator for query results
pub struct StructsyIter<'a, T> {
    iterator: Box<dyn Iterator<Item = T> + 'a>,
}

impl<'a, T> StructsyIter<'a, T> {
    pub fn new<I>(iterator: I) -> StructsyIter<'a, T>
    where
        I: Iterator<Item = T>,
        I: 'a,
    {
        StructsyIter {
            iterator: Box::new(iterator),
        }
    }
}

impl<'a, T> Iterator for StructsyIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}

/// And/Or/Not Operators
/// # Example
/// ```
/// use structsy::{ Structsy, StructsyTx, StructsyError, Operators};
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
///     tx.insert(&Basic::new("bbb"))?;
///     tx.commit()?;
///     let count = structsy.query::<Basic>().or(|or| {
///             or.by_name("aaa".to_string()).by_name("bbb".to_string())
///         }).fetch().count();
///     assert_eq!(count, 2);
///     let count = structsy.query::<Basic>().not(|not| {
///             not.by_name("aaa".to_string())
///         }).fetch().count();
///     assert_eq!(count, 1);
///     let count = structsy.query::<Basic>().and(|and| {
///             and.by_name("aaa".to_string()).by_name("bbb".to_string())
///         }).fetch().count();
///     assert_eq!(count, 0);
///     Ok(())
/// }
/// ```
pub trait Operators<F> {
    fn or<FN: Fn(F) -> F>(self, builder: FN) -> Self;
    fn and<FN: Fn(F) -> F>(self, builder: FN) -> Self;
    fn not<FN: Fn(F) -> F>(self, builder: FN) -> Self;
}

pub trait EmbeddedQuery<T: PersistentEmbedded + FilterDefinition + 'static>: Sized {
    fn filter_builder(&mut self) -> &mut EmbeddedFilterBuilder<T>;
    fn add_group(&mut self, filter: Filter<T>);
}

impl<T: EmbeddedDescription + FilterDefinition + 'static> EmbeddedQuery<T> for Filter<T> {
    fn filter_builder(&mut self) -> &mut EmbeddedFilterBuilder<T> {
        &mut self.filter_builder
    }
    fn add_group(&mut self, filter: Filter<T>) {
        let base = self.filter_builder();
        base.and(filter.extract_filter());
    }
}

impl<T: Persistent + 'static, Q: Query<T>> Operators<StructsyFilter<T>> for Q {
    fn or<FN: Fn(StructsyFilter<T>) -> StructsyFilter<T>>(mut self, builder: FN) -> Self {
        self.filter_builder().or(builder(StructsyFilter::<T>::new()));
        self
    }
    fn and<FN: Fn(StructsyFilter<T>) -> StructsyFilter<T>>(mut self, builder: FN) -> Self {
        self.filter_builder().and(builder(StructsyFilter::<T>::new()));
        self
    }
    fn not<FN: Fn(StructsyFilter<T>) -> StructsyFilter<T>>(mut self, builder: FN) -> Self {
        self.filter_builder().not(builder(StructsyFilter::<T>::new()));
        self
    }
}

impl<T: EmbeddedDescription + FilterDefinition + 'static, Q: EmbeddedQuery<T>> Operators<Filter<T>> for Q {
    fn or<FN: Fn(Filter<T>) -> Filter<T>>(mut self, builder: FN) -> Self {
        self.filter_builder().or(builder(Filter::<T>::new()).extract_filter());
        self
    }
    fn and<FN: Fn(Filter<T>) -> Filter<T>>(mut self, builder: FN) -> Self {
        self.filter_builder().and(builder(Filter::<T>::new()).extract_filter());
        self
    }
    fn not<FN: Fn(Filter<T>) -> Filter<T>>(mut self, builder: FN) -> Self {
        self.filter_builder().not(builder(Filter::<T>::new()).extract_filter());
        self
    }
}

pub struct ProjectionResult<P: Projection<T>, T: FilterDefinition> {
    filter: FilterBuilder<T>,
    phantom: std::marker::PhantomData<P>,
}

impl<P: Projection<T>, T: Persistent + 'static> Fetch<P> for ProjectionResult<P, T> {
    fn into(self, structsy: &Structsy) -> StructsyIter<P> {
        self.fetch(structsy)
    }

    fn into_tx(self, tx: &mut OwnedSytx) -> StructsyIter<P> {
        self.fetch_tx(tx)
    }

    fn fetch(self, structsy: &Structsy) -> StructsyIter<P> {
        let data = self.filter.finish(&structsy);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
    }

    fn fetch_tx(self, tx: &mut OwnedSytx) -> StructsyIter<P> {
        let data = self.filter.finish_tx(tx);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
    }

    fn fetch_snapshot(self, snapshot: &Snapshot) -> StructsyIter<P> {
        let data = self.filter.finish_snap(&snapshot);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
    }
}

#[allow(deprecated)]
impl<P: Projection<T>, T: Persistent + 'static> IntoResult<P> for ProjectionResult<P, T> {}

/// Generic filter for any Persistent structures
///
///
/// # Example
/// ```rust
/// use structsy::{ Structsy, StructsyTx, StructsyError, Filter};
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
///     fn embedded(self, embedded: Filter<Embedded>) -> Self;
/// }
///
/// #[embedded_queries(Embedded)]
/// trait EmbeddedQuery {
///     fn by_name(self, name: String) -> Self;
/// }
///
/// fn main() -> Result<(), StructsyError> {
///     let structsy = Structsy::memory()?;
///     structsy.define::<WithEmbedded>()?;
///     let mut tx = structsy.begin()?;
///     tx.insert(&WithEmbedded::new("aaa"))?;
///     tx.commit()?;
///     let embedded_filter = Filter::<Embedded>::new().by_name("aaa".to_string());
///     let query = Filter::<WithEmbedded>::new().embedded(embedded_filter);
///     assert_eq!(structsy.fetch(query).count(), 1);
///     Ok(())
/// }
/// ```
pub struct Filter<T: FilterDefinition> {
    filter_builder: T::Filter,
}

impl<T: FilterDefinition> Filter<T> {
    pub fn new() -> Self {
        Filter {
            filter_builder: T::Filter::default(),
        }
    }
    pub(crate) fn extract_filter(self) -> T::Filter {
        self.filter_builder
    }
}

impl<T: Persistent> Filter<T> {
    /// Make a projection from filtered structs.
    ///
    ///
    /// # Example
    /// ```rust
    /// use structsy::{ Structsy, StructsyTx, StructsyError, Filter};
    /// use structsy_derive::{queries, Projection, Persistent};
    ///
    /// #[derive(Persistent)]
    /// struct Person {
    ///     name:String,
    ///     surname:String,
    /// }
    ///
    /// impl Person {
    ///     fn new(name:&str, surname:&str) -> Self {
    ///         Person {
    ///             name: name.to_string(),
    ///             surname: surname.to_string(),
    ///         }
    ///     }
    /// }
    ///
    /// #[queries(Person)]
    /// trait PersonQuery {
    ///     fn by_name(self, name:&str) -> Self;
    /// }
    ///
    /// #[derive(Projection)]
    /// #[projection = "Person" ]
    /// struct NameProjection {
    ///     name:String,
    /// }
    ///
    ///
    /// fn main() -> Result<(), StructsyError> {
    ///     let structsy = Structsy::memory()?;
    ///     structsy.define::<Person>()?;
    ///     let mut tx = structsy.begin()?;
    ///     tx.insert(&Person::new("a_name", "a_surname"))?;
    ///     tx.commit()?;
    ///     let query =
    ///     Filter::<Person>::new().by_name("a_name").projection::<NameProjection>();
    ///     assert_eq!(structsy.fetch(query).next().unwrap().name, "a_name");
    ///     Ok(())
    /// }
    /// ```
    pub fn projection<P: Projection<T>>(self) -> ProjectionResult<P, T> {
        ProjectionResult {
            filter: self.filter_builder,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T: FilterDefinition> Default for Filter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Persistent + 'static> Fetch<(Ref<T>, T)> for Filter<T> {
    fn into(self, structsy: &Structsy) -> StructsyIter<(Ref<T>, T)> {
        self.fetch(structsy)
    }

    fn into_tx(self, tx: &mut OwnedSytx) -> StructsyIter<(Ref<T>, T)> {
        self.fetch_tx(tx)
    }

    fn fetch(self, structsy: &Structsy) -> StructsyIter<(Ref<T>, T)> {
        let data = self.extract_filter().finish(&structsy);
        StructsyIter::new(data)
    }

    fn fetch_tx(self, tx: &mut OwnedSytx) -> StructsyIter<(Ref<T>, T)> {
        let data = self.extract_filter().finish_tx(tx);
        StructsyIter::new(data)
    }

    fn fetch_snapshot(self, snapshot: &Snapshot) -> StructsyIter<(Ref<T>, T)> {
        let data = self.extract_filter().finish_snap(&snapshot);
        StructsyIter::new(data)
    }
}

#[allow(deprecated)]
impl<T: Persistent + 'static> IntoResult<(Ref<T>, T)> for Filter<T> {}

impl<T: Persistent + 'static> Query<T> for Filter<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.filter_builder
    }
    fn add_group(&mut self, filter: Filter<T>) {
        let base = self.filter_builder();
        base.and_filter(filter.extract_filter());
    }
}

/// Base trait for all the query types
pub trait Query<T: Persistent + 'static>: Sized {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T>;
    fn add_group(&mut self, filter: Filter<T>);
}

/// A query to be executed on a specific snapshot
pub struct SnapshotQuery<T: Persistent + 'static> {
    pub(crate) snapshot: Snapshot,
    pub(crate) builder: FilterBuilder<T>,
}

impl<T: Persistent> IntoIterator for SnapshotQuery<T> {
    type Item = (Ref<T>, T);
    type IntoIter = StructsyIter<'static, (Ref<T>, T)>;
    fn into_iter(self) -> Self::IntoIter {
        StructsyIter::new(self.builder.finish_snap(&self.snapshot))
    }
}

impl<T: Persistent + 'static> Query<T> for SnapshotQuery<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.builder
    }
    fn add_group(&mut self, filter: Filter<T>) {
        let base = self.filter_builder();
        base.and_filter(filter.extract_filter());
    }
}
impl<T: Persistent + 'static> SnapshotQuery<T> {
    pub(crate) fn builder(self) -> FilterBuilder<T> {
        self.builder
    }
    pub fn projection<P: Projection<T>>(self) -> ProjectionSnapshotQuery<P, T> {
        ProjectionSnapshotQuery {
            builder: self.builder,
            snapshot: self.snapshot,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn fetch(self) -> StructsyIter<'static, (Ref<T>, T)> {
        StructsyIter::new(self.builder.finish_snap(&self.snapshot))
    }
}

pub struct ProjectionSnapshotQuery<P: Projection<T>, T: FilterDefinition> {
    builder: FilterBuilder<T>,
    snapshot: Snapshot,
    phantom: std::marker::PhantomData<P>,
}

impl<P: Projection<T>, T: Persistent + 'static> ProjectionSnapshotQuery<P, T> {
    pub fn fetch(self) -> StructsyIter<'static, P> {
        let data = self.builder.finish_snap(&self.snapshot);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
    }
}

impl<P: Projection<T>, T: Persistent + 'static> IntoIterator for ProjectionSnapshotQuery<P, T> {
    type Item = P;
    type IntoIter = StructsyIter<'static, P>;
    fn into_iter(self) -> Self::IntoIter {
        let data = self.builder.finish_snap(&self.snapshot);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
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
///     let count = structsy.query::<Basic>().by_name("aaa".to_string()).fetch().count();
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
    fn add_group(&mut self, filter: Filter<T>) {
        let base = self.filter_builder();
        base.and_filter(filter.extract_filter());
    }
}
impl<T: Persistent + 'static> StructsyQuery<T> {
    pub(crate) fn builder(self) -> FilterBuilder<T> {
        self.builder
    }
    pub fn projection<P: Projection<T>>(self) -> ProjectionQuery<P, T> {
        ProjectionQuery {
            builder: self.builder,
            structsy: self.structsy,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn fetch(self) -> StructsyIter<'static, (Ref<T>, T)> {
        StructsyIter::new(self.builder.finish(&self.structsy))
    }
}

impl<T: Persistent> IntoIterator for StructsyQuery<T> {
    type Item = (Ref<T>, T);
    type IntoIter = StructsyIter<'static, (Ref<T>, T)>;
    fn into_iter(self) -> Self::IntoIter {
        StructsyIter::new(self.builder.finish(&self.structsy))
    }
}

pub struct ProjectionQuery<P: Projection<T>, T: FilterDefinition> {
    builder: FilterBuilder<T>,
    structsy: Structsy,
    phantom: std::marker::PhantomData<P>,
}

impl<P: Projection<T>, T: Persistent + 'static> ProjectionQuery<P, T> {
    pub fn fetch(self) -> StructsyIter<'static, P> {
        let data = self.builder.finish(&self.structsy);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
    }
}

impl<P: Projection<T>, T: Persistent + 'static> IntoIterator for ProjectionQuery<P, T> {
    type Item = P;
    type IntoIter = StructsyIter<'static, P>;
    fn into_iter(self) -> Self::IntoIter {
        let data = self.builder.finish(&self.structsy);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
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
///     let count = tx.query::<Basic>().by_name("aaa".to_string()).fetch().count();
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
    fn add_group(&mut self, filter: Filter<T>) {
        let base = self.filter_builder();
        base.and_filter(filter.extract_filter());
    }
}
impl<'a, T: Persistent> StructsyQueryTx<'a, T> {
    /// Make a projection from filtered structs.
    ///
    ///
    /// # Example
    /// ```rust
    /// use structsy::{ Structsy, StructsyTx, StructsyError, Filter};
    /// use structsy_derive::{queries, Projection, Persistent};
    ///
    /// #[derive(Persistent)]
    /// struct Person {
    ///     name:String,
    ///     surname:String,
    /// }
    ///
    /// impl Person {
    ///     fn new(name:&str, surname:&str) -> Self {
    ///         Person {
    ///             name: name.to_string(),
    ///             surname: surname.to_string(),
    ///         }
    ///     }
    /// }
    ///
    /// #[queries(Person)]
    /// trait PersonQuery {
    ///     fn by_name(self, name:&str) -> Self;
    /// }
    ///
    /// #[derive(Projection)]
    /// #[projection = "Person" ]
    /// struct NameProjection {
    ///     name:String,
    /// }
    ///
    ///
    /// fn main() -> Result<(), StructsyError> {
    ///     let structsy = Structsy::memory()?;
    ///     structsy.define::<Person>()?;
    ///     let mut tx = structsy.begin()?;
    ///     tx.insert(&Person::new("a_name", "a_surname"))?;
    ///     tx.commit()?;
    ///     let query = structsy.query::<Person>().by_name("a_name").projection::<NameProjection>();
    ///     assert_eq!(query.fetch().next().unwrap().name, "a_name");
    ///     Ok(())
    /// }
    /// ```
    pub fn projection<P: Projection<T>>(self) -> ProjectionQueryTx<'a, P, T> {
        ProjectionQueryTx {
            tx: self.tx,
            builder: self.builder,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn fetch(self) -> StructsyIter<'a, (Ref<T>, T)> {
        StructsyIter::new(self.builder.finish_tx(self.tx))
    }
}
pub struct ProjectionQueryTx<'a, P, T> {
    tx: &'a mut OwnedSytx,
    builder: FilterBuilder<T>,
    phantom: std::marker::PhantomData<P>,
}

impl<'a, P: Projection<T>, T: Persistent + 'static> ProjectionQueryTx<'a, P, T> {
    pub fn fetch(self) -> StructsyIter<'a, P> {
        let data = self.builder.finish_tx(self.tx);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
    }
}

impl<'a, P: Projection<T>, T: Persistent + 'static> IntoIterator for ProjectionQueryTx<'a, P, T> {
    type Item = P;
    type IntoIter = StructsyIter<'a, P>;
    fn into_iter(self) -> Self::IntoIter {
        let data = self.builder.finish_tx(self.tx);
        StructsyIter::new(Box::new(data.map(|(_, r)| Projection::projection(&r))))
    }
}

impl<'a, T: Persistent> IntoIterator for StructsyQueryTx<'a, T> {
    type Item = (Ref<T>, T);
    type IntoIter = StructsyIter<'a, (Ref<T>, T)>;
    fn into_iter(self) -> Self::IntoIter {
        StructsyIter::new(self.builder.finish_tx(self.tx))
    }
}

pub struct StructsyFilter<T: Persistent> {
    pub(crate) builder: FilterBuilder<T>,
}

impl<T: Persistent + 'static> StructsyFilter<T> {
    pub fn new() -> StructsyFilter<T> {
        StructsyFilter {
            builder: FilterBuilder::new(),
        }
    }
    pub(crate) fn filter(self) -> FilterBuilder<T> {
        self.builder
    }
}
impl<T: Persistent + 'static> Query<T> for StructsyFilter<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.builder
    }
    fn add_group(&mut self, filter: Filter<T>) {
        let base = self.filter_builder();
        base.and_filter(filter.extract_filter());
    }
}

pub trait EqualAction<X> {
    fn equal(self, value: X);
}
impl<T, V> EqualAction<V> for (Field<T, V>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: SimpleCondition<T, V> + PartialEq + Clone + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        V::equal(self.1, self.0, value);
    }
}

impl<T> EqualAction<&str> for (Field<T, String>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
{
    #[inline]
    fn equal(self, value: &str) {
        <String as SimpleCondition<T, String>>::equal(self.1, self.0, value.to_string());
    }
}

impl<T, V> EqualAction<V> for (Field<T, Vec<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: SimpleCondition<T, V> + PartialEq + Clone + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        V::contains(self.1, self.0, value);
    }
}
impl<T> EqualAction<&str> for (Field<T, Vec<String>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
{
    #[inline]
    fn equal(self, value: &str) {
        <String as SimpleCondition<T, String>>::contains(self.1, self.0, value.to_string());
    }
}

impl<T, V> EqualAction<V> for (Field<T, Option<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: SimpleCondition<T, V> + PartialEq + Clone + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        V::is(self.1, self.0, value);
    }
}
impl<T> EqualAction<&str> for (Field<T, Option<String>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
{
    #[inline]
    fn equal(self, value: &str) {
        <String as SimpleCondition<T, String>>::is(self.1, self.0, value.to_string());
    }
}

impl<T: 'static, V> EqualAction<V> for (Field<T, V>, &mut EmbeddedFilterBuilder<T>)
where
    V: SimpleEmbeddedCondition<T, V> + PartialEq + Clone + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        V::equal(self.1, self.0, value);
    }
}

impl<T: 'static> EqualAction<&str> for (Field<T, String>, &mut EmbeddedFilterBuilder<T>) {
    #[inline]
    fn equal(self, value: &str) {
        <String as SimpleEmbeddedCondition<T, String>>::equal(self.1, self.0, value.to_string());
    }
}

impl<T: 'static, V> EqualAction<V> for (Field<T, Vec<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: SimpleEmbeddedCondition<T, V> + PartialEq + Clone + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        V::contains(self.1, self.0, value);
    }
}
impl<T: 'static, V> EqualAction<V> for (Field<T, Option<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: SimpleEmbeddedCondition<T, V> + PartialEq + Clone + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        V::is(self.1, self.0, value);
    }
}

pub trait RangeAction<X> {
    fn range(self, value: impl RangeBounds<X>);
}
impl<T, V> RangeAction<V> for (Field<T, V>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: RangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        V::range(self.1, self.0, value);
    }
}
impl<T, V> RangeAction<V> for (Field<T, Vec<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: RangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        V::range_contains(self.1, self.0, value);
    }
}

impl<T, V> RangeAction<V> for (Field<T, Option<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: RangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        V::range_is(self.1, self.0, value);
    }
}

impl<'a, T> RangeAction<&'a str> for (Field<T, String>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<&'a str>) {
        self.1.indexable_range_str(self.0, value)
    }
}

impl<T: 'static, V> RangeAction<V> for (Field<T, V>, &mut EmbeddedFilterBuilder<T>)
where
    V: EmbeddedRangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        V::range(self.1, self.0, value);
    }
}
impl<T: 'static, V> RangeAction<V> for (Field<T, Vec<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: EmbeddedRangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        V::range_contains(self.1, self.0, value);
    }
}
impl<T: 'static, V> RangeAction<V> for (Field<T, Option<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: EmbeddedRangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        V::range_is(self.1, self.0, value);
    }
}

impl<'a, T: 'static> RangeAction<&'a str> for (Field<T, String>, &mut EmbeddedFilterBuilder<T>) {
    #[inline]
    fn range(self, value: impl RangeBounds<&'a str>) {
        self.1.simple_range_str(self.0, value)
    }
}

pub trait QueryAction<X> {
    fn query(self, value: X);
}

impl<T: 'static, V> QueryAction<StructsyQuery<V>> for (Field<T, Ref<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: StructsyQuery<V>) {
        self.1.ref_query(self.0, value.builder());
    }
}

impl<T: 'static, V> QueryAction<Filter<V>> for (Field<T, V>, &mut EmbeddedFilterBuilder<T>)
where
    V: EmbeddedDescription + 'static,
{
    #[inline]
    fn query(self, value: Filter<V>) {
        self.1.simple_persistent_embedded(self.0, value.extract_filter());
    }
}

impl<T: 'static, V> QueryAction<SnapshotQuery<V>> for (Field<T, Ref<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: SnapshotQuery<V>) {
        self.1.ref_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<StructsyQuery<V>> for (Field<T, Option<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: StructsyQuery<V>) {
        self.1.ref_option_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<SnapshotQuery<V>> for (Field<T, Option<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: SnapshotQuery<V>) {
        self.1.ref_option_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<Filter<V>> for (Field<T, Option<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: Filter<V>) {
        self.1.ref_option_query(self.0, value.extract_filter());
    }
}

impl<T, V> QueryAction<Filter<V>> for (Field<T, V>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: EmbeddedDescription + 'static,
{
    #[inline]
    fn query(self, value: Filter<V>) {
        self.1.simple_persistent_embedded(self.0, value.extract_filter());
    }
}

impl<T, V> QueryAction<Filter<V>> for (Field<T, Ref<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: Filter<V>) {
        self.1.ref_query(self.0, value.extract_filter());
    }
}

impl<T, V> QueryAction<StructsyQuery<V>> for (Field<T, Ref<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: StructsyQuery<V>) {
        self.1.ref_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<SnapshotQuery<V>> for (Field<T, Ref<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: SnapshotQuery<V>) {
        self.1.ref_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<StructsyQuery<V>> for (Field<T, Vec<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: StructsyQuery<V>) {
        self.1.ref_vec_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<SnapshotQuery<V>> for (Field<T, Vec<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: SnapshotQuery<V>) {
        self.1.ref_vec_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<Filter<V>> for (Field<T, Vec<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: Filter<V>) {
        self.1.ref_vec_query(self.0, value.extract_filter());
    }
}

pub trait OrderAction {
    fn order(self, value: Order);
}

impl<T, V> OrderAction for (Field<T, V>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: Ord + crate::filter::Scan<T> + 'static,
{
    #[inline]
    fn order(self, value: Order) {
        self.1.order(self.0, value)
    }
}

impl<T, V> OrderAction for (Field<T, V>, &mut EmbeddedFilterBuilder<T>)
where
    T: PersistentEmbedded + 'static,
    V: Ord + 'static,
{
    #[inline]
    fn order(self, value: Order) {
        self.1.order(self.0, value)
    }
}

#[cfg(test)]
mod tests {
    use super::{EqualAction, FilterBuilder, Query, StructsyFilter};
    use crate::{
        internal::{Description, Field, FilterDefinition},
        Persistent, Ref, SRes, Sytx,
    };

    use std::io::{Read, Write};
    struct ToQuery {
        first: String,
        second: Vec<String>,
    }
    impl FilterDefinition for ToQuery {
        type Filter = FilterBuilder<Self>;
    }
    impl Persistent for ToQuery {
        fn get_name() -> &'static str {
            "ToQuery"
        }
        fn get_description() -> Description {
            let fields = [
                crate::internal::FieldDescription::new::<String>(0u32, "first", None),
                crate::internal::FieldDescription::new::<Vec<String>>(2u32, "second", None),
            ];
            Description::Struct(crate::internal::StructDescription::new("ToQuery", &fields))
        }
        fn read(_read: &mut dyn Read) -> SRes<Self>
        where
            Self: std::marker::Sized,
        {
            unimplemented!()
        }
        fn remove_indexes(&self, _tx: &mut dyn Sytx, _id: &Ref<Self>) -> SRes<()>
        where
            Self: std::marker::Sized,
        {
            unimplemented!()
        }
        fn write(&self, _write: &mut dyn Write) -> SRes<()> {
            unimplemented!()
        }
        fn put_indexes(&self, _tx: &mut dyn Sytx, _id: &Ref<Self>) -> SRes<()>
        where
            Self: std::marker::Sized,
        {
            unimplemented!()
        }
        fn declare(_db: &mut dyn Sytx) -> SRes<()> {
            unimplemented!()
        }
    }
    impl ToQuery {
        pub fn field_first() -> Field<Self, String> {
            Field::<ToQuery, String>::new("first", |x| &x.first)
        }
        pub fn field_second() -> Field<Self, Vec<String>> {
            Field::<ToQuery, Vec<String>>::new("second", |x| &x.second)
        }
    }

    trait MyQuery {
        fn by_name(self, first: String) -> Self;
        fn by_second(self, second: String) -> Self;
        fn by_first_and_second(self, first: String, second: String) -> Self;
    }

    impl MyQuery for StructsyFilter<ToQuery> {
        fn by_name(mut self, first: String) -> Self {
            let builder = self.filter_builder();
            EqualAction::equal((ToQuery::field_first(), builder), first);
            self
        }
        fn by_second(mut self, second: String) -> Self {
            let builder = self.filter_builder();
            EqualAction::equal((ToQuery::field_second(), builder), second);
            self
        }
        fn by_first_and_second(mut self, first: String, second: String) -> Self {
            EqualAction::equal((ToQuery::field_first(), self.filter_builder()), first);
            EqualAction::equal((ToQuery::field_second(), self.filter_builder()), second);
            self
        }
    }
    #[test]
    fn test_query_build() {
        let filter = StructsyFilter::<ToQuery>::new();
        filter.by_name("one".to_string()).by_second("second".to_string());
    }
}
