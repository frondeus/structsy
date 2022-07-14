use crate::{
    filter::Filter,
    filter_builder::{FilterBuilder, SolveQueryValue, ValueCompare, ValueRange},
    internal::{EmbeddedDescription, Field},
    queries::{SnapshotQuery, StructsyQuery},
    Order, Persistent, PersistentEmbedded, Ref,
};
use std::ops::RangeBounds;

pub trait EqualAction<X> {
    fn equal(self, value: X);
}
impl<T, V: PersistentEmbedded + SolveQueryValue + ValueCompare> EqualAction<V> for (Field<T, V>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: PartialEq + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        self.1.cond_equal(self.0, value);
    }
}

impl<T> EqualAction<&str> for (Field<T, String>, &mut FilterBuilder<T>)
where
    T: 'static,
{
    #[inline]
    fn equal(self, value: &str) {
        self.1.cond_equal(self.0, value.to_string());
    }
}

impl<T, V: PersistentEmbedded + SolveQueryValue + ValueCompare> EqualAction<V>
    for (Field<T, Vec<V>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: PartialEq + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        self.1.cond_contains(self.0, value);
    }
}
impl<T> EqualAction<&str> for (Field<T, Vec<String>>, &mut FilterBuilder<T>)
where
    T: 'static,
{
    #[inline]
    fn equal(self, value: &str) {
        self.1.cond_contains(self.0, value.to_string());
    }
}

impl<T, V: PersistentEmbedded + SolveQueryValue + ValueCompare> EqualAction<V>
    for (Field<T, Option<V>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: PartialEq + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        self.1.cond_is(self.0, value);
    }
}
impl<T> EqualAction<&str> for (Field<T, Option<String>>, &mut FilterBuilder<T>)
where
    T: 'static,
{
    #[inline]
    fn equal(self, value: &str) {
        self.1.cond_is(self.0, value.to_string());
    }
}

pub trait RangeAction<X> {
    fn range(self, value: impl RangeBounds<X>);
}

impl<T, V: PersistentEmbedded + SolveQueryValue + ValueRange> RangeAction<V> for (Field<T, V>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        self.1.cond_range(self.0, value);
    }
}
impl<T, V: PersistentEmbedded + SolveQueryValue + ValueRange> RangeAction<V>
    for (Field<T, Vec<V>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        self.1.cond_range_contains(self.0, value);
    }
}

impl<T, V: PersistentEmbedded + SolveQueryValue + ValueRange> RangeAction<V>
    for (Field<T, Option<V>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        self.1.cond_range_is(self.0, value);
    }
}

impl<'a, T> RangeAction<&'a str> for (Field<T, String>, &mut FilterBuilder<T>)
where
    T: 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<&'a str>) {
        self.1.cond_range_str(self.0, value)
    }
}

pub trait QueryAction<X> {
    fn query(self, value: X);
}

impl<T, V> QueryAction<StructsyQuery<V>> for (Field<T, Option<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: StructsyQuery<V>) {
        self.1.ref_option_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<SnapshotQuery<V>> for (Field<T, Option<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: SnapshotQuery<V>) {
        self.1.ref_option_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<Filter<V>> for (Field<T, Option<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: Filter<V>) {
        self.1.ref_option_query(self.0, value.extract_filter());
    }
}

impl<T, V> QueryAction<Filter<V>> for (Field<T, V>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: EmbeddedDescription + 'static,
{
    #[inline]
    fn query(self, value: Filter<V>) {
        self.1.simple_persistent_embedded(self.0, value.extract_filter());
    }
}

impl<T, V> QueryAction<Filter<V>> for (Field<T, Ref<V>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: Filter<V>) {
        self.1.ref_query(self.0, value.extract_filter());
    }
}

impl<T, V> QueryAction<StructsyQuery<V>> for (Field<T, Ref<V>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: StructsyQuery<V>) {
        self.1.ref_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<SnapshotQuery<V>> for (Field<T, Ref<V>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: SnapshotQuery<V>) {
        self.1.ref_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<StructsyQuery<V>> for (Field<T, Vec<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: StructsyQuery<V>) {
        self.1.ref_vec_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<SnapshotQuery<V>> for (Field<T, Vec<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: 'static,
    V: Persistent + 'static,
{
    #[inline]
    fn query(self, value: SnapshotQuery<V>) {
        self.1.ref_vec_query(self.0, value.builder());
    }
}

impl<T, V> QueryAction<Filter<V>> for (Field<T, Vec<Ref<V>>>, &mut FilterBuilder<T>)
where
    T: 'static,
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
    T: 'static,
    V: Ord + ValueRange + 'static,
{
    #[inline]
    fn order(self, value: Order) {
        self.1.order(self.0, value)
    }
}
