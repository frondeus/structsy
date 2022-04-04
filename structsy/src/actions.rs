use crate::{
    filter::Filter,
    filter_builder::{
        EmbeddedFilterBuilder, EmbeddedRangeCondition, FilterBuilder, RangeCondition, SimpleCondition,
        SimpleEmbeddedCondition, SolveQueryValue, ValueCompare, ValueRange,
    },
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

impl<T, V: PersistentEmbedded + SolveQueryValue + ValueCompare> EqualAction<V>
    for (Field<T, Vec<V>>, &mut FilterBuilder<T>)
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

impl<T, V: PersistentEmbedded + SolveQueryValue + ValueCompare> EqualAction<V>
    for (Field<T, Option<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: SimpleCondition<T, V> + PartialEq + Clone + 'static,
{
    #[inline]
    fn equal(self, value: V) {
        <V as SimpleCondition<T, V>>::is(self.1, self.0, value);
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
    V: SimpleEmbeddedCondition<T, V> + PartialEq + Clone + 'static + SolveQueryValue + ValueCompare,
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
    V: SimpleEmbeddedCondition<T, V> + PartialEq + Clone + 'static + SolveQueryValue + ValueCompare,
{
    #[inline]
    fn equal(self, value: V) {
        V::contains(self.1, self.0, value);
    }
}
impl<T: 'static, V> EqualAction<V> for (Field<T, Option<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: SimpleEmbeddedCondition<T, V> + PartialEq + Clone + 'static + SolveQueryValue + ValueCompare,
{
    #[inline]
    fn equal(self, value: V) {
        <V as SimpleEmbeddedCondition<T, V>>::is(self.1, self.0, value);
    }
}

pub trait RangeAction<X> {
    fn range(self, value: impl RangeBounds<X>);
}
impl<T, V: PersistentEmbedded + SolveQueryValue + ValueRange> RangeAction<V> for (Field<T, V>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: RangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        <V as RangeCondition<T, V>>::range(self.1, self.0, value);
    }
}
impl<T, V: PersistentEmbedded + SolveQueryValue + ValueRange> RangeAction<V>
    for (Field<T, Vec<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: RangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        <V as RangeCondition<T, V>>::range_contains(self.1, self.0, value);
    }
}

impl<T, V: PersistentEmbedded + SolveQueryValue + ValueRange> RangeAction<V>
    for (Field<T, Option<V>>, &mut FilterBuilder<T>)
where
    T: Persistent + 'static,
    V: RangeCondition<T, V> + PartialOrd + Clone + 'static,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        <V as RangeCondition<T, V>>::range_is(self.1, self.0, value);
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
    V: EmbeddedRangeCondition<T, V> + PartialOrd + Clone + 'static + SolveQueryValue + ValueRange,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        <V as EmbeddedRangeCondition<T, V>>::range(self.1, self.0, value);
    }
}
impl<T: 'static, V> RangeAction<V> for (Field<T, Vec<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: EmbeddedRangeCondition<T, V> + PartialOrd + Clone + 'static + SolveQueryValue + ValueRange,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        <V as EmbeddedRangeCondition<T, V>>::range_contains(self.1, self.0, value);
    }
}
impl<T: 'static, V> RangeAction<V> for (Field<T, Option<V>>, &mut EmbeddedFilterBuilder<T>)
where
    V: EmbeddedRangeCondition<T, V> + PartialOrd + Clone + 'static + SolveQueryValue + ValueRange,
{
    #[inline]
    fn range(self, value: impl RangeBounds<V>) {
        <V as EmbeddedRangeCondition<T, V>>::range_is(self.1, self.0, value);
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
    V: Ord + ValueRange + crate::filter_builder::Scan<T> + 'static,
{
    #[inline]
    fn order(self, value: Order) {
        self.1.order(self.0, value)
    }
}

impl<T, V> OrderAction for (Field<T, V>, &mut EmbeddedFilterBuilder<T>)
where
    T: PersistentEmbedded + 'static,
    V: Ord + ValueRange + 'static,
{
    #[inline]
    fn order(self, value: Order) {
        self.1.order(self.0, value)
    }
}
