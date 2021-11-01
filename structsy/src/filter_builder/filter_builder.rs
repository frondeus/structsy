use crate::{
    filter_builder::{
        embedded_filter_builder::EmbeddedFilterBuilder,
        execution_iterator::ExecutionIterator,
        execution_step::ExecutionStep,
        filter_builder_step::{
            AndFilter, ConditionFilter, ConditionSingleFilter, EmbeddedFieldFilter, FilterBuilderStep, IndexFilter,
            NotFilter, OptionQueryFilter, OrFilter, QueryFilter, RangeConditionFilter, RangeIndexFilter,
            RangeOptionConditionFilter, RangeSingleConditionFilter, RangeSingleIndexFilter, VecQueryFilter,
        },
        reader::Reader,
        start::{ScanStartStep, StartStep},
    },
    index::{find_range, find_range_snap, find_range_tx},
    internal::{Description, EmbeddedDescription, Field},
    structsy::SnapshotIterator,
    transaction::{RefSytx, TxIterator},
    Order, OwnedSytx, Persistent, PersistentEmbedded, Ref, Snapshot, Structsy,
};
use std::ops::{Bound, RangeBounds};

pub(crate) struct Item<P> {
    pub(crate) id: Ref<P>,
    pub(crate) record: P,
}

impl<P> Item<P> {
    pub(crate) fn new((id, record): (Ref<P>, P)) -> Self {
        Self { id, record }
    }
}

struct RevTx<X> {
    base: X,
}

impl<X, P, K> Iterator for RevTx<X>
where
    X: Iterator<Item = (Ref<P>, P, K)>,
    X: DoubleEndedIterator,
{
    type Item = (Ref<P>, P, K);
    fn next(&mut self) -> Option<Self::Item> {
        self.base.next_back()
    }
}

impl<'a, X, P, K> TxIterator<'a> for RevTx<X>
where
    X: TxIterator<'a>,
    X: Iterator<Item = (Ref<P>, P, K)>,
    X: DoubleEndedIterator,
{
    fn tx(&mut self) -> RefSytx {
        self.base.tx()
    }
}

struct MapTx<'a, P, K> {
    base: Box<dyn TxIterator<'a, Item = (Ref<P>, P, K)> + 'a>,
}

impl<'a, P, K> MapTx<'a, P, K> {
    fn new<T: TxIterator<'a, Item = (Ref<P>, P, K)> + 'a>(base: T) -> Self {
        MapTx { base: Box::new(base) }
    }
}

impl<'a, P, K> Iterator for MapTx<'a, P, K> {
    type Item = (Ref<P>, P);
    fn next(&mut self) -> Option<Self::Item> {
        self.base.next().map(|(id, rec, _)| (id, rec))
    }
}

impl<'a, P, K> TxIterator<'a> for MapTx<'a, P, K> {
    fn tx(&mut self) -> RefSytx {
        self.base.tx()
    }
}

pub enum Iter<'a, P> {
    TxIter(Box<dyn TxIterator<'a, Item = (Ref<P>, P)> + 'a>),
    SnapshotIter(Box<dyn SnapshotIterator<Item = (Ref<P>, P)>>),
    Iter(Box<dyn Iterator<Item = (Ref<P>, P)>>),
}

pub(crate) trait Source<T> {
    fn next_item(&mut self) -> Option<Item<T>>;
}
impl<'a, T: Persistent + 'static> Source<T> for (&mut Iter<'a, T>, &mut Conditions<T>, &Structsy) {
    fn next_item(&mut self) -> Option<Item<T>> {
        ExecutionIterator::filtered_next(self.0, self.1, self.2)
    }
}

fn clone_bound_ref<X: Clone>(bound: &Bound<&X>) -> Bound<X> {
    match bound {
        Bound::Included(x) => Bound::Included((*x).clone()),
        Bound::Excluded(x) => Bound::Excluded((*x).clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub trait RangeCondition<T: Persistent + 'static, V: Clone + PartialOrd + 'static> {
    fn range<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, V>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter.add(RangeConditionFilter::new(field, (start, end)))
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter.add(RangeSingleConditionFilter::new(field, (start, end)))
    }

    fn range_is<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, Option<V>>, range: R) {
        let start = match range.start_bound() {
            Bound::Included(x) => Bound::Included(Some(x.clone())),
            Bound::Excluded(x) => Bound::Excluded(Some(x.clone())),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(x) => Bound::Included(Some(x.clone())),
            Bound::Excluded(x) => Bound::Excluded(Some(x.clone())),
            Bound::Unbounded => Bound::Unbounded,
        };
        // This may support index in future, but it does not now
        filter.add(RangeOptionConditionFilter::new(field, (start, end)))
    }
}
pub trait SimpleCondition<T: Persistent + 'static, V: Clone + PartialEq + 'static> {
    fn equal(filter: &mut FilterBuilder<T>, field: Field<T, V>, value: V) {
        filter.add(ConditionFilter::new(field, value))
    }

    fn contains(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter.add(ConditionSingleFilter::new(field, value))
    }

    fn is(filter: &mut FilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.add(ConditionFilter::new(field, Some(value)))
    }
}

impl<T: Persistent + 'static> RangeCondition<T, bool> for bool {}
impl<T: Persistent + 'static> SimpleCondition<T, bool> for bool {}
impl<T: Persistent + 'static> SimpleCondition<T, Vec<bool>> for Vec<bool> {}
impl<T: Persistent + 'static> RangeCondition<T, Vec<bool>> for Vec<bool> {}

impl<T: Persistent + 'static, R: Persistent + 'static> SimpleCondition<T, Ref<R>> for Ref<R> {}
impl<T: Persistent + 'static, R: Persistent + 'static> RangeCondition<T, Ref<R>> for Ref<R> {}
impl<T: Persistent + 'static, R: Persistent + 'static> SimpleCondition<T, Vec<Ref<R>>> for Vec<Ref<R>> {}
impl<T: Persistent + 'static, R: Persistent + 'static> RangeCondition<T, Vec<Ref<R>>> for Vec<Ref<R>> {}
impl<T: Persistent + 'static, R: Persistent + 'static> SimpleCondition<T, Option<Ref<R>>> for Option<Ref<R>> {}
impl<T: Persistent + 'static, R: Persistent + 'static> RangeCondition<T, Option<Ref<R>>> for Option<Ref<R>> {}
macro_rules! index_conditions {
    ($($t:ty),+) => {
        $(
        impl<T: Persistent + 'static> RangeCondition<T, $t> for $t {
            fn range<R: RangeBounds<$t>>(filter: &mut FilterBuilder<T>, field: Field<T, $t>, range: R) {
                let start = clone_bound_ref(&range.start_bound());
                let end = clone_bound_ref(&range.end_bound());
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(RangeIndexFilter::new(index_name, field, (start, end)))
                } else {
                    filter.add(RangeConditionFilter::new(field,( start, end)))
                }
            }

            fn range_contains<R: RangeBounds<$t>>(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, range: R) {
                let start = clone_bound_ref(&range.start_bound());
                let end = clone_bound_ref(&range.end_bound());
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(RangeSingleIndexFilter::new(index_name, field, (start, end)))
                } else {
                    filter.add(RangeSingleConditionFilter::new(field, (start, end)))
                }
            }
        }

        impl<T: Persistent + 'static> SimpleCondition<T, $t>  for $t {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, $t>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field, value))
                }
            }
            fn contains(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionSingleFilter::new(field, value))
                }
            }
            fn is(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field, Some(value)))
                }
            }
        }

        impl<T: Persistent + 'static> SimpleCondition<T, Vec<$t>> for Vec<$t> {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, value: Vec<$t>) {
                if let Some(_index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    // TODO: support index search for vec types
                    filter.add(ConditionFilter::new(field, value))
                //filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field, value))
                }
            }
        }

        impl<T: Persistent + 'static> RangeCondition<T, Vec<$t>> for Vec<$t>{}

        impl<T: Persistent + 'static> SimpleCondition<T, Option<$t>> for Option<$t>
        {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, value: Option<$t>) {
                if let (Some(index_name), Some(v)) = (FilterBuilder::<T>::is_indexed(field.name), value.clone()) {
                    filter.add(IndexFilter::new(index_name, v));
                } else {
                    filter.add(ConditionFilter::<Option<$t>, T>::new(field, value));
                }
            }
        }

        impl<T: Persistent + 'static> RangeCondition<T, Option<$t>> for Option<$t> {
            fn range<R: RangeBounds<Option<$t>>>(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, range: R) {
                let start = clone_bound_ref(&range.start_bound());
                let end = clone_bound_ref(&range.end_bound());
                // This may support index in future, but it does not now
                filter.add(RangeOptionConditionFilter::new(field, (start, end)))
            }
        }

        impl<P:Persistent + 'static> Scan<P> for $t {
            fn scan_impl<'a>( field:&Field<P,Self>, structsy:Structsy, order:&Order) -> Option<Iter<'a,P>> {
                if let Some(index_name) = FilterBuilder::<P>::is_indexed(field.name) {
                    if let Ok(iter) = find_range::<Self,P,_>(&structsy, &index_name, ..) {
                        let it:Box<dyn Iterator<Item = (Ref<P>, P)>> = if order == &Order::Desc {
                            Box::new(iter.rev().map(|(r, e, _)| (r, e)))
                        } else {
                            Box::new(iter.map(|(r, e, _)| (r, e)))
                        };
                        Some(Iter::Iter(it))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            fn scan_tx_impl<'a>(field:&Field<P,Self>, tx:&'a mut OwnedSytx, order:&Order) -> Option<Iter<'a,P>> {
                if let Some(index_name) = FilterBuilder::<P>::is_indexed(field.name) {
                    if let Ok(iter) = find_range_tx::<Self,P,_>(tx, &index_name, ..) {
                        let it = if order == &Order::Desc {
                            MapTx::new(RevTx{base:iter})
                        } else {
                            MapTx::new(iter)
                        };
                        Some(Iter::<'a>::TxIter(Box::new(it)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            fn scan_snapshot_impl<'a>( field:&Field<P,Self>, snapshot:&Snapshot, order:&Order) -> Option<Iter<'a,P>> {
                if let Some(index_name) = FilterBuilder::<P>::is_indexed(field.name) {
                    if let Ok(iter) = find_range_snap::<Self,P,_>(snapshot, &index_name, ..) {
                        let it:Box<dyn Iterator<Item = (Ref<P>, P)>> = if order == &Order::Desc {
                            Box::new(iter.rev().map(|(r, e, _)| (r, e)))
                        } else {
                            Box::new(iter.map(|(r, e, _)| (r, e)))
                        };
                        Some(Iter::Iter(it))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            fn is_indexed_impl(field:&Field<P,Self>) -> bool {
                if let Some(_) = FilterBuilder::<P>::is_indexed(field.name) {
                    true
                } else {
                    false
                }
            }
        }

        )+
    };
}

index_conditions!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, String);

impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static> SimpleCondition<T, V> for V {}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static> RangeCondition<T, V> for V {}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static> SimpleCondition<T, Vec<V>>
    for Vec<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static> RangeCondition<T, Vec<V>>
    for Vec<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static> SimpleCondition<T, Option<V>>
    for Option<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static> RangeCondition<T, Option<V>>
    for Option<V>
{
}

impl<P, V: EmbeddedDescription> Scan<P> for V {}

pub(crate) struct Conditions<T> {
    conditions: Vec<Box<dyn ExecutionStep<Target = T>>>,
}
impl<T: 'static> Conditions<T> {
    pub(crate) fn check(&self, item: &Item<T>, reader: &mut Reader) -> bool {
        for condition in &self.conditions {
            if !condition.check(item, reader) {
                return false;
            }
        }
        true
    }
}

pub(crate) struct FieldOrder<T, V> {
    field: Field<T, V>,
    order: Order,
}

impl<T: 'static, V: Ord + 'static + Scan<T>> FieldOrder<T, V> {
    pub(crate) fn new(field: Field<T, V>, order: Order) -> Self {
        Self { field, order }
    }
}

impl<T: 'static, V: Ord + 'static> FieldOrder<T, V> {
    pub(crate) fn new_emb(field: Field<T, V>, order: Order) -> Self {
        Self { field, order }
    }
}

pub(crate) trait OrderStep<P> {
    fn compare(&self, first: &P, second: &P) -> std::cmp::Ordering;
}

pub(crate) trait ScanOrderStep<P>: OrderStep<P> {
    fn scan(&self, structsy: Structsy) -> Option<Iter<'static, P>>;
    fn scan_tx<'a>(&self, tx: &'a mut OwnedSytx) -> Option<Iter<'a, P>>;
    fn scan_snapshot(&self, snapshot: &Snapshot) -> Option<Iter<'static, P>>;
    fn is_indexed(&self) -> bool;
}

impl<P, V: Ord> OrderStep<P> for FieldOrder<P, V> {
    fn compare(&self, first: &P, second: &P) -> std::cmp::Ordering {
        let ord = (self.field.access)(first).cmp((self.field.access)(second));
        if self.order == Order::Asc {
            ord
        } else {
            ord.reverse()
        }
    }
}

impl<P, V: Ord + Scan<P>> ScanOrderStep<P> for FieldOrder<P, V> {
    fn scan(&self, structsy: Structsy) -> Option<Iter<'static, P>> {
        Scan::scan_impl(&self.field, structsy, &self.order)
    }
    fn scan_tx<'a>(&self, tx: &'a mut OwnedSytx) -> Option<Iter<'a, P>> {
        Scan::scan_tx_impl(&self.field, tx, &self.order)
    }
    fn scan_snapshot(&self, snapshot: &Snapshot) -> Option<Iter<'static, P>> {
        Scan::scan_snapshot_impl(&self.field, snapshot, &self.order)
    }
    fn is_indexed(&self) -> bool {
        Scan::is_indexed_impl(&self.field)
    }
}
pub trait Scan<P>: Sized {
    fn scan_impl<'a>(_field: &Field<P, Self>, _structsy: Structsy, _order: &Order) -> Option<Iter<'a, P>> {
        None
    }
    fn scan_tx_impl<'a>(_field: &Field<P, Self>, _tx: &'a mut OwnedSytx, _order: &Order) -> Option<Iter<'a, P>> {
        None
    }
    fn scan_snapshot_impl<'a>(_field: &Field<P, Self>, _snapshot: &Snapshot, _order: &Order) -> Option<Iter<'a, P>> {
        None
    }
    fn is_indexed_impl(_field: &Field<P, Self>) -> bool {
        false
    }
}

pub(crate) struct EmbeddedOrder<T, V> {
    field: Field<T, V>,
    orders: Vec<Box<dyn OrderStep<V>>>,
}

impl<T: 'static, V: 'static> EmbeddedOrder<T, V> {
    pub(crate) fn new(field: Field<T, V>, orders: Vec<Box<dyn OrderStep<V>>>) -> Self {
        Self { field, orders }
    }
    pub(crate) fn new_emb(field: Field<T, V>, orders: Vec<Box<dyn OrderStep<V>>>) -> Self {
        Self { field, orders }
    }
}

impl<P, V> OrderStep<P> for EmbeddedOrder<P, V> {
    fn compare(&self, first: &P, second: &P) -> std::cmp::Ordering {
        let emb_first = (self.field.access)(first);
        let emb_second = (self.field.access)(second);
        for order in &self.orders {
            let ord = order.compare(emb_first, emb_second);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    }
}
impl<P, V> ScanOrderStep<P> for EmbeddedOrder<P, V> {
    fn scan(&self, _structsy: Structsy) -> Option<Iter<'static, P>> {
        None
    }
    fn scan_tx<'a>(&self, _tx: &'a mut OwnedSytx) -> Option<Iter<'a, P>> {
        None
    }
    fn scan_snapshot(&self, _snapshot: &Snapshot) -> Option<Iter<'static, P>> {
        None
    }
    fn is_indexed(&self) -> bool {
        false
    }
}

pub(crate) trait BufferedExection<P> {
    fn next(&mut self, source: &mut dyn Source<P>) -> Option<Item<P>>;
}
struct BufferedOrderExecution<T> {
    buffer: Option<std::vec::IntoIter<Item<T>>>,
    orders: Vec<Box<dyn ScanOrderStep<T>>>,
}
impl<T: 'static> BufferedOrderExecution<T> {
    fn new(orders: Vec<Box<dyn ScanOrderStep<T>>>) -> Box<dyn BufferedExection<T>> {
        Box::new(BufferedOrderExecution { buffer: None, orders })
    }
}

impl<T> BufferedOrderExecution<T> {
    fn order_item(&self, first: &Item<T>, second: &Item<T>) -> std::cmp::Ordering {
        for order in &self.orders {
            let ord = order.compare(&first.record, &second.record);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl<T> BufferedExection<T> for BufferedOrderExecution<T> {
    fn next(&mut self, source: &mut dyn Source<T>) -> Option<Item<T>> {
        if let Some(b) = &mut self.buffer {
            b.next()
        } else {
            let mut buffer = Vec::<Item<T>>::new();
            while let Some(item) = source.next_item() {
                let index = match buffer.binary_search_by(|e| self.order_item(e, &item)) {
                    Ok(index) => index,
                    Err(index) => index,
                };
                buffer.insert(index, item);
            }
            self.buffer = Some(buffer.into_iter());
            self.buffer.as_mut().unwrap().next()
        }
    }
}

pub(crate) struct Orders<T> {
    order: Vec<Box<dyn ScanOrderStep<T>>>,
}
impl<T: 'static> Orders<T> {
    pub(crate) fn buffered(self) -> Option<Box<dyn BufferedExection<T>>> {
        if self.order.is_empty() {
            None
        } else {
            Some(BufferedOrderExecution::new(self.order))
        }
    }
}
impl<T: Persistent + 'static> Orders<T> {
    pub(crate) fn scan(self, structsy: Structsy) -> (Option<Box<dyn BufferedExection<T>>>, Option<Iter<'static, T>>) {
        if self.order.is_empty() {
            return (None, None);
        }
        let mut orders = self.order;
        let first_entry = orders.remove(0);
        let scan = first_entry.scan(structsy);
        if let Some(iter) = scan {
            if orders.is_empty() {
                (None, Some(iter))
            } else {
                (Some(BufferedOrderExecution::new(orders)), Some(iter))
            }
        } else {
            orders.insert(0, first_entry);
            (Some(BufferedOrderExecution::new(orders)), None)
        }
    }

    pub(crate) fn index_order(&self) -> bool {
        if let Some(first) = self.order.first() {
            first.is_indexed()
        } else {
            false
        }
    }

    pub(crate) fn scan_tx(self, tx: &mut OwnedSytx) -> (Option<Box<dyn BufferedExection<T>>>, Option<Iter<T>>) {
        if self.order.is_empty() {
            return (None, None);
        }
        let mut orders = self.order;
        let first_entry = orders.remove(0);
        let scan = first_entry.scan_tx(tx);
        if let Some(iter) = scan {
            if orders.is_empty() {
                (None, Some(iter))
            } else {
                (Some(BufferedOrderExecution::new(orders)), Some(iter))
            }
        } else {
            orders.insert(0, first_entry);
            (Some(BufferedOrderExecution::new(orders)), None)
        }
    }
    pub(crate) fn scan_snapshot(
        self,
        snapshot: &Snapshot,
    ) -> (Option<Box<dyn BufferedExection<T>>>, Option<Iter<'static, T>>) {
        if self.order.is_empty() {
            return (None, None);
        }
        let mut orders = self.order;
        let first_entry = orders.remove(0);
        let scan = first_entry.scan_snapshot(snapshot);
        if let Some(iter) = scan {
            if orders.is_empty() {
                (None, Some(iter))
            } else {
                (Some(BufferedOrderExecution::new(orders)), Some(iter))
            }
        } else {
            orders.insert(0, first_entry);
            (Some(BufferedOrderExecution::new(orders)), None)
        }
    }
}

pub struct FilterBuilder<T> {
    pub(crate) steps: Vec<Box<dyn FilterBuilderStep<Target = T>>>,
    order: Orders<T>,
}
impl<T: 'static> Default for FilterBuilder<T> {
    fn default() -> Self {
        FilterBuilder::new()
    }
}

impl<T: 'static> FilterBuilder<T> {
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder {
            steps: Vec::new(),
            order: Orders { order: Vec::new() },
        }
    }

    fn add<F: FilterBuilderStep<Target = T> + 'static>(&mut self, filter: F) {
        self.steps.push(Box::new(filter));
    }

    fn add_order<O: ScanOrderStep<T> + 'static>(&mut self, order: O) {
        self.order.order.push(Box::new(order))
    }
}

impl<T: Persistent + 'static> FilterBuilder<T> {
    fn is_indexed(name: &str) -> Option<String> {
        let desc = T::get_description();
        if let Description::Struct(st) = &desc {
            if let Some(f) = st.get_field(name) {
                if f.indexed.is_some() {
                    Some(format!("{}.{}", st.get_name(), f.name))
                } else {
                    None
                }
            } else {
                panic!("field with name:'{}' not found", name)
            }
        } else {
            None
        }
    }

    pub(crate) fn fill_conditions_step(self, reader: &mut Reader) -> Conditions<T> {
        let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
        executions.sort_by_key(|x| x.get_score());
        Self::fill_conditions(executions)
    }

    fn fill_conditions(executions: Vec<Box<dyn ExecutionStep<Target = T>>>) -> Conditions<T> {
        Conditions { conditions: executions }
    }

    pub fn finish<'a>(self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            let start = Box::new(ScanStartStep::new());
            let cond = Self::fill_conditions(Vec::new());
            Box::new(start.start(cond, self.order, structsy.clone()))
        } else {
            let reader = &mut Reader::Structsy(structsy.clone());
            let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
            executions.sort_by_key(|x| x.get_score());
            let (step, start) = executions.pop().unwrap().as_start();
            if let Some(es) = step {
                executions.insert(0, es);
            }
            let cond = Self::fill_conditions(executions);
            Box::new(start.start(cond, self.order, structsy.clone()))
        }
    }

    pub fn finish_tx<'a>(self, tx: &'a mut OwnedSytx) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            let start = Box::new(ScanStartStep::new());
            let cond = Self::fill_conditions(Vec::new());
            Box::new(start.start_tx(cond, self.order, tx))
        } else {
            let reader = &mut Reader::Tx(tx.reference());
            let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
            let (step, start) = executions.pop().unwrap().as_start();
            if let Some(es) = step {
                executions.insert(0, es);
            }
            let cond = Self::fill_conditions(executions);
            Box::new(start.start_tx(cond, self.order, tx))
        }
    }

    pub fn finish_snap(self, tx: &Snapshot) -> Box<dyn Iterator<Item = (Ref<T>, T)>> {
        if self.steps.is_empty() {
            let start = Box::new(ScanStartStep::new());
            let cond = Self::fill_conditions(Vec::new());
            Box::new(start.start_snapshot(cond, self.order, tx))
        } else {
            let reader = &mut Reader::Snapshot(tx.clone());
            let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
            let (step, start) = executions.pop().unwrap().as_start();
            if let Some(es) = step {
                executions.insert(0, es);
            }
            let cond = Self::fill_conditions(executions);
            Box::new(start.start_snapshot(cond, self.order, tx))
        }
    }

    pub fn indexable_range_str<'a, R>(&mut self, field: Field<T, String>, range: R)
    where
        R: RangeBounds<&'a str>,
    {
        let start = match range.start_bound() {
            Bound::Included(x) => Bound::Included(x.to_string()),
            Bound::Excluded(x) => Bound::Excluded(x.to_string()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(x) => Bound::Included(x.to_string()),
            Bound::Excluded(x) => Bound::Excluded(x.to_string()),
            Bound::Unbounded => Bound::Unbounded,
        };
        String::range(self, field, (start, end))
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilterBuilder<V>)
    where
        V: PersistentEmbedded + 'static,
    {
        let (conditions, order) = filter.components();
        self.add_order(EmbeddedOrder::new(field.clone(), order));
        self.add(EmbeddedFieldFilter::new(conditions, field))
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        self.add(QueryFilter::new(query, field))
    }

    pub fn ref_vec_query<V>(&mut self, field: Field<T, Vec<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        self.add(VecQueryFilter::new(query, field))
    }

    pub fn ref_option_query<V>(&mut self, field: Field<T, Option<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        self.add(OptionQueryFilter::new(query, field))
    }

    pub fn or(&mut self, builder: FilterBuilder<T>) {
        self.add(OrFilter::new(builder))
    }

    pub fn and(&mut self, builder: FilterBuilder<T>) {
        self.add(AndFilter::new(builder))
    }

    pub fn and_filter(&mut self, mut filters: FilterBuilder<T>) {
        for order in filters.order.order.drain(..) {
            self.order.order.push(order);
        }
        self.add(AndFilter::new(filters));
    }

    pub fn not(&mut self, builder: FilterBuilder<T>) {
        self.add(NotFilter::new(builder))
    }

    pub fn order<V: Ord + 'static + Scan<T>>(&mut self, field: Field<T, V>, order: Order) {
        self.add_order(FieldOrder::new(field, order))
    }
}
