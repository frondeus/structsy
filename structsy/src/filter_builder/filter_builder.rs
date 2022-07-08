use crate::{
    filter_builder::{
        embedded_filter_builder::EmbeddedFilterBuilder,
        execution_iterator::{IterT, Source},
        execution_model::execute,
        execution_model::FieldsHolder,
        execution_step::ExecutionStep,
        filter_builder_step::{
            AndFilter, ConditionFilter, ConditionSingleFilter, EmbeddedFieldFilter, FilterBuilderStep, IndexFilter,
            NotFilter, OptionQueryFilter, OrFilter, QueryFilter, RangeConditionFilter, RangeIndexFilter,
            RangeOptionConditionFilter, RangeSingleConditionFilter, RangeSingleIndexFilter, VecQueryFilter,
        },
        plan_model::plan_from_query,
        query_model::{FilterHolder, FilterMode, Orders as OrdersModel, Query, SolveQueryValue, SolveSimpleQueryValue},
        reader::{Reader, ReaderIterator},
        start::{ScanStartStep, StartStep},
        ValueCompare, ValueRange,
    },
    index::RangeInstanceIter,
    internal::{Description, EmbeddedDescription, Field},
    Order, Persistent, PersistentEmbedded, Ref,
};
use std::{
    ops::{Bound, RangeBounds},
    rc::Rc,
};

pub(crate) struct Item<P> {
    pub(crate) id: Ref<P>,
    pub(crate) record: P,
}

impl<P> Item<P> {
    pub(crate) fn new((id, record): (Ref<P>, P)) -> Self {
        Self { id, record }
    }
}

fn clone_bound_ref<X: Clone>(bound: &Bound<&X>) -> Bound<X> {
    match bound {
        Bound::Included(x) => Bound::Included((*x).clone()),
        Bound::Excluded(x) => Bound::Excluded((*x).clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub trait RangeCondition<
    T: Persistent + 'static,
    V: PersistentEmbedded + Clone + PartialOrd + 'static + SolveQueryValue + ValueRange,
>
{
    fn range<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, V>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter
            .get_filter()
            .add_field_range(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
        if V::indexable() {
            if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                filter.add(RangeIndexFilter::new(index_name, field, (start, end)))
            } else {
                filter.add(RangeConditionFilter::new(field, (start, end)))
            }
        } else {
            filter.add(RangeConditionFilter::new(field, (start, end)))
        }
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter
            .get_filter()
            .add_field_range_contains(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
        if V::indexable() {
            if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                filter.add(RangeSingleIndexFilter::new(index_name, field, (start, end)))
            } else {
                filter.add(RangeSingleConditionFilter::new(field, (start, end)))
            }
        } else {
            filter.add(RangeSingleConditionFilter::new(field, (start, end)))
        }
    }

    fn range_is<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, Option<V>>, range: R) {
        filter
            .get_filter()
            .add_field_range_is(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
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
pub trait SimpleCondition<
    T: Persistent + 'static,
    V: PersistentEmbedded + ValueCompare + Clone + PartialEq + 'static + SolveQueryValue,
>
{
    fn equal(filter: &mut FilterBuilder<T>, field: Field<T, V>, value: V) {
        filter
            .get_filter()
            .add_field_equal(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
        if V::indexable() {
            if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                filter.add(IndexFilter::new(index_name, value))
            } else {
                filter.add(ConditionFilter::new(field, value))
            }
        } else {
            filter.add(ConditionFilter::new(field, value))
        }
    }

    fn contains(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter
            .get_filter()
            .add_field_contains(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
        if V::indexable() {
            if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                filter.add(IndexFilter::new(index_name, value))
            } else {
                filter.add(ConditionSingleFilter::new(field, value))
            }
        } else {
            filter.add(ConditionSingleFilter::new(field, value))
        }
    }

    fn is(filter: &mut FilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.get_filter().add_field_is(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
        if V::indexable() {
            if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                filter.add(IndexFilter::new(index_name, value))
            } else {
                filter.add(ConditionFilter::new(field, Some(value)))
            }
        } else {
            filter.add(ConditionFilter::new(field, Some(value)))
        }
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
        impl<T: Persistent + 'static> SimpleCondition<T, $t>  for $t {}

        impl<T: Persistent + 'static> SimpleCondition<T, Vec<$t>> for Vec<$t> {}

        impl<T: Persistent + 'static> RangeCondition<T, Vec<$t>> for Vec<$t>{}

        impl<T: Persistent + 'static> SimpleCondition<T, Option<$t>> for Option<$t> {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, value: Option<$t>) {
                filter.get_filter().add_field_equal(
                    Rc::new(field.clone()),
                    value.clone()
                );
                filter.get_fields().add_field(field.clone());
                if let (Some(index_name), Some(v)) = (FilterBuilder::<T>::is_indexed(field.name), value.clone()) {
                    filter.add(IndexFilter::new(index_name, v));
                } else {
                    filter.add(ConditionFilter::<Option<$t>, T>::new(field, value));
                }
            }
        }

        impl<T: Persistent + 'static> RangeCondition<T, $t> for $t {}

        impl< T: Persistent + 'static> RangeCondition<T, Option<$t>> for Option<$t> {
            fn range<R: RangeBounds<Option<$t>>>(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, range: R) {
                filter.get_filter().add_field_range(
                    Rc::new(field.clone()),
                    (&range.start_bound(),&range.end_bound())
                );
                filter.get_fields().add_field_ord(field.clone());
                let start = clone_bound_ref(&range.start_bound());
                let end = clone_bound_ref(&range.end_bound());
                // This may support index in future, but it does not now
                filter.add(RangeOptionConditionFilter::new(field, (start, end)))
            }
        }

        )+
    };
}

index_conditions!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, String);

impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static + SolveQueryValue>
    SimpleCondition<T, V> for V
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static + SolveQueryValue>
    RangeCondition<T, V> for V
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static + SolveSimpleQueryValue>
    SimpleCondition<T, Vec<V>> for Vec<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static + SolveSimpleQueryValue>
    RangeCondition<T, Vec<V>> for Vec<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static + SolveSimpleQueryValue>
    SimpleCondition<T, Option<V>> for Option<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static + SolveSimpleQueryValue>
    RangeCondition<T, Option<V>> for Option<V>
{
}

impl<P: Persistent + 'static, V: PersistentEmbedded + 'static> Scan<P> for V {
    fn scan<'a>(field: &Field<P, Self>, reader: Reader<'a>, order: &Order) -> Option<IterT<'a, P>> {
        if let Some(index_name) = FilterBuilder::<P>::is_indexed(field.name) {
            if let Ok(iter) = Self::finder().find_range(reader, &index_name, (Bound::Unbounded, Bound::Unbounded)) {
                let it: Box<dyn ReaderIterator<Item = (Ref<P>, P)>> = if order == &Order::Desc {
                    Box::new(RangeInstanceIter::new(iter).reader_rev())
                } else {
                    Box::new(RangeInstanceIter::new(iter))
                };
                Some(it)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn is_indexed_impl(field: &Field<P, Self>) -> bool {
        if let Some(_) = FilterBuilder::<P>::is_indexed(field.name) {
            true
        } else {
            false
        }
    }
}

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
    fn scan_reader<'a>(&self, reader: Reader<'a>) -> Option<IterT<'a, P>>;
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
    fn scan_reader<'a>(&self, reader: Reader<'a>) -> Option<IterT<'a, P>> {
        Scan::scan(&self.field, reader, &self.order)
    }
    fn is_indexed(&self) -> bool {
        Scan::is_indexed_impl(&self.field)
    }
}
pub trait Scan<P>: Sized {
    fn scan<'a>(_filed: &Field<P, Self>, _reader: Reader<'a>, _order: &Order) -> Option<IterT<'a, P>> {
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
    fn scan_reader<'a>(&self, _reader: Reader<'a>) -> Option<IterT<'a, P>> {
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
    pub(crate) fn scan<'a>(self, reader: Reader<'a>) -> (Option<Box<dyn BufferedExection<T>>>, Option<IterT<'a, T>>) {
        if self.order.is_empty() {
            return (None, None);
        }
        let mut orders = self.order;
        let first_entry = orders.remove(0);
        let scan = first_entry.scan_reader(reader);
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
}

pub struct FilterBuilder<T> {
    pub(crate) steps: Vec<Box<dyn FilterBuilderStep<Target = T>>>,
    order: Orders<T>,
    filter: FilterHolder,
    fields_holder: FieldsHolder<T>,
    orders: Vec<OrdersModel>,
}
impl<T: 'static> Default for FilterBuilder<T> {
    fn default() -> Self {
        FilterBuilder::new()
    }
}

impl<T: 'static> FilterBuilder<T> {
    pub(crate) fn move_out_filter(&mut self) -> FilterHolder {
        std::mem::replace(&mut self.filter, FilterHolder::new(FilterMode::And))
    }
    pub(crate) fn move_out_fields_holder(&mut self) -> FieldsHolder<T> {
        std::mem::replace(&mut self.fields_holder, FieldsHolder::default())
    }
    pub(crate) fn move_out_orders(&mut self) -> Vec<OrdersModel> {
        std::mem::replace(&mut self.orders, Vec::new())
    }
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder {
            steps: Vec::new(),
            order: Orders { order: Vec::new() },
            filter: FilterHolder::new(FilterMode::And),
            fields_holder: Default::default(),
            orders: Vec::new(),
        }
    }

    fn add<F: FilterBuilderStep<Target = T> + 'static>(&mut self, filter: F) {
        self.steps.push(Box::new(filter));
    }

    fn add_order<O: ScanOrderStep<T> + 'static>(&mut self, order: O) {
        self.order.order.push(Box::new(order))
    }

    fn get_filter(&mut self) -> &mut FilterHolder {
        &mut self.filter
    }
    fn get_fields(&mut self) -> &mut FieldsHolder<T> {
        &mut self.fields_holder
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

    pub fn finish<'a>(self, mut reader_inst: Reader<'a>) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            let start = Box::new(ScanStartStep::new());
            let cond = Self::fill_conditions(Vec::new());
            Box::new(start.start_reader(cond, self.order, reader_inst))
        } else {
            let reader = &mut reader_inst;
            let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
            executions.sort_by_key(|x| x.get_score());
            let (step, start) = executions.pop().unwrap().as_start();
            if let Some(es) = step {
                executions.insert(0, es);
            }
            let cond = Self::fill_conditions(executions);
            Box::new(start.start_reader(cond, self.order, reader_inst))
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
        <String as RangeCondition<T, String>>::range(self, field, (start, end))
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilterBuilder<V>)
    where
        V: PersistentEmbedded + 'static,
    {
        let (conditions, order, filter, orders_model, fields_holder) = filter.components();

        self.get_fields().add_nested_field(field.clone(), fields_holder);
        self.filter.add_field_embedded(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_embedded(Rc::new(field.clone()), orders_model));
        self.add_order(EmbeddedOrder::new(field.clone(), order));
        self.add(EmbeddedFieldFilter::new(conditions, field))
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        let FilterBuilder {
            steps,
            order,
            filter,
            fields_holder,
            orders,
        } = query;
        //TODO: merge field holder
        self.get_fields().add_field_ref(field.clone(), fields_holder.clone());
        self.filter.add_field_ref_query_equal(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_query_equal(Rc::new(field.clone()), orders));
        self.add(QueryFilter::new(
            FilterBuilder {
                steps,
                order,
                filter: FilterHolder::new(FilterMode::And),
                fields_holder,
                orders: vec![],
            },
            field,
        ))
    }

    pub fn ref_vec_query<V>(&mut self, field: Field<T, Vec<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        let FilterBuilder {
            steps,
            order,
            filter,
            fields_holder,
            orders,
        } = query;
        //TODO: merge field holder
        self.get_fields()
            .add_field_vec_ref(field.clone(), fields_holder.clone());
        self.filter.add_field_ref_query_contains(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_query_contains(Rc::new(field.clone()), orders));
        self.add(VecQueryFilter::new(
            FilterBuilder {
                steps,
                order,
                filter: FilterHolder::new(FilterMode::And),
                fields_holder,
                orders: vec![],
            },
            field,
        ))
    }

    pub fn ref_option_query<V>(&mut self, field: Field<T, Option<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        //TODO: merge field holder
        let FilterBuilder {
            steps,
            order,
            filter,
            fields_holder,
            orders,
        } = query;
        self.get_fields()
            .add_field_option_ref(field.clone(), fields_holder.clone());
        self.orders
            .push(OrdersModel::new_query_is(Rc::new(field.clone()), orders));
        self.filter.add_field_ref_query_is(Rc::new(field.clone()), filter);
        self.add(OptionQueryFilter::new(
            FilterBuilder {
                steps,
                order,
                filter: FilterHolder::new(FilterMode::And),
                fields_holder,
                orders: vec![],
            },
            field,
        ))
    }

    pub fn or(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            steps,
            order,
            mut filter,
            orders,
            fields_holder,
        } = builder;
        filter.mode = FilterMode::Or;
        self.fields_holder.merge(fields_holder);
        self.filter.add_group(filter);
        self.orders.extend(orders);
        self.add(OrFilter::new(FilterBuilder {
            steps,
            order,
            filter: FilterHolder::new(FilterMode::Or),
            fields_holder: FieldsHolder::default(),
            orders: vec![],
        }));
    }

    pub fn and(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            steps,
            order,
            mut filter,
            fields_holder,
            orders,
        } = builder;
        filter.mode = FilterMode::And;
        self.fields_holder.merge(fields_holder);
        self.filter.add_group(filter);
        self.orders.extend(orders);
        self.add(AndFilter::new(FilterBuilder {
            steps,
            order,
            filter: FilterHolder::new(FilterMode::And),
            fields_holder: FieldsHolder::default(),
            orders: vec![],
        }))
    }

    pub fn and_filter(&mut self, filters: FilterBuilder<T>) {
        let FilterBuilder {
            steps,
            order,
            mut filter,
            fields_holder,
            orders,
        } = filters;
        filter.mode = FilterMode::And;
        self.fields_holder.merge(fields_holder);
        self.order.order.extend(order.order);
        self.filter.add_group(filter);
        self.orders.extend(orders);
        self.add(AndFilter::new(FilterBuilder {
            steps,
            order: Orders { order: vec![] },
            filter: FilterHolder::new(FilterMode::And),
            fields_holder: FieldsHolder::default(),
            orders: vec![],
        }));
    }

    pub fn not(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            steps,
            order,
            mut filter,
            fields_holder,
            orders,
        } = builder;
        filter.mode = FilterMode::Not;
        self.filter.add_group(filter);
        self.fields_holder.merge(fields_holder);
        self.orders.extend(orders);
        self.add(NotFilter::new(FilterBuilder {
            steps,
            order,
            filter: FilterHolder::new(FilterMode::Not),
            fields_holder: FieldsHolder::default(),
            orders: vec![],
        }))
    }

    pub fn order<V: ValueRange + Ord + 'static + Scan<T>>(&mut self, field: Field<T, V>, order: Order) {
        self.orders
            .push(OrdersModel::new_field(Rc::new(field.clone()), order.clone()));
        self.get_fields().add_field_ord(field.clone());
        self.add_order(FieldOrder::new(field, order))
    }
}
