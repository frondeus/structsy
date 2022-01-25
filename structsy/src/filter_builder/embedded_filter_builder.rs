use crate::filter_builder::{
    filter_builder::{EmbeddedOrder, FieldOrder, FilterBuilder, Item, OrderStep},
    query_model::{FilterHolder, FilterMode, Orders as OrdersModel, SolveQueryValue},
    reader::Reader,
};
use crate::internal::Field;
use crate::{Order, Persistent, Ref};
use std::{
    ops::{Bound, RangeBounds},
    rc::Rc,
};

pub(crate) trait EmbeddedFilterBuilderStep {
    type Target;
    fn condition(self: Box<Self>, reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool>;
}

struct ConditionFilter<V, T> {
    value: V,
    field: Field<T, V>,
}

impl<V: PartialEq + Clone + 'static, T: 'static> ConditionFilter<V, T> {
    fn new(field: Field<T, V>, value: V) -> Self {
        ConditionFilter { field, value }
    }
}
impl<V: PartialEq + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for ConditionFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>, _reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        Box::new(move |s, _| *(self.field.access)(s) == self.value)
    }
}

struct ConditionSingleFilter<V, T> {
    value: V,
    field: Field<T, Vec<V>>,
}

impl<V: PartialEq + Clone + 'static, T: 'static> ConditionSingleFilter<V, T> {
    fn new(field: Field<T, Vec<V>>, value: V) -> Self {
        ConditionSingleFilter { field, value }
    }
}

impl<V: PartialEq + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for ConditionSingleFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>, _reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        Box::new(move |s, _| (self.field.access)(s).contains(&self.value))
    }
}
struct RangeConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, V>,
}

impl<V: PartialOrd + Clone + 'static, T: 'static> RangeConditionFilter<V, T> {
    fn new(field: Field<T, V>, values: (Bound<V>, Bound<V>)) -> Self {
        RangeConditionFilter { field, values }
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeConditionFilter<V, T> {
    type Target = T;

    fn condition(self: Box<Self>, _reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        Box::new(move |s, _| self.values.contains((self.field.access)(s)))
    }
}

struct RangeSingleConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, Vec<V>>,
}

impl<V: PartialOrd + Clone + 'static, T: 'static> RangeSingleConditionFilter<V, T> {
    fn new(field: Field<T, Vec<V>>, values: (Bound<V>, Bound<V>)) -> Self {
        RangeSingleConditionFilter { field, values }
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeSingleConditionFilter<V, T> {
    type Target = T;

    fn condition(self: Box<Self>, _reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        Box::new(move |s, _| {
            for el in (self.field.access)(s) {
                if self.values.contains(el) {
                    return true;
                }
            }
            false
        })
    }
}

struct RangeOptionConditionFilter<V, T> {
    values: (Bound<Option<V>>, Bound<Option<V>>),
    field: Field<T, Option<V>>,
}

impl<V: PartialOrd + Clone + 'static, T: 'static> RangeOptionConditionFilter<V, T> {
    fn new(field: Field<T, Option<V>>, values: (Bound<Option<V>>, Bound<Option<V>>)) -> Self {
        RangeOptionConditionFilter { field, values }
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeOptionConditionFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>, _reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        let (b1, none_end) = match &self.values.start_bound() {
            Bound::Included(Some(x)) => (Bound::Included(x.clone()), false),
            Bound::Excluded(Some(x)) => (Bound::Excluded(x.clone()), false),
            Bound::Included(None) => (Bound::Unbounded, true),
            Bound::Excluded(None) => (Bound::Unbounded, true),
            Bound::Unbounded => (Bound::Unbounded, false),
        };
        let (b2, none_start) = match &self.values.end_bound() {
            Bound::Included(Some(x)) => (Bound::Included(x.clone()), false),
            Bound::Excluded(Some(x)) => (Bound::Excluded(x.clone()), false),
            Bound::Included(None) => (Bound::Unbounded, true),
            Bound::Excluded(None) => (Bound::Unbounded, true),
            Bound::Unbounded => (Bound::Unbounded, false),
        };
        let val = (b1, b2);
        let include_none = none_end | none_start;
        Box::new(move |s, _| {
            if let Some(z) = (self.field.access)(s) {
                val.contains(z)
            } else {
                include_none
            }
        })
    }
}

pub struct EmbeddedFieldFilter<V, T> {
    steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = V>>>,
    field: Field<T, V>,
}

impl<V: 'static, T: 'static> EmbeddedFieldFilter<V, T> {
    fn new(steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = V>>>, field: Field<T, V>) -> Self {
        EmbeddedFieldFilter { steps, field }
    }
}

impl<V: 'static, T: 'static> EmbeddedFilterBuilderStep for EmbeddedFieldFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>, reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        let access = self.field.access;
        let condition = build_condition(self.steps, reader);
        Box::new(move |r, reader| condition((access)(r), reader))
    }
}

pub struct QueryFilter<V: Persistent + 'static, T> {
    query: FilterBuilder<V>,
    field: Field<T, Ref<V>>,
}

impl<V: Persistent + 'static, T: 'static> QueryFilter<V, T> {
    fn new(query: FilterBuilder<V>, field: Field<T, Ref<V>>) -> Self {
        QueryFilter { query, field }
    }
}

impl<V: Persistent + 'static, T: 'static> EmbeddedFilterBuilderStep for QueryFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>, reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        let condition = self.query.fill_conditions_step(reader);
        let access = self.field.access;
        Box::new(move |x, reader| {
            let id = (access)(x).clone();
            if let Some(r) = reader.read(&id).unwrap_or(None) {
                condition.check(&Item::new((id.clone(), r)), reader)
            } else {
                false
            }
        })
    }
}

pub struct OrFilter<T> {
    filters: EmbeddedFilterBuilder<T>,
}

impl<T: 'static> OrFilter<T> {
    fn new(filters: EmbeddedFilterBuilder<T>) -> Self {
        OrFilter { filters }
    }
}

impl<T: 'static> EmbeddedFilterBuilderStep for OrFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>, reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        let mut conditions = Vec::new();
        for step in self.filters.steps {
            conditions.push(step.condition(reader));
        }
        Box::new(move |x, reader| {
            for condition in &conditions {
                if condition(x, reader) {
                    return true;
                }
            }
            false
        })
    }
}

pub struct AndFilter<T> {
    filters: EmbeddedFilterBuilder<T>,
}

impl<T: 'static> AndFilter<T> {
    fn new(filters: EmbeddedFilterBuilder<T>) -> Self {
        AndFilter { filters }
    }
}

impl<T: 'static> EmbeddedFilterBuilderStep for AndFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>, reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        let (steps, _, _, _) = self.filters.components();
        let condition = build_condition(steps, reader);
        Box::new(move |r, reader| condition(r, reader))
    }
}

pub struct NotFilter<T> {
    filters: EmbeddedFilterBuilder<T>,
}

impl<T: 'static> NotFilter<T> {
    fn new(filters: EmbeddedFilterBuilder<T>) -> Self {
        NotFilter { filters }
    }
}

impl<T: 'static> EmbeddedFilterBuilderStep for NotFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>, reader: &mut Reader) -> Box<dyn Fn(&Self::Target, &mut Reader) -> bool> {
        let (steps, _, _, _) = self.filters.components();
        let condition = build_condition(steps, reader);
        Box::new(move |r, reader| !condition(r, reader))
    }
}

pub trait SimpleEmbeddedCondition<T: 'static, V: Clone + PartialEq + SolveQueryValue + 'static> {
    fn equal(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, V>, value: V) {
        filter
            .get_filter()
            .add_field_equal(Rc::new(field.clone()), value.clone());
        filter.add(ConditionFilter::new(field, value))
    }

    fn contains(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter
            .get_filter()
            .add_field_contains(Rc::new(field.clone()), value.clone());
        filter.add(ConditionSingleFilter::new(field, value))
    }

    fn is(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.get_filter().add_field_is(Rc::new(field.clone()), value.clone());
        filter.add(ConditionFilter::new(field, Some(value)))
    }
}

pub trait EmbeddedRangeCondition<T: 'static, V: Clone + SolveQueryValue + PartialOrd + 'static> {
    fn range<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, V>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter
            .get_filter()
            .add_field_range(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.add(RangeConditionFilter::new(field, (start, end)))
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter
            .get_filter()
            .add_field_range_contains(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.add(RangeSingleConditionFilter::new(field, (start, end)))
    }

    fn range_is<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Option<V>>, range: R) {
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
        filter
            .get_filter()
            .add_field_range_is(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        // This may support index in future, but it does not now
        filter.add(RangeOptionConditionFilter::new(field, (start, end)))
    }
}
impl<T: 'static, V: Clone + PartialOrd + 'static + SolveQueryValue> EmbeddedRangeCondition<T, V> for V {}

impl<T: 'static, V: Clone + PartialEq + 'static + SolveQueryValue> SimpleEmbeddedCondition<T, V> for V {}

pub struct EmbeddedFilterBuilder<T> {
    steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = T>>>,
    order: Vec<Box<dyn OrderStep<T>>>,
    filter: FilterHolder,
    orders: Vec<OrdersModel>,
}
impl<T> Default for EmbeddedFilterBuilder<T> {
    fn default() -> Self {
        EmbeddedFilterBuilder::<T>::new()
    }
}

fn clone_bound_ref<X: Clone>(bound: &Bound<&X>) -> Bound<X> {
    match bound {
        Bound::Included(x) => Bound::Included((*x).clone()),
        Bound::Excluded(x) => Bound::Excluded((*x).clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
impl<T> EmbeddedFilterBuilder<T> {
    pub fn new() -> EmbeddedFilterBuilder<T> {
        EmbeddedFilterBuilder {
            steps: Vec::new(),
            order: Vec::new(),
            filter: FilterHolder::new(FilterMode::And),
            orders: Vec::new(),
        }
    }
    fn get_filter(&mut self) -> &mut FilterHolder {
        &mut self.filter
    }
}
pub(crate) fn build_condition<T: 'static>(
    steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = T>>>,
    reader: &mut Reader,
) -> Box<dyn Fn(&T, &mut Reader) -> bool> {
    let mut conditions = Vec::new();
    for filter in steps {
        conditions.push(filter.condition(reader));
    }

    Box::new(move |t, reader| {
        for condition in &conditions {
            if !condition(t, reader) {
                return false;
            }
        }
        true
    })
}

impl<T: 'static> EmbeddedFilterBuilder<T> {
    pub(crate) fn components(
        self,
    ) -> (
        Vec<Box<dyn EmbeddedFilterBuilderStep<Target = T>>>,
        Vec<Box<dyn OrderStep<T>>>,
        FilterHolder,
        Vec<OrdersModel>,
    ) {
        (self.steps, self.order, self.filter, self.orders)
    }

    fn add<F: EmbeddedFilterBuilderStep<Target = T> + 'static>(&mut self, filter: F) {
        self.steps.push(Box::new(filter));
    }

    pub fn simple_range_str<'a, R>(&mut self, field: Field<T, String>, range: R)
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
        self.add(RangeConditionFilter::new(field, (start, end)))
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilterBuilder<V>)
    where
        V: 'static,
    {
        let (conditions, orders, filter, orders_model) = filter.components();

        self.filter.add_field_embedded(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_embedded(Rc::new(field.clone()), orders_model));
        self.order.push(Box::new(EmbeddedOrder::new_emb(field.clone(), orders)));
        self.add(EmbeddedFieldFilter::new(conditions, field))
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        self.add(QueryFilter::new(query, field))
    }

    pub fn or(&mut self, filters: EmbeddedFilterBuilder<T>) {
        let EmbeddedFilterBuilder {
            steps,
            order,
            mut filter,
            orders,
        } = filters;
        filter.mode = FilterMode::Or;
        self.filter.add_group(filter);
        self.orders.extend(orders);
        self.add(OrFilter::new(EmbeddedFilterBuilder {
            steps,
            order,
            filter: FilterHolder::new(FilterMode::Or),
            orders: vec![],
        }))
    }

    pub fn and(&mut self, filters: EmbeddedFilterBuilder<T>) {
        let EmbeddedFilterBuilder {
            steps,
            order,
            mut filter,
            orders,
        } = filters;
        filter.mode = FilterMode::And;
        self.filter.add_group(filter);
        self.orders.extend(orders);
        self.add(AndFilter::new(EmbeddedFilterBuilder {
            steps,
            order,
            filter: FilterHolder::new(FilterMode::And),
            orders: vec![],
        }))
    }

    pub fn not(&mut self, filters: EmbeddedFilterBuilder<T>) {
        let EmbeddedFilterBuilder {
            steps,
            order,
            mut filter,
            orders,
        } = filters;
        filter.mode = FilterMode::Not;
        self.filter.add_group(filter);
        self.orders.extend(orders);
        self.add(NotFilter::new(EmbeddedFilterBuilder {
            steps,
            order,
            filter: FilterHolder::new(FilterMode::And),
            orders: vec![],
        }))
    }
    pub fn order<V: Ord + 'static>(&mut self, field: Field<T, V>, order: Order) {
        self.orders
            .push(OrdersModel::new_field(Rc::new(field.clone()), order.clone()));
        self.order.push(Box::new(FieldOrder::new_emb(field, order)))
    }
}
