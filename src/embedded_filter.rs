use crate::filter::{EmbeddedOrder, FieldOrder, Item, Order, OrderStep, Reader};
use crate::internal::Field;
use crate::{EmbeddedFilter, Persistent, Ref, StructsyQuery};
use std::ops::{Bound, RangeBounds};

trait EmbeddedFilterBuilderStep {
    type Target;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool>;
}

struct ConditionFilter<V, T> {
    value: V,
    field: Field<T, V>,
}

impl<V: PartialEq + Clone + 'static, T: 'static> ConditionFilter<V, T> {
    fn new(field: Field<T, V>, value: V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(ConditionFilter { field, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for ConditionFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
        Box::new(move |s, _| *(self.field.access)(s) == self.value)
    }
}

struct ConditionSingleFilter<V, T> {
    value: V,
    field: Field<T, Vec<V>>,
}

impl<V: PartialEq + Clone + 'static, T: 'static> ConditionSingleFilter<V, T> {
    fn new(field: Field<T, Vec<V>>, value: V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(ConditionSingleFilter { field, value })
    }
}

impl<V: PartialEq + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for ConditionSingleFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
        Box::new(move |s, _| (self.field.access)(s).contains(&self.value))
    }
}
struct RangeConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, V>,
}

impl<V: PartialOrd + Clone + 'static, T: 'static> RangeConditionFilter<V, T> {
    fn new(field: Field<T, V>, values: (Bound<V>, Bound<V>)) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(RangeConditionFilter { field, values })
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeConditionFilter<V, T> {
    type Target = T;

    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
        Box::new(move |s, _| self.values.contains((self.field.access)(s)))
    }
}

struct RangeSingleConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, Vec<V>>,
}

impl<V: PartialOrd + Clone + 'static, T: 'static> RangeSingleConditionFilter<V, T> {
    fn new(field: Field<T, Vec<V>>, values: (Bound<V>, Bound<V>)) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(RangeSingleConditionFilter { field, values })
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeSingleConditionFilter<V, T> {
    type Target = T;

    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
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
    fn new(
        field: Field<T, Option<V>>,
        values: (Bound<Option<V>>, Bound<Option<V>>),
    ) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(RangeOptionConditionFilter { field, values })
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeOptionConditionFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
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
    condition: Box<dyn FnMut(&V, &mut Reader) -> bool>,
    field: Field<T, V>,
}

impl<V: 'static, T: 'static> EmbeddedFieldFilter<V, T> {
    fn new(
        condition: Box<dyn FnMut(&V, &mut Reader) -> bool>,
        field: Field<T, V>,
    ) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(EmbeddedFieldFilter { condition, field })
    }
}

impl<V: 'static, T: 'static> EmbeddedFilterBuilderStep for EmbeddedFieldFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
        let access = self.field.access;
        let mut condition = self.condition;
        Box::new(move |r, reader| condition((access)(r), reader))
    }
}

pub struct QueryFilter<V: Persistent + 'static, T> {
    query: StructsyQuery<V>,
    field: Field<T, Ref<V>>,
}

impl<V: Persistent + 'static, T: 'static> QueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, field: Field<T, Ref<V>>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(QueryFilter { query, field })
    }
}

impl<V: Persistent + 'static, T: 'static> EmbeddedFilterBuilderStep for QueryFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
        let st = self.query.structsy.clone();
        let mut condition = self.query.builder().fill_conditions_step(&mut Reader::Structsy(st));
        let access = self.field.access;
        Box::new(move |x, reader| {
            let id = (access)(&x).clone();
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
    fn new(filters: EmbeddedFilterBuilder<T>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(OrFilter { filters })
    }
}

impl<T: 'static> EmbeddedFilterBuilderStep for OrFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
        let mut conditions = Vec::new();
        for step in self.filters.steps {
            conditions.push(step.condition());
        }
        Box::new(move |x, reader| {
            for condition in &mut conditions {
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
    fn new(filters: EmbeddedFilterBuilder<T>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(AndFilter { filters })
    }
}

impl<T: 'static> EmbeddedFilterBuilderStep for AndFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
        let (mut condition, _) = self.filters.components();
        Box::new(move |r, reader| condition(r, reader))
    }
}

pub struct NotFilter<T> {
    filters: EmbeddedFilterBuilder<T>,
}

impl<T: 'static> NotFilter<T> {
    fn new(filters: EmbeddedFilterBuilder<T>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(NotFilter { filters })
    }
}

impl<T: 'static> EmbeddedFilterBuilderStep for NotFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target, &mut Reader) -> bool> {
        let (mut condition, _) = self.filters.components();
        Box::new(move |r, reader| !condition(r, reader))
    }
}

pub trait SimpleEmbeddedCondition<T: 'static, V: Clone + PartialEq + 'static> {
    fn equal(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, V>, value: V) {
        filter.add(ConditionFilter::new(field, value))
    }

    fn contains(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter.add(ConditionSingleFilter::new(field, value))
    }

    fn is(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.add(ConditionFilter::new(field, Some(value)))
    }
}

pub trait EmbeddedRangeCondition<T: 'static, V: Clone + PartialOrd + 'static> {
    fn range<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, V>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter.add(RangeConditionFilter::new(field, (start, end)))
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
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
        // This may support index in future, but it does not now
        filter.add(RangeOptionConditionFilter::new(field, (start, end)))
    }
}
impl<T: 'static, V: Clone + PartialOrd + 'static> EmbeddedRangeCondition<T, V> for V {}

impl<T: 'static, V: Clone + PartialEq + 'static> SimpleEmbeddedCondition<T, V> for V {}

pub struct EmbeddedFilterBuilder<T> {
    steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = T>>>,
    order: Vec<Box<dyn OrderStep<T>>>,
}

fn clone_bound_ref<X: Clone>(bound: &Bound<&X>) -> Bound<X> {
    match bound {
        Bound::Included(x) => Bound::Included((*x).clone()),
        Bound::Excluded(x) => Bound::Excluded((*x).clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl<T: 'static> EmbeddedFilterBuilder<T> {
    pub fn new() -> EmbeddedFilterBuilder<T> {
        EmbeddedFilterBuilder {
            steps: Vec::new(),
            order: Vec::new(),
        }
    }

    pub(crate) fn components(self) -> (Box<dyn FnMut(&T, &mut Reader) -> bool>, Vec<Box<dyn OrderStep<T>>>) {
        let mut conditions = Vec::new();
        for filter in self.steps {
            conditions.push(filter.condition());
        }
        (
            Box::new(move |t, reader| {
                for condition in &mut conditions {
                    if !condition(t, reader) {
                        return false;
                    }
                }
                return true;
            }),
            self.order,
        )
    }

    fn add(&mut self, filter: Box<dyn EmbeddedFilterBuilderStep<Target = T>>) {
        self.steps.push(filter);
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

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilter<V>)
    where
        V: 'static,
    {
        let (conditions, orders) = filter.components();
        self.order.push(EmbeddedOrder::new(field.clone(), orders));
        self.add(EmbeddedFieldFilter::new(conditions, field))
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: StructsyQuery<V>)
    where
        V: Persistent + 'static,
    {
        self.add(QueryFilter::new(query, field))
    }

    pub fn or(&mut self, filters: EmbeddedFilter<T>) {
        self.add(OrFilter::new(filters.filter()))
    }

    pub fn and(&mut self, filters: EmbeddedFilter<T>) {
        self.add(AndFilter::new(filters.filter()))
    }

    pub fn not(&mut self, filters: EmbeddedFilter<T>) {
        self.add(NotFilter::new(filters.filter()))
    }
    pub fn order<V: Ord + 'static>(&mut self, field: Field<T, V>, order: Order) {
        self.order.push(FieldOrder::new(field, order))
    }
}
