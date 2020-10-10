use crate::internal::Field;
use crate::{EmbeddedFilter, Persistent, Ref, StructsyQuery};
use std::ops::{Bound, RangeBounds};

trait EmbeddedFilterBuilderStep {
    type Target;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool>;
}

struct ConditionFilter<V, T> {
    value: V,
    access: fn(&T) -> &V,
}

impl<V: PartialEq + Clone + 'static, T: 'static> ConditionFilter<V, T> {
    fn new(access: fn(&T) -> &V, value: V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(ConditionFilter { access, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for ConditionFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        Box::new(move |s| *(self.access)(s) == self.value)
    }
}

struct ConditionSingleFilter<V, T> {
    value: V,
    access: fn(&T) -> &Vec<V>,
}

impl<V: PartialEq + Clone + 'static, T: 'static> ConditionSingleFilter<V, T> {
    fn new(access: fn(&T) -> &Vec<V>, value: V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(ConditionSingleFilter { access, value })
    }
}

impl<V: PartialEq + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for ConditionSingleFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        Box::new(move |s| (self.access)(s).contains(&self.value))
    }
}

struct RangeConditionFilter<V, T> {
    value_start: Bound<V>,
    value_end: Bound<V>,
    access: fn(&T) -> &V,
}

impl<V: PartialOrd + Clone + 'static, T: 'static> RangeConditionFilter<V, T> {
    fn new(
        access: fn(&T) -> &V,
        value_start: Bound<V>,
        value_end: Bound<V>,
    ) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(RangeConditionFilter {
            access,
            value_start,
            value_end,
        })
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeConditionFilter<V, T> {
    type Target = T;

    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        Box::new(move |s| (self.value_start.clone(), self.value_end.clone()).contains((self.access)(s)))
    }
}

struct RangeSingleConditionFilter<V, T> {
    value_start: Bound<V>,
    value_end: Bound<V>,
    access: fn(&T) -> &Vec<V>,
}

impl<V: PartialOrd + Clone + 'static, T: 'static> RangeSingleConditionFilter<V, T> {
    fn new(
        access: fn(&T) -> &Vec<V>,
        value_start: Bound<V>,
        value_end: Bound<V>,
    ) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(RangeSingleConditionFilter {
            access,
            value_start,
            value_end,
        })
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeSingleConditionFilter<V, T> {
    type Target = T;

    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        Box::new(move |s| {
            for el in (self.access)(s) {
                if (self.value_start.clone(), self.value_end.clone()).contains(el) {
                    return true;
                }
            }
            false
        })
    }
}

struct RangeOptionConditionFilter<V, T> {
    value_start: Bound<Option<V>>,
    value_end: Bound<Option<V>>,
    access: fn(&T) -> &Option<V>,
}

impl<V: PartialOrd + Clone + 'static, T: 'static> RangeOptionConditionFilter<V, T> {
    fn new(
        access: fn(&T) -> &Option<V>,
        value_start: Bound<Option<V>>,
        value_end: Bound<Option<V>>,
    ) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(RangeOptionConditionFilter {
            access,
            value_start,
            value_end,
        })
    }
}
impl<V: PartialOrd + Clone + 'static, T: 'static> EmbeddedFilterBuilderStep for RangeOptionConditionFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let (b1, none_end) = match &self.value_start {
            Bound::Included(Some(x)) => (Bound::Included(x.clone()), false),
            Bound::Excluded(Some(x)) => (Bound::Excluded(x.clone()), false),
            Bound::Included(None) => (Bound::Unbounded, true),
            Bound::Excluded(None) => (Bound::Unbounded, true),
            Bound::Unbounded => (Bound::Unbounded, false),
        };
        let (b2, none_start) = match &self.value_end {
            Bound::Included(Some(x)) => (Bound::Included(x.clone()), false),
            Bound::Excluded(Some(x)) => (Bound::Excluded(x.clone()), false),
            Bound::Included(None) => (Bound::Unbounded, true),
            Bound::Excluded(None) => (Bound::Unbounded, true),
            Bound::Unbounded => (Bound::Unbounded, false),
        };
        let val = (b1, b2);
        let include_none = none_end | none_start;
        Box::new(move |s| {
            if let Some(z) = (self.access)(s) {
                val.contains(z)
            } else {
                include_none
            }
        })
    }
}

pub struct EmbeddedFieldFilter<V, T> {
    filter: EmbeddedFilter<V>,
    access: fn(&T) -> &V,
}

impl<V: 'static, T: 'static> EmbeddedFieldFilter<V, T> {
    fn new(filter: EmbeddedFilter<V>, access: fn(&T) -> &V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(EmbeddedFieldFilter { filter, access })
    }
}

impl<V: 'static, T: 'static> EmbeddedFilterBuilderStep for EmbeddedFieldFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let access = self.access;
        let mut condition = self.filter.condition();
        Box::new(move |r| condition((access)(r)))
    }
}

pub struct QueryFilter<V: Persistent + 'static, T> {
    query: StructsyQuery<V>,
    access: fn(&T) -> &Ref<V>,
}

impl<V: Persistent + 'static, T: 'static> QueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, access: fn(&T) -> &Ref<V>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(QueryFilter { query, access })
    }
}

impl<V: Persistent + 'static, T: 'static> EmbeddedFilterBuilderStep for QueryFilter<V, T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let st = self.query.structsy.clone();
        let mut condition = self.query.builder().condition();
        let access = self.access;
        Box::new(move |x| {
            let id = (access)(&x).clone();
            if let Some(r) = st.read(&id).unwrap_or(None) {
                condition(&id, &r)
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
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let mut conditions = Vec::new();
        for step in self.filters.steps {
            conditions.push(step.condition());
        }
        Box::new(move |x| {
            for condition in &mut conditions {
                if condition(x) {
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
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |r| condition(r))
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
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |r| !condition(r))
    }
}

pub trait SimpleEmbeddedCondition<T: 'static, V: Clone + PartialEq + 'static> {
    fn equal(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, V>, value: V) {
        filter.add(ConditionFilter::new(field.access, value))
    }

    fn contains(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter.add(ConditionSingleFilter::new(field.access, value))
    }

    fn is(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.add(ConditionFilter::new(field.access, Some(value)))
    }
}

pub trait EmbeddedRangeCondition<T: 'static, V: Clone + PartialOrd + 'static> {
    fn range<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, V>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter.add(RangeConditionFilter::new(field.access, start, end))
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter.add(RangeSingleConditionFilter::new(field.access, start, end))
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
        filter.add(RangeOptionConditionFilter::new(field.access, start, end))
    }
}
impl<T: 'static, V: Clone + PartialOrd + 'static> EmbeddedRangeCondition<T, V> for V {}

impl<T: 'static, V: Clone + PartialEq + 'static> SimpleEmbeddedCondition<T, V> for V {}

pub struct EmbeddedFilterBuilder<T> {
    steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = T>>>,
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
        EmbeddedFilterBuilder { steps: Vec::new() }
    }

    pub(crate) fn condition(self) -> Box<dyn FnMut(&T) -> bool> {
        let mut conditions = Vec::new();
        for filter in self.steps {
            conditions.push(filter.condition());
        }
        Box::new(move |t| {
            for condition in &mut conditions {
                if !condition(t) {
                    return false;
                }
            }
            return true;
        })
    }

    fn add(&mut self, filter: Box<dyn EmbeddedFilterBuilderStep<Target = T>>) {
        self.steps.push(filter);
    }

    pub fn simple_condition<V>(&mut self, field: Field<T, V>, value: V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::new(field.access, value))
    }

    pub fn simple_option_condition<V>(&mut self, field: Field<T, Option<V>>, value: Option<V>)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::<Option<V>, T>::new(field.access, value));
    }
    pub fn simple_vec_condition<V>(&mut self, field: Field<T, V>, value: V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::new(field.access, value))
    }

    pub fn simple_vec_single_condition<V>(&mut self, field: Field<T, Vec<V>>, value: V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionSingleFilter::new(field.access, value))
    }

    pub fn simple_option_single_condition<V>(&mut self, field: Field<T, Option<V>>, value: V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::<Option<V>, T>::new(field.access, Some(value)));
    }

    pub fn simple_range<V, R>(&mut self, field: Field<T, V>, range: R)
    where
        V: Clone + PartialOrd + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        self.add(RangeConditionFilter::new(field.access, start, end))
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
        self.add(RangeConditionFilter::new(field.access, start, end))
    }

    pub fn simple_vec_single_range<V, R>(&mut self, field: Field<T, Vec<V>>, range: R)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        self.add(RangeSingleConditionFilter::new(field.access, start, end))
    }

    pub fn simple_option_single_range<V, R>(&mut self, field: Field<T, Option<V>>, range: R)
    where
        V: EmbeddedRangeCondition<T, V> + PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        V::range_is(self, field, range)
    }

    pub fn simple_vec_range<V, R>(&mut self, field: Field<T, V>, range: R)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        // This may support index in future, but it does not now
        self.add(RangeConditionFilter::new(field.access, start, end))
    }

    pub fn simple_option_range<V, R>(&mut self, field: Field<T, Option<V>>, range: R)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<Option<V>>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        // This may support index in future, but it does not now
        self.add(RangeOptionConditionFilter::new(field.access, start, end))
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilter<V>)
    where
        V: 'static,
    {
        self.add(EmbeddedFieldFilter::new(filter, field.access))
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: StructsyQuery<V>)
    where
        V: Persistent + 'static,
    {
        self.add(QueryFilter::new(query, field.access))
    }

    pub fn ref_condition<V>(&mut self, field: Field<T, V>, value: V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::new(field.access, value))
    }

    pub fn ref_range<V, R>(&mut self, field: Field<T, V>, range: R)
    where
        V: Clone + PartialOrd + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        self.add(RangeConditionFilter::new(field.access, start, end))
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
}
