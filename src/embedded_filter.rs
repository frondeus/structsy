use crate::internal::Field;
use crate::{EmbeddedFilter, Persistent, PersistentEmbedded, Ref, StructsyQuery};
use std::ops::{Bound, RangeBounds};

trait EmbeddedFilterBuilderStep {
    type Target;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool>;
}

struct ConditionFilter<V: PartialEq + Clone + 'static, T: PersistentEmbedded + 'static> {
    value: V,
    access: fn(&T) -> &V,
}

impl<V: PartialEq + Clone + 'static, T: PersistentEmbedded + 'static> ConditionFilter<V, T> {
    fn new(access: fn(&T) -> &V, value: V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(ConditionFilter { access, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep
    for ConditionFilter<V, T>
{
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        Box::new(move |s| *(self.access)(s) == self.value)
    }
}

struct ConditionSingleFilter<V: PartialEq + Clone + 'static, T: PersistentEmbedded + 'static> {
    value: V,
    access: fn(&T) -> &Vec<V>,
}

impl<V: PartialEq + Clone + 'static, T: PersistentEmbedded + 'static> ConditionSingleFilter<V, T> {
    fn new(access: fn(&T) -> &Vec<V>, value: V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(ConditionSingleFilter { access, value })
    }
}

impl<V: PartialEq + Clone + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep
    for ConditionSingleFilter<V, T>
{
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        Box::new(move |s| (self.access)(s).contains(&self.value))
    }
}

struct RangeConditionFilter<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> {
    value_start: Bound<V>,
    value_end: Bound<V>,
    access: fn(&T) -> &V,
}

impl<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> RangeConditionFilter<V, T> {
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
impl<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep
    for RangeConditionFilter<V, T>
{
    type Target = T;

    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        Box::new(move |s| (self.value_start.clone(), self.value_end.clone()).contains((self.access)(s)))
    }
}

struct RangeSingleConditionFilter<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> {
    value_start: Bound<V>,
    value_end: Bound<V>,
    access: fn(&T) -> &Vec<V>,
}

impl<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> RangeSingleConditionFilter<V, T> {
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
impl<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep
    for RangeSingleConditionFilter<V, T>
{
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

struct RangeOptionConditionFilter<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> {
    value_start: Bound<V>,
    value_end: Bound<V>,
    access: fn(&T) -> &Option<V>,
    include_none: bool,
}

impl<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> RangeOptionConditionFilter<V, T> {
    fn new(
        access: fn(&T) -> &Option<V>,
        value_start: Bound<V>,
        value_end: Bound<V>,
        include_none: bool,
    ) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(RangeOptionConditionFilter {
            access,
            value_start,
            value_end,
            include_none,
        })
    }
}
impl<V: PartialOrd + Clone + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep
    for RangeOptionConditionFilter<V, T>
{
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        Box::new(move |s| {
            if let Some(z) = (self.access)(s) {
                (self.value_start.clone(), self.value_end.clone()).contains(z)
            } else {
                self.include_none
            }
        })
    }
}

pub struct EmbeddedFieldFilter<V: PersistentEmbedded, T: PersistentEmbedded> {
    filter: EmbeddedFilter<V>,
    access: fn(&T) -> &V,
}

impl<V: PersistentEmbedded + 'static, T: PersistentEmbedded + 'static> EmbeddedFieldFilter<V, T> {
    fn new(filter: EmbeddedFilter<V>, access: fn(&T) -> &V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(EmbeddedFieldFilter { filter, access })
    }
}

impl<V: PersistentEmbedded + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep
    for EmbeddedFieldFilter<V, T>
{
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let access = self.access;
        let mut condition = self.filter.condition();
        Box::new(move |r| condition((access)(r)))
    }
}

pub struct QueryFilter<V: Persistent + 'static, T: PersistentEmbedded> {
    query: StructsyQuery<V>,
    access: fn(&T) -> &Ref<V>,
}

impl<V: Persistent + 'static, T: PersistentEmbedded + 'static> QueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, access: fn(&T) -> &Ref<V>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(QueryFilter { query, access })
    }
}

impl<V: Persistent + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep for QueryFilter<V, T> {
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

pub struct OrFilter<T: PersistentEmbedded> {
    filters: EmbeddedFilterBuilder<T>,
}

impl<T: PersistentEmbedded + 'static> OrFilter<T> {
    fn new(filters: EmbeddedFilterBuilder<T>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(OrFilter { filters })
    }
}

impl<T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep for OrFilter<T> {
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

pub struct AndFilter<T: PersistentEmbedded> {
    filters: EmbeddedFilterBuilder<T>,
}

impl<T: PersistentEmbedded + 'static> AndFilter<T> {
    fn new(filters: EmbeddedFilterBuilder<T>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(AndFilter { filters })
    }
}

impl<T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep for AndFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |r| condition(r))
    }
}

pub struct NotFilter<T: PersistentEmbedded> {
    filters: EmbeddedFilterBuilder<T>,
}

impl<T: PersistentEmbedded + 'static> NotFilter<T> {
    fn new(filters: EmbeddedFilterBuilder<T>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(NotFilter { filters })
    }
}

impl<T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep for NotFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |r| !condition(r))
    }
}

pub struct EmbeddedFilterBuilder<T: PersistentEmbedded> {
    steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = T>>>,
}

fn clone_bound_ref<X: Clone>(bound: &Bound<&X>) -> Bound<X> {
    match bound {
        Bound::Included(x) => Bound::Included((*x).clone()),
        Bound::Excluded(x) => Bound::Excluded((*x).clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl<T: PersistentEmbedded + 'static> EmbeddedFilterBuilder<T> {
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
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        // This may support index in future, but it does not now
        self.add(RangeOptionConditionFilter::new(field.access, start, end, false))
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
        let (start, none_end) = match range.start_bound() {
            Bound::Included(Some(x)) => (Bound::Included(x.clone()), false),
            Bound::Excluded(Some(x)) => (Bound::Excluded(x.clone()), false),
            Bound::Included(None) => (Bound::Unbounded, true),
            Bound::Excluded(None) => (Bound::Unbounded, true),
            Bound::Unbounded => (Bound::Unbounded, false),
        };
        let (end, none_start) = match range.end_bound() {
            Bound::Included(Some(x)) => (Bound::Included(x.clone()), false),
            Bound::Excluded(Some(x)) => (Bound::Excluded(x.clone()), false),
            Bound::Included(None) => (Bound::Unbounded, true),
            Bound::Excluded(None) => (Bound::Unbounded, true),
            Bound::Unbounded => (Bound::Unbounded, false),
        };
        // This may support index in future, but it does not now
        self.add(RangeOptionConditionFilter::new(
            field.access,
            start,
            end,
            none_end || none_start,
        ))
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilter<V>)
    where
        V: PersistentEmbedded + 'static,
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
