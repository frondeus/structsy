use crate::PersistentEmbedded;
use std::ops::{Bound, RangeBounds};

trait EmbeddedFilterBuilderStep {
    type Target;
    fn filter(&mut self, to_filter: &Self::Target) -> bool;
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
    fn filter(&mut self, to_filter: &Self::Target) -> bool {
        *(self.access)(to_filter) == self.value
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
    fn filter(&mut self, to_filter: &Self::Target) -> bool {
        (self.access)(to_filter).contains(&self.value)
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

    fn filter(&mut self, to_filter: &Self::Target) -> bool {
        (self.value_start.clone(), self.value_end.clone()).contains((self.access)(to_filter))
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

    fn filter(&mut self, to_filter: &Self::Target) -> bool {
        for el in (self.access)(to_filter) {
            if (self.value_start.clone(), self.value_end.clone()).contains(el) {
                return true;
            }
        }
        false
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
    fn filter(&mut self, to_filter: &Self::Target) -> bool {
        if let Some(z) = (self.access)(to_filter) {
            (self.value_start.clone(), self.value_end.clone()).contains(z)
        } else {
            self.include_none
        }
    }
}

pub struct EmbeddedFilterBuilder<T: PersistentEmbedded + 'static> {
    steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = T>>>,
}

fn clone_bound<X: Clone>(bound: &Bound<X>) -> Bound<X> {
    match bound {
        Bound::Included(x) => Bound::Included(x.clone()),
        Bound::Excluded(x) => Bound::Excluded(x.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
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

    fn add(&mut self, filter: Box<dyn EmbeddedFilterBuilderStep<Target = T>>) {
        self.steps.push(filter);
    }

    pub fn simple_condition<V>(&mut self, _name: &str, value: V, access: fn(&T) -> &V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::new(access, value))
    }

    pub fn simple_vec_condition<V>(&mut self, _name: &str, value: V, access: fn(&T) -> &V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::new(access, value))
    }

    pub fn simple_vec_single_condition<V>(&mut self, _name: &str, value: V, access: fn(&T) -> &Vec<V>)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionSingleFilter::new(access, value))
    }

    pub fn simple_option_single_condition<V>(&mut self, _name: &str, value: V, access: fn(&T) -> &Option<V>)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::<Option<V>, T>::new(access, Some(value)));
    }

    pub fn simple_vec_single_range<V, R>(&mut self, name: &str, range: R, access: fn(&T) -> &Vec<V>)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        self.add(RangeSingleConditionFilter::new(access, start, end))
    }

    pub fn simpl_option_single_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &Option<V>)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        // This may support index in future, but it does not now
        self.add(RangeOptionConditionFilter::new(access, start, end, false))
    }

    pub fn simple_vec_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &V)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        // This may support index in future, but it does not now
        self.add(RangeConditionFilter::new(access, start, end))
    }

    pub fn simple_option_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &Option<V>)
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
            access,
            start,
            end,
            none_end || none_start,
        ))
    }
}
