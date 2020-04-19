use crate::{EmbeddedFilter, Persistent, PersistentEmbedded, Ref, Structsy, StructsyQuery};
use std::any::Any;
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;

pub(crate) struct EmbPrevInst<T> {
    record: Rc<Box<dyn Source<T>>>,
    prev: Option<Box<dyn Any>>,
}

impl<T: 'static> EmbPrevInst<T> {
    fn new(record: Rc<Box<dyn Source<T>>>, prev: Option<Box<dyn Any>>) -> EmbPrevInst<T> {
        EmbPrevInst { record, prev }
    }
    fn extract(prev: Option<Box<dyn Any>>) -> Option<(Box<dyn Source<T>>, Option<Box<dyn Any>>)> {
        if let Some(b) = prev {
            if let Ok(pre) = b.downcast::<EmbPrevInst<T>>() {
                let EmbPrevInst { record, prev } = *pre;
                if let Ok(rec) = Rc::try_unwrap(record) {
                    Some((rec, prev))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

pub(crate) struct EmbProvider<T, P> {
    inst: Rc<Box<dyn Source<T>>>,
    access: fn(&T) -> &P,
}

impl<T, P> EmbProvider<T, P> {
    pub(crate) fn new(x: Rc<Box<dyn Source<T>>>, access: fn(&T) -> &P) -> EmbProvider<T, P> {
        EmbProvider {
            inst: x,
            access: access,
        }
    }
}
impl<T, P> Source<P> for EmbProvider<T, P> {
    fn source(&self) -> &P {
        (self.access)(&self.inst.source())
    }
}

pub(crate) trait Source<P> {
    fn source(&self) -> &P;
}

pub(crate) type EIter<P> = Box<dyn Iterator<Item = (Box<dyn Source<P>>, Option<Box<dyn Any>>)>>;

trait EmbeddedFilterBuilderStep {
    type Target;
    fn filter(&mut self, structsy: &Structsy, iter: EIter<Self::Target>) -> EIter<Self::Target>;
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
    fn filter(&mut self, _structsy: &Structsy, iter: EIter<Self::Target>) -> EIter<Self::Target> {
        let value = self.value.clone();
        let access = self.access;
        Box::new(iter.filter(move |(s, _)| *(access)(s.source()) == value))
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
    fn filter(&mut self, _structsy: &Structsy, iter: EIter<Self::Target>) -> EIter<Self::Target> {
        let value = self.value.clone();
        let access = self.access;
        Box::new(iter.filter(move |(s, _)| (access)(s.source()).contains(&value)))
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

    fn filter(&mut self, _structsy: &Structsy, iter: EIter<Self::Target>) -> EIter<Self::Target> {
        let value_start = self.value_start.clone();
        let value_end = self.value_end.clone();
        let access = self.access;
        Box::new(iter.filter(move |(s, _)| (value_start.clone(), value_end.clone()).contains((access)(s.source()))))
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

    fn filter(&mut self, _structsy: &Structsy, iter: EIter<Self::Target>) -> EIter<Self::Target> {
        let value_start = self.value_start.clone();
        let value_end = self.value_end.clone();
        let access = self.access;
        Box::new(iter.filter(move |(s, _)| {
            for el in (access)(s.source()) {
                if (value_start.clone(), value_end.clone()).contains(el) {
                    return true;
                }
            }
            false
        }))
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
    fn filter(&mut self, _structsy: &Structsy, iter: EIter<Self::Target>) -> EIter<Self::Target> {
        let value_start = self.value_start.clone();
        let value_end = self.value_end.clone();
        let access = self.access;
        let include_none = self.include_none;
        Box::new(iter.filter(move |(s, _)| {
            if let Some(z) = (access)(s.source()) {
                (value_start.clone(), value_end.clone()).contains(z)
            } else {
                include_none
            }
        }))
    }
}

pub struct EmbeddedFieldFilter<V: PersistentEmbedded, T: PersistentEmbedded> {
    filter: Option<EmbeddedFilter<V>>,
    access: fn(&T) -> &V,
}

impl<V: PersistentEmbedded + 'static, T: PersistentEmbedded + 'static> EmbeddedFieldFilter<V, T> {
    fn new(filter: EmbeddedFilter<V>, access: fn(&T) -> &V) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(EmbeddedFieldFilter {
            filter: Some(filter),
            access,
        })
    }
}

impl<V: PersistentEmbedded + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep
    for EmbeddedFieldFilter<V, T>
{
    type Target = T;
    fn filter(&mut self, structsy: &Structsy, iter: EIter<Self::Target>) -> EIter<Self::Target> {
        let filter = std::mem::replace(&mut self.filter, None);
        let access = self.access;
        let nested_filter = Box::new(iter.filter_map(move |(x, prev)| {
            let rcx = Rc::new(x);
            let provider = EmbProvider::new(rcx.clone(), access);
            let new_prev: Box<dyn Any> = Box::new(EmbPrevInst::new(rcx, prev));
            let prov: Box<dyn Source<V>> = Box::new(provider);
            Some((prov, Some(new_prev)))
        }));
        let filterd = filter.expect("the filter is here").filter(structsy, nested_filter);
        Box::new(filterd.map(|(_, prev)| prev).filter_map(EmbPrevInst::<T>::extract))
    }
}

pub struct QueryFilter<V: Persistent + 'static, T: PersistentEmbedded> {
    query: Option<StructsyQuery<V>>,
    access: fn(&T) -> &Ref<V>,
}

impl<V: Persistent + 'static, T: PersistentEmbedded + 'static> QueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, access: fn(&T) -> &Ref<V>) -> Box<dyn EmbeddedFilterBuilderStep<Target = T>> {
        Box::new(QueryFilter {
            query: Some(query),
            access,
        })
    }
}

impl<V: Persistent + 'static, T: PersistentEmbedded + 'static> EmbeddedFilterBuilderStep for QueryFilter<V, T> {
    type Target = T;
    fn filter(&mut self, structsy: &Structsy, iter: EIter<Self::Target>) -> EIter<Self::Target> {
        let query = std::mem::replace(&mut self.query, None);
        let access = self.access;
        let st = structsy.clone();
        let nested_filter = iter.filter_map(move |(x, prev)| {
            let id = (access)(&x.source()).clone();
            if let Some(r) = st.read(&id).unwrap_or(None) {
                let new_prev: Box<dyn Any> = Box::new(EmbPrevInst::new(Rc::new(x), prev));
                Some((id.clone(), r, Some(new_prev)))
            } else {
                None
            }
        });
        Box::new(
            query
                .unwrap()
                .builder()
                .check(structsy, Box::new(nested_filter))
                .map(|(_, _, prev)| prev)
                .filter_map(EmbPrevInst::<T>::extract),
        )
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

    pub(crate) fn filter(self, structsy: &Structsy, iter: EIter<T>) -> EIter<T> {
        let mut new_iter = iter;
        for mut filter in self.steps {
            new_iter = filter.filter(structsy, new_iter);
        }
        new_iter
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

    pub fn simple_option_condition<V>(&mut self, _name: &str, value: Option<V>, access: fn(&T) -> &Option<V>)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::<Option<V>, T>::new(access, value));
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

    pub fn simple_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &V)
    where
        V: Clone + PartialOrd + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        self.add(RangeConditionFilter::new(access, start, end))
    }

    pub fn simple_vec_single_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &Vec<V>)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        self.add(RangeSingleConditionFilter::new(access, start, end))
    }

    pub fn simple_option_single_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &Option<V>)
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

    pub fn simple_persistent_embedded<V>(&mut self, _name: &str, filter: EmbeddedFilter<V>, access: fn(&T) -> &V)
    where
        V: PersistentEmbedded + 'static,
    {
        self.add(EmbeddedFieldFilter::new(filter, access))
    }

    pub fn ref_query<V>(&mut self, _name: &str, query: StructsyQuery<V>, access: fn(&T) -> &Ref<V>)
    where
        V: Persistent + 'static,
    {
        self.add(QueryFilter::new(query, access))
    }

    pub fn ref_condition<V>(&mut self, _name: &str, value: V, access: fn(&T) -> &V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::new(access, value))
    }

    pub fn ref_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &V)
    where
        V: Clone + PartialOrd + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        self.add(RangeConditionFilter::new(access, start, end))
    }
}
