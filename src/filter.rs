use crate::{
    index::{find, find_range},
    Persistent, Ref, Structsy,
};
use persy::IndexType;
use std::ops::{Bound, RangeBounds};

trait FilterBuilderStep {
    type Target;
    fn score(&mut self, _structsy: &Structsy) -> u32 {
        std::u32::MAX
    }
    fn get_score(&self) -> u32 {
        std::u32::MAX
    }
    fn filter(
        &mut self,
        structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>;
    fn first(&mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>;
}

struct IndexFilter<V: IndexType + 'static, T: Persistent + 'static> {
    index_name: String,
    index_value: V,
    data: Option<Vec<(Ref<T>, T)>>,
}

impl<V: IndexType + 'static, T: Persistent + 'static> IndexFilter<V, T> {
    fn new(index_name: String, index_value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(IndexFilter {
            index_name,
            index_value,
            data: None,
        })
    }
}

impl<V: IndexType + 'static, T: Persistent + 'static> FilterBuilderStep for IndexFilter<V, T> {
    type Target = T;
    fn score(&mut self, structsy: &Structsy) -> u32 {
        self.data = find(&structsy, &self.index_name, &self.index_value).ok();
        self.get_score()
    }
    fn get_score(&self) -> u32 {
        if let Some(x) = &self.data {
            x.len() as u32
        } else {
            std::u32::MAX
        }
    }
    fn filter(
        &mut self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let data = std::mem::replace(&mut self.data, None);
        if let Some(found) = data {
            let to_filter = found.into_iter().map(|(r, _)| r).collect::<Vec<_>>();
            Box::new(iter.filter(move |(r, _x)| to_filter.contains(r)))
        } else {
            iter
        }
    }
    fn first(&mut self, _structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let data = std::mem::replace(&mut self.data, None);
        if let Some(found) = data {
            Box::new(found.into_iter())
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct ConditionSingleFilter<V: PartialEq + Clone + 'static, T: Persistent + 'static> {
    value: V,
    access: fn(&T) -> &Vec<V>,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionSingleFilter<V, T> {
    fn new(access: fn(&T) -> &Vec<V>, value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(ConditionSingleFilter { access, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionSingleFilter<V, T> {
    type Target = T;
    fn filter(
        &mut self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let value = self.value.clone();
        let access = self.access.clone();
        Box::new(iter.filter(move |(_, x)| (access)(x).contains(&value)))
    }
    fn first(&mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = structsy.scan::<T>() {
            self.filter(structsy, Box::new(found))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct ConditionFilter<V: PartialEq + Clone + 'static, T: Persistent + 'static> {
    value: V,
    access: fn(&T) -> &V,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionFilter<V, T> {
    fn new(access: fn(&T) -> &V, value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(ConditionFilter { access, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionFilter<V, T> {
    type Target = T;
    fn filter(
        &mut self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let val = self.value.clone();
        let access = self.access.clone();
        Box::new(iter.filter(move |(_, x)| *(access)(x) == val))
    }

    fn first(&mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = structsy.scan::<T>() {
            self.filter(structsy, Box::new(found))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct RangeSingleConditionFilter<V: PartialOrd + Clone + 'static, T: Persistent + 'static> {
    value_start: Bound<V>,
    value_end: Bound<V>,
    access: fn(&T) -> &Vec<V>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeSingleConditionFilter<V, T> {
    fn new(
        access: fn(&T) -> &Vec<V>,
        value_start: Bound<V>,
        value_end: Bound<V>,
    ) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeSingleConditionFilter {
            access,
            value_start,
            value_end,
        })
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeSingleConditionFilter<V, T> {
    type Target = T;
    fn filter(
        &mut self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        let access = self.access.clone();
        Box::new(iter.filter(move |(_, x)| {
            for el in (access)(x) {
                if val.contains(el) {
                    return true;
                }
            }
            false
        }))
    }

    fn first(&mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = structsy.scan::<T>() {
            self.filter(structsy, Box::new(found))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct RangeConditionFilter<V: PartialOrd + Clone + 'static, T: Persistent + 'static> {
    value_start: Bound<V>,
    value_end: Bound<V>,
    access: fn(&T) -> &V,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeConditionFilter<V, T> {
    fn new(access: fn(&T) -> &V, value_start: Bound<V>, value_end: Bound<V>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeConditionFilter {
            access,
            value_start,
            value_end,
        })
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeConditionFilter<V, T> {
    type Target = T;
    fn filter(
        &mut self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        let access = self.access.clone();
        Box::new(iter.filter(move |(_, x)| val.contains((access)(x))))
    }

    fn first(&mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = structsy.scan::<T>() {
            self.filter(structsy, Box::new(found))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct RangeIndexFilter<V: IndexType + 'static, T: Persistent + 'static> {
    index_name: String,
    value_start: Bound<V>,
    value_end: Bound<V>,
    data: Option<Box<dyn Iterator<Item = (Ref<T>, T)>>>,
    score: Option<u32>,
}

impl<V: IndexType + 'static, T: Persistent + 'static> RangeIndexFilter<V, T> {
    fn new(index_name: String, value_start: Bound<V>, value_end: Bound<V>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeIndexFilter {
            index_name,
            value_start,
            value_end,
            data: None,
            score: None,
        })
    }
}

impl<V: IndexType + 'static, T: Persistent + 'static> FilterBuilderStep for RangeIndexFilter<V, T> {
    type Target = T;
    fn score(&mut self, structsy: &Structsy) -> u32 {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        if let Ok(iter) = find_range(&structsy, &self.index_name, val) {
            let mut no_key = iter.map(|(r, e, _)| (r, e));
            let mut i = 0;
            let mut vec = Vec::new();
            while let Some(el) = no_key.next() {
                vec.push(el);
                i += 1;
                if i == 1000 {
                    break;
                }
            }
            self.score = Some(vec.len() as u32);
            self.data = Some(Box::new(vec.into_iter().chain(no_key)));
        }
        self.get_score()
    }
    fn get_score(&self) -> u32 {
        if let Some(x) = self.score {
            x
        } else {
            std::u32::MAX
        }
    }
    fn filter(
        &mut self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let data = std::mem::replace(&mut self.data, None);
        if let Some(found) = data {
            let to_filter = found.into_iter().map(|(r, _)| r).collect::<Vec<_>>();
            Box::new(iter.filter(move |(r, _x)| to_filter.contains(r)))
        } else {
            iter
        }
    }
    fn first(&mut self, _structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let data = std::mem::replace(&mut self.data, None);
        if let Some(found) = data {
            Box::new(found.into_iter())
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct RangeOptionConditionFilter<V: PartialOrd + Clone + 'static, T: Persistent + 'static> {
    value_start: Bound<V>,
    value_end: Bound<V>,
    access: fn(&T) -> &Option<V>,
    include_none: bool,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeOptionConditionFilter<V, T> {
    fn new(
        access: fn(&T) -> &Option<V>,
        value_start: Bound<V>,
        value_end: Bound<V>,
        include_none: bool,
    ) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeOptionConditionFilter {
            access,
            value_start,
            value_end,
            include_none,
        })
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeOptionConditionFilter<V, T> {
    type Target = T;
    fn filter(
        &mut self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        let access = self.access.clone();
        let include_none = self.include_none;
        Box::new(iter.filter(move |(_, x)| {
            if let Some(z) = (access)(x) {
                val.contains(z)
            } else {
                include_none
            }
        }))
    }

    fn first(&mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = structsy.scan::<T>() {
            self.filter(structsy, Box::new(found))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

pub struct FilterBuilder<T: Persistent + 'static> {
    steps: Vec<Box<dyn FilterBuilderStep<Target = T>>>,
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

impl<T: Persistent + 'static> FilterBuilder<T> {
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder { steps: Vec::new() }
    }

    fn is_indexed(name: &str) -> Option<String> {
        let desc = T::get_description();
        if let Some(f) = desc.get_field(name) {
            if f.indexed.is_some() {
                Some(format!("{}.{}", desc.name, f.name))
            } else {
                None
            }
        } else {
            panic!("field with name:'{}' not found", name)
        }
    }

    pub(crate) fn merge(&mut self, other: FilterBuilder<T>) {
        self.steps.extend(other.steps);
    }

    pub fn finish(mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<T>, T)>> {
        for x in &mut self.steps {
            x.score(structsy);
        }
        self.steps.sort_by_key(|x| x.get_score());
        let mut res = None;
        for mut s in self.steps.into_iter() {
            res = Some(if let Some(prev) = res {
                s.filter(structsy, prev)
            } else {
                s.first(structsy)
            });
        }
        res.expect("there is every time at least one element")
    }

    fn add(&mut self, filter: Box<dyn FilterBuilderStep<Target = T>>) {
        self.steps.push(filter);
    }

    pub fn indexable_condition<V>(&mut self, name: &str, value: V, access: fn(&T) -> &V)
    where
        V: IndexType + PartialEq + 'static,
    {
        if let Some(index_name) = Self::is_indexed(name) {
            self.add(IndexFilter::new(index_name, value))
        } else {
            self.add(ConditionFilter::new(access, value))
        }
    }

    pub fn simple_condition<V>(&mut self, _name: &str, value: V, access: fn(&T) -> &V)
    where
        V: PartialEq + Clone + 'static,
    {
        self.add(ConditionFilter::new(access, value))
    }

    pub fn indexable_vec_condition<V>(&mut self, _name: &str, value: Vec<V>, access: fn(&T) -> &Vec<V>)
    where
        V: IndexType + PartialEq + 'static,
    {
        //TODO: support lookup in index
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

    pub fn indexable_vec_single_condition<V>(&mut self, name: &str, value: V, access: fn(&T) -> &Vec<V>)
    where
        V: IndexType + PartialEq + 'static,
    {
        if let Some(index_name) = Self::is_indexed(name) {
            self.add(IndexFilter::new(index_name, value))
        } else {
            self.add(ConditionSingleFilter::new(access, value))
        }
    }

    pub fn indexable_option_single_condition<V>(&mut self, name: &str, value: V, access: fn(&T) -> &Option<V>)
    where
        V: IndexType + PartialEq + 'static,
    {
        self.indexable_option_condition(name, Some(value), access);
    }

    pub fn simple_option_single_condition<V>(&mut self, _name: &str, value: V, access: fn(&T) -> &Option<V>)
    where
        V: IndexType + PartialEq + 'static,
    {
        self.add(ConditionFilter::<Option<V>, T>::new(access, Some(value)));
    }

    pub fn indexable_option_condition<V>(&mut self, name: &str, value: Option<V>, access: fn(&T) -> &Option<V>)
    where
        V: IndexType + PartialEq + 'static,
    {
        if let Some(index_name) = Self::is_indexed(name) {
            if let Some(v) = value {
                self.add(IndexFilter::new(index_name, v));
            } else {
                self.add(ConditionFilter::<Option<V>, T>::new(access, value));
            }
        } else {
            self.add(ConditionFilter::<Option<V>, T>::new(access, value));
        }
    }

    pub fn indexable_range<V, R>(&mut self, name: &str, range: R, access: fn(&T) -> &V)
    where
        V: IndexType + PartialOrd + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        if let Some(index_name) = Self::is_indexed(name) {
            self.add(RangeIndexFilter::new(index_name, start, end))
        } else {
            self.add(RangeConditionFilter::new(access, start, end))
        }
    }

    pub fn indexable_vec_single_range<V, R>(&mut self, name: &str, range: R, access: fn(&T) -> &Vec<V>)
    where
        V: IndexType + PartialOrd + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        if let Some(index_name) = Self::is_indexed(name) {
            self.add(RangeIndexFilter::new(index_name, start, end))
        } else {
            self.add(RangeSingleConditionFilter::new(access, start, end))
        }
    }

    pub fn indexable_option_single_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &Option<V>)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        // This may support index in future, but it does not now
        self.add(RangeOptionConditionFilter::new(access, start, end, false))
    }

    pub fn indexable_vec_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &V)
    where
        V: PartialOrd + Clone + 'static,
        R: RangeBounds<V>,
    {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        // This may support index in future, but it does not now
        self.add(RangeConditionFilter::new(access, start, end))
    }

    pub fn indexable_option_range<V, R>(&mut self, _name: &str, range: R, access: fn(&T) -> &Option<V>)
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

pub trait FieldConditionType<T: Persistent, V, F> {
    fn add_to_filter(&self, filter_builder: &mut FilterBuilder<T>, v: V);
}
