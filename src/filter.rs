use crate::{
    index::{find, find_range, find_range_tx, find_tx},
    queries::{StructsyAndFilter, StructsyNotFilter, StructsyOrFilter},
    EmbeddedFilter, OwnedSytx, Persistent, PersistentEmbedded, Ref, Structsy, StructsyQuery, StructsyTx,
};
use persy::IndexType;
use std::ops::{Bound, RangeBounds};

pub(crate) type FIter<'a, P> = Box<dyn Iterator<Item = (Ref<P>, P)> + 'a>;

trait FilterBuilderStep {
    type Target: Persistent + 'static;
    fn score(&mut self, _structsy: &Structsy) -> u32 {
        std::u32::MAX
    }

    fn score_tx<'a>(&mut self, tx: &'a mut OwnedSytx) -> (u32, &'a mut OwnedSytx) {
        (std::u32::MAX, tx)
    }

    fn get_score(&self) -> u32 {
        std::u32::MAX
    }

    fn first<'a>(self: Box<Self>, structsy: &Structsy) -> FIter<'a, Self::Target> {
        let mut condition = self.condition();
        if let Ok(found) = structsy.scan::<Self::Target>() {
            Box::new(found.filter(move |(id, r)| condition(id, r)))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }

    fn first_tx<'a>(self: Box<Self>, tx: &'a mut OwnedSytx) -> FIter<'a, Self::Target> {
        let mut condition = self.condition();
        if let Ok(found) = StructsyTx::scan::<Self::Target>(tx) {
            Box::new(found.filter(move |(id, r)| condition(id, r)))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool>;
}

struct IndexFilter<V: IndexType, T: Persistent> {
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
    fn score_tx<'a>(&mut self, tx: &'a mut OwnedSytx) -> (u32, &'a mut OwnedSytx) {
        self.data = find_tx(tx, &self.index_name, &self.index_value).ok();
        (self.get_score(), tx)
    }
    fn get_score(&self) -> u32 {
        if let Some(x) = &self.data {
            x.len() as u32
        } else {
            std::u32::MAX
        }
    }
    fn first<'a>(self: Box<Self>, _structsy: &Structsy) -> FIter<'a, Self::Target> {
        if let Some(found) = self.data {
            Box::new(found.into_iter())
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        if let Some(found) = self.data {
            let to_filter = found.into_iter().map(|(r, _)| r).collect::<Vec<_>>();
            Box::new(move |id, _| to_filter.contains(id))
        } else {
            Box::new(|_, _| true)
        }
    }
}

struct ConditionSingleFilter<V: PartialEq + Clone, T: Persistent> {
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
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| (self.access)(x).contains(&self.value))
    }
}

struct ConditionFilter<V: PartialEq + Clone, T: Persistent> {
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
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| *(self.access)(x) == self.value)
    }
}

struct RangeSingleConditionFilter<V: PartialOrd + Clone, T: Persistent> {
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
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        Box::new(move |_, x| {
            for el in (self.access)(x) {
                if val.contains(el) {
                    return true;
                }
            }
            false
        })
    }
}

struct RangeConditionFilter<V: PartialOrd + Clone, T: Persistent> {
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
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        Box::new(move |_, x| val.contains((self.access)(x)))
    }
}

struct RangeIndexFilter<V: IndexType, T: Persistent> {
    index_name: String,
    access: fn(&T) -> &V,
    value_start: Bound<V>,
    value_end: Bound<V>,
    data: Option<Box<dyn Iterator<Item = (Ref<T>, T)>>>,
    score: Option<u32>,
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> RangeIndexFilter<V, T> {
    fn new(
        index_name: String,
        access: fn(&T) -> &V,
        value_start: Bound<V>,
        value_end: Bound<V>,
    ) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeIndexFilter {
            index_name,
            access,
            value_start,
            value_end,
            data: None,
            score: None,
        })
    }
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> FilterBuilderStep for RangeIndexFilter<V, T> {
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

    fn score_tx<'a>(&mut self, tx: &'a mut OwnedSytx) -> (u32, &'a mut OwnedSytx) {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        if let Ok(iter) = find_range_tx(tx, &self.index_name, val) {
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
            if i < 1000 {
                self.score = Some(vec.len() as u32);
                self.data = Some(Box::new(vec.into_iter()));
            }
        }
        (self.get_score(), tx)
    }

    fn get_score(&self) -> u32 {
        if let Some(x) = self.score {
            x
        } else {
            std::u32::MAX
        }
    }
    fn first<'a>(self: Box<Self>, _structsy: &Structsy) -> FIter<'a, Self::Target> {
        if let Some(found) = self.data {
            Box::new(found.into_iter())
        } else {
            unreachable!()
        }
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        if let Some(found) = self.data {
            let to_filter = found.into_iter().map(|(r, _)| r).collect::<Vec<_>>();
            Box::new(move |r, _x| to_filter.contains(r))
        } else {
            let b1 = clone_bound(&self.value_start);
            let b2 = clone_bound(&self.value_end);
            let val = (b1, b2);
            Box::new(move |_, x| val.contains((self.access)(x)))
        }
    }
}

struct RangeSingleIndexFilter<V: IndexType, T: Persistent> {
    index_name: String,
    access: fn(&T) -> &Vec<V>,
    value_start: Bound<V>,
    value_end: Bound<V>,
    data: Option<Box<dyn Iterator<Item = (Ref<T>, T)>>>,
    score: Option<u32>,
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> RangeSingleIndexFilter<V, T> {
    fn new(
        index_name: String,
        access: fn(&T) -> &Vec<V>,
        value_start: Bound<V>,
        value_end: Bound<V>,
    ) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeSingleIndexFilter {
            index_name,
            access,
            value_start,
            value_end,
            data: None,
            score: None,
        })
    }
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> FilterBuilderStep for RangeSingleIndexFilter<V, T> {
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

    fn score_tx<'a>(&mut self, tx: &'a mut OwnedSytx) -> (u32, &'a mut OwnedSytx) {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        if let Ok(iter) = find_range_tx(tx, &self.index_name, val) {
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
            if i < 1000 {
                self.score = Some(vec.len() as u32);
                self.data = Some(Box::new(vec.into_iter()));
            }
        }
        (self.get_score(), tx)
    }

    fn get_score(&self) -> u32 {
        if let Some(x) = self.score {
            x
        } else {
            std::u32::MAX
        }
    }
    fn first<'a>(self: Box<Self>, _structsy: &Structsy) -> FIter<'a, Self::Target> {
        if let Some(found) = self.data {
            Box::new(found.into_iter())
        } else {
            unreachable!()
        }
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        if let Some(found) = self.data {
            let to_filter = found.into_iter().map(|(r, _)| r).collect::<Vec<_>>();
            Box::new(move |r, _x| to_filter.contains(r))
        } else {
            let b1 = clone_bound(&self.value_start);
            let b2 = clone_bound(&self.value_end);
            let val = (b1, b2);
            Box::new(move |_, x| {
                for el in (self.access)(x) {
                    if val.contains(el) {
                        return true;
                    }
                }
                false
            })
        }
    }
}

struct RangeOptionConditionFilter<V: PartialOrd + Clone, T: Persistent> {
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
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        Box::new(move |_, x| {
            if let Some(z) = (self.access)(x) {
                val.contains(z)
            } else {
                self.include_none
            }
        })
    }
}

pub struct EmbeddedFieldFilter<V: PersistentEmbedded, T: Persistent> {
    filter: EmbeddedFilter<V>,
    access: fn(&T) -> &V,
}

impl<V: PersistentEmbedded + 'static, T: Persistent + 'static> EmbeddedFieldFilter<V, T> {
    fn new(filter: EmbeddedFilter<V>, access: fn(&T) -> &V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(EmbeddedFieldFilter { filter, access })
    }
}

impl<V: PersistentEmbedded + 'static, T: Persistent + 'static> FilterBuilderStep for EmbeddedFieldFilter<V, T> {
    type Target = T;

    fn condition<'a>(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let mut condition = self.filter.condition();
        let access = self.access;
        Box::new(move |_, x| condition((access)(x)))
    }
}

pub struct QueryFilter<V: Persistent + 'static, T: Persistent> {
    query: StructsyQuery<V>,
    access: fn(&T) -> &Ref<V>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> QueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, access: fn(&T) -> &Ref<V>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(QueryFilter { query, access })
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for QueryFilter<V, T> {
    type Target = T;
    fn condition<'a>(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let st = self.query.structsy.clone();
        let mut condition = self.query.builder().condition();
        let access = self.access;
        Box::new(move |_, x| {
            let id = (access)(&x).clone();
            if let Some(r) = st.read(&id).unwrap_or(None) {
                condition(&id, &r)
            } else {
                false
            }
        })
    }
}

pub struct OrFilter<T: Persistent> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> OrFilter<T> {
    fn new(filters: FilterBuilder<T>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(OrFilter { filters })
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for OrFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let mut conditions = Vec::new();
        for step in self.filters.steps {
            conditions.push(step.condition());
        }
        Box::new(move |id, x| {
            for condition in &mut conditions {
                if condition(id, x) {
                    return true;
                }
            }
            false
        })
    }
}

pub struct AndFilter<T: Persistent> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> AndFilter<T> {
    fn new(filters: FilterBuilder<T>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(AndFilter { filters })
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for AndFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |id, r| condition(id, r))
    }
}

pub struct NotFilter<T: Persistent> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> NotFilter<T> {
    fn new(filters: FilterBuilder<T>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(NotFilter { filters })
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for NotFilter<T> {
    type Target = T;
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |id, r| !condition(id, r))
    }
}

pub struct FilterBuilder<T: Persistent> {
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

    pub(crate) fn condition(self) -> Box<dyn FnMut(&Ref<T>, &T) -> bool> {
        let mut conditions = Vec::new();
        for filter in self.steps {
            conditions.push(filter.condition());
        }
        Box::new(move |id, t| {
            for condition in &mut conditions {
                if !condition(id, t) {
                    return false;
                }
            }
            return true;
        })
    }
    pub fn finish<'a>(mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            if let Ok(ok) = structsy.scan::<T>() {
                Box::new(ok)
            } else {
                Box::new(Vec::new().into_iter())
            }
        } else {
            for x in &mut self.steps {
                x.score(&structsy);
            }
            self.steps.sort_by_key(|x| x.get_score());
            let res = self.steps.remove(0).first(structsy);
            let mut condition = self.condition();
            Box::new(res.filter(move |(id, r)| condition(id, r)))
        }
    }

    pub fn finish_tx<'a>(mut self, mut tx: &'a mut OwnedSytx) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            if let Ok(ok) = tx.scan::<T>() {
                Box::new(ok)
            } else {
                Box::new(Vec::new().into_iter())
            }
        } else {
            for x in &mut self.steps {
                tx = x.score_tx(tx).1;
            }
            self.steps.sort_by_key(|x| x.get_score());
            let res = self.steps.remove(0).first_tx(tx);
            let mut condition = self.condition();
            Box::new(res.filter(move |(id, r)| condition(id, r)))
        }
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
            self.add(RangeIndexFilter::new(index_name, access, start, end))
        } else {
            self.add(RangeConditionFilter::new(access, start, end))
        }
    }

    pub fn indexable_range_str<'a, R>(&mut self, name: &str, range: R, access: fn(&T) -> &String)
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
        if let Some(index_name) = Self::is_indexed(name) {
            self.add(RangeIndexFilter::new(index_name, access, start, end))
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
            self.add(RangeSingleIndexFilter::new(index_name, access, start, end))
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
    pub fn or(&mut self, filters: StructsyOrFilter<T>) {
        self.add(OrFilter::new(filters.filter()))
    }

    pub fn and(&mut self, filters: StructsyAndFilter<T>) {
        self.add(AndFilter::new(filters.filter()))
    }

    pub fn not(&mut self, filters: StructsyNotFilter<T>) {
        self.add(NotFilter::new(filters.filter()))
    }
}
