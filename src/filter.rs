use crate::{
    index::{find, find_range, find_range_tx, find_tx},
    internal::{Description, EmbeddedDescription, Field},
    queries::StructsyFilter,
    EmbeddedFilter, OwnedSytx, Persistent, PersistentEmbedded, Ref, Structsy, StructsyQuery, StructsyTx,
};
use persy::IndexType;
use std::ops::{Bound, RangeBounds};

pub(crate) type FIter<'a, P> = Box<dyn Iterator<Item = (Ref<P>, P)> + 'a>;

trait Starter<'a, T> {
    fn start(self: Box<Self>, structsy: &Structsy) -> FIter<'a, T>;
    fn start_tx(self: Box<Self>, tx: &'a mut OwnedSytx) -> FIter<'a, T>;
}

struct ScanStarter<T> {
    condition: Box<dyn FnMut(&Ref<T>, &T) -> bool>,
}
impl<T> ScanStarter<T> {
    fn new(condition: Box<dyn FnMut(&Ref<T>, &T) -> bool>) -> Self {
        Self { condition }
    }
}
impl<'a, T: Persistent + 'static> Starter<'a, T> for ScanStarter<T> {
    fn start(self: Box<Self>, structsy: &Structsy) -> FIter<'a, T> {
        let mut condition = self.condition;
        if let Ok(found) = structsy.scan::<T>() {
            Box::new(found.filter(move |(id, r)| condition(id, r)))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
    fn start_tx(self: Box<Self>, tx: &'a mut OwnedSytx) -> FIter<'a, T> {
        let mut condition = self.condition;
        if let Ok(found) = StructsyTx::scan::<T>(tx) {
            Box::new(found.filter(move |(id, r)| condition(id, r)))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct DataStarter<T> {
    data: Box<dyn Iterator<Item = (Ref<T>, T)>>,
}
impl<'a, T> DataStarter<T> {
    fn new(data: Box<dyn Iterator<Item = (Ref<T>, T)>>) -> Self {
        Self { data }
    }
}
impl<'a, T: Persistent + 'static> Starter<'a, T> for DataStarter<T> {
    fn start(self: Box<Self>, _structsy: &Structsy) -> FIter<'a, T> {
        self.data
    }
    fn start_tx(self: Box<Self>, _tx: &'a mut OwnedSytx) -> FIter<'a, T> {
        self.data
    }
}

trait FilterBuilderStep {
    type Target: 'static;
    fn score(&mut self, _structsy: &Structsy) -> u32 {
        std::u32::MAX
    }

    fn score_tx<'a>(&mut self, tx: &'a mut OwnedSytx) -> (u32, &'a mut OwnedSytx) {
        (std::u32::MAX, tx)
    }

    fn get_score(&self) -> u32 {
        std::u32::MAX
    }

    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>>;

    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool>;
}

struct IndexFilter<V, T> {
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
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        let data = Box::new(self.data.unwrap_or_else(|| Vec::new()).into_iter());
        Box::new(DataStarter::new(data))
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

struct ConditionSingleFilter<V, T> {
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
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| (self.access)(x).contains(&self.value))
    }
}

struct ConditionFilter<V, T> {
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
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| *(self.access)(x) == self.value)
    }
}

struct RangeSingleConditionFilter<V, T> {
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
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
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

struct RangeConditionFilter<V, T> {
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
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let b1 = clone_bound(&self.value_start);
        let b2 = clone_bound(&self.value_end);
        let val = (b1, b2);
        Box::new(move |_, x| val.contains((self.access)(x)))
    }
}

struct RangeIndexFilter<V, T> {
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

    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        let data = self.data.unwrap_or_else(|| Box::new(Vec::new().into_iter()));
        Box::new(DataStarter::new(data))
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

struct RangeSingleIndexFilter<V, T> {
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
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        let data = self.data.unwrap_or_else(|| Box::new(Vec::new().into_iter()));
        Box::new(DataStarter::new(data))
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

struct RangeOptionConditionFilter<V, T> {
    value_start: Bound<Option<V>>,
    value_end: Bound<Option<V>>,
    access: fn(&T) -> &Option<V>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeOptionConditionFilter<V, T> {
    fn new(
        access: fn(&T) -> &Option<V>,
        value_start: Bound<Option<V>>,
        value_end: Bound<Option<V>>,
    ) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeOptionConditionFilter {
            access,
            value_start,
            value_end,
        })
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeOptionConditionFilter<V, T> {
    type Target = T;
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
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
        Box::new(move |_, x| {
            if let Some(z) = (self.access)(x) {
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

impl<V: 'static, T: Persistent + 'static> EmbeddedFieldFilter<V, T> {
    fn new(filter: EmbeddedFilter<V>, access: fn(&T) -> &V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(EmbeddedFieldFilter { filter, access })
    }
}

impl<V: 'static, T: Persistent + 'static> FilterBuilderStep for EmbeddedFieldFilter<V, T> {
    type Target = T;

    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
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
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
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

pub struct VecQueryFilter<V: Persistent + 'static, T: Persistent> {
    query: StructsyQuery<V>,
    access: fn(&T) -> &Vec<Ref<V>>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> VecQueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, access: fn(&T) -> &Vec<Ref<V>>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(VecQueryFilter { query, access })
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for VecQueryFilter<V, T> {
    type Target = T;
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
    fn condition<'a>(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let st = self.query.structsy.clone();
        let mut condition = self.query.builder().condition();
        let access = self.access;
        Box::new(move |_, x| {
            let ids = (access)(&x).clone();
            for id in ids {
                if let Some(r) = st.read(&id).unwrap_or(None) {
                    if condition(&id, &r) {
                        return true;
                    }
                }
            }
            false
        })
    }
}

pub struct OptionQueryFilter<V: Persistent + 'static, T: Persistent> {
    query: StructsyQuery<V>,
    access: fn(&T) -> &Option<Ref<V>>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> OptionQueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, access: fn(&T) -> &Option<Ref<V>>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(OptionQueryFilter { query, access })
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for OptionQueryFilter<V, T> {
    type Target = T;
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
    fn condition<'a>(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let st = self.query.structsy.clone();
        let mut condition = self.query.builder().condition();
        let access = self.access;
        Box::new(move |_, x| {
            if let Some(id) = (access)(&x).clone() {
                if let Some(r) = st.read(&id).unwrap_or(None) {
                    condition(&id, &r)
                } else {
                    false
                }
            } else {
                false
            }
        })
    }
}

pub struct OrFilter<T> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> OrFilter<T> {
    fn new(filters: FilterBuilder<T>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(OrFilter { filters })
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for OrFilter<T> {
    type Target = T;
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
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

pub struct AndFilter<T> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> AndFilter<T> {
    fn new(filters: FilterBuilder<T>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(AndFilter { filters })
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for AndFilter<T> {
    type Target = T;
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |id, r| condition(id, r))
    }
}

pub struct NotFilter<T> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> NotFilter<T> {
    fn new(filters: FilterBuilder<T>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(NotFilter { filters })
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for NotFilter<T> {
    type Target = T;
    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition()))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |id, r| !condition(id, r))
    }
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

pub trait RangeCondition<T: Persistent + 'static, V: Clone + PartialOrd + 'static> {
    fn range<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, V>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter.add(RangeConditionFilter::new(field.access, start, end))
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter.add(RangeSingleConditionFilter::new(field.access, start, end))
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
        filter.add(RangeOptionConditionFilter::new(field.access, start, end))
    }
}
pub trait SimpleCondition<T: Persistent + 'static, V: Clone + PartialEq + 'static> {
    fn equal(filter: &mut FilterBuilder<T>, field: Field<T, V>, value: V) {
        filter.add(ConditionFilter::new(field.access, value))
    }

    fn contains(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter.add(ConditionSingleFilter::new(field.access, value))
    }

    fn is(filter: &mut FilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.add(ConditionFilter::new(field.access, Some(value)))
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
                    filter.add(RangeIndexFilter::new(index_name, field.access, start, end))
                } else {
                    filter.add(RangeConditionFilter::new(field.access, start, end))
                }
            }

            fn range_contains<R: RangeBounds<$t>>(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, range: R) {
                let start = clone_bound_ref(&range.start_bound());
                let end = clone_bound_ref(&range.end_bound());
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(RangeSingleIndexFilter::new(index_name, field.access, start, end))
                } else {
                    filter.add(RangeSingleConditionFilter::new(field.access, start, end))
                }
            }
        }

        impl<T: Persistent + 'static> SimpleCondition<T, $t>  for $t {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, $t>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field.access, value))
                }
            }
            fn contains(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionSingleFilter::new(field.access, value))
                }
            }
            fn is(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field.access, Some(value)))
                }
            }
        }

        impl<T: Persistent + 'static> SimpleCondition<T, Vec<$t>> for Vec<$t> {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, value: Vec<$t>) {
                if let Some(_index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    // TODO: support index search for vec types
                    filter.add(ConditionFilter::new(field.access, value))
                //filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field.access, value))
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
                    filter.add(ConditionFilter::<Option<$t>, T>::new(field.access, value));
                }
            }
        }

        impl<T: Persistent + 'static> RangeCondition<T, Option<$t>> for Option<$t> {
            fn range<R: RangeBounds<Option<$t>>>(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, range: R) {
                let start = clone_bound_ref(&range.start_bound());
                let end = clone_bound_ref(&range.end_bound());
                // This may support index in future, but it does not now
                filter.add(RangeOptionConditionFilter::new(field.access, start, end))
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

pub struct FilterBuilder<T> {
    steps: Vec<Box<dyn FilterBuilderStep<Target = T>>>,
}

impl<T> FilterBuilder<T> {
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder { steps: Vec::new() }
    }

    fn add(&mut self, filter: Box<dyn FilterBuilderStep<Target = T>>) {
        self.steps.push(filter);
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
            let res = self.steps.remove(0).first().start(structsy);
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
            let res = self.steps.remove(0).first().start_tx(tx);
            let mut condition = self.condition();
            Box::new(res.filter(move |(id, r)| condition(id, r)))
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

    pub fn ref_vec_query<V>(&mut self, field: Field<T, Vec<Ref<V>>>, query: StructsyQuery<V>)
    where
        V: Persistent + 'static,
    {
        self.add(VecQueryFilter::new(query, field.access))
    }

    pub fn ref_option_query<V>(&mut self, field: Field<T, Option<Ref<V>>>, query: StructsyQuery<V>)
    where
        V: Persistent + 'static,
    {
        self.add(OptionQueryFilter::new(query, field.access))
    }

    pub fn or(&mut self, filters: StructsyFilter<T>) {
        self.add(OrFilter::new(filters.filter()))
    }

    pub fn and(&mut self, filters: StructsyFilter<T>) {
        self.add(AndFilter::new(filters.filter()))
    }

    pub fn not(&mut self, filters: StructsyFilter<T>) {
        self.add(NotFilter::new(filters.filter()))
    }
}
