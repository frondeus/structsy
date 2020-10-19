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

trait ExecutionStep {
    type Target: 'static;
    fn get_score(&self) -> u32;

    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>>;

    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool>;
}

struct DataExecution<T> {
    score: u32,
    data: Vec<(Ref<T>, T)>,
}
impl<T> DataExecution<T> {
    fn new(data: Vec<(Ref<T>, T)>, score: u32) -> Self {
        Self { score, data }
    }
}

impl<T: 'static + Persistent> ExecutionStep for DataExecution<T> {
    type Target = T;
    fn get_score(&self) -> u32 {
        self.score
    }

    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(DataStarter::new(Box::new(self.data.into_iter())))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let to_filter = self.data.into_iter().map(|(r, _)| r).collect::<Vec<_>>();
        Box::new(move |id, _| to_filter.contains(id))
    }
}
struct FilterExecution<T> {
    score: u32,
    condition: Box<dyn FnMut(&Ref<T>, &T) -> bool>,
}
impl<T> FilterExecution<T> {
    fn new(condition: Box<dyn FnMut(&Ref<T>, &T) -> bool>, score: u32) -> Self {
        Self { score, condition }
    }
}

impl<T: 'static + Persistent> ExecutionStep for FilterExecution<T> {
    type Target = T;
    fn get_score(&self) -> u32 {
        self.score
    }

    fn first<'a>(self: Box<Self>) -> Box<dyn Starter<'a, Self::Target>> {
        Box::new(ScanStarter::new(self.condition))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        self.condition
    }
}

trait FilterBuilderStep {
    type Target: 'static;
    fn prepare(self: Box<Self>, structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>>;
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx);

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
    fn prepare(self: Box<Self>, structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let data = find(&structsy, &self.index_name, &self.index_value).unwrap_or_else(|_| Vec::new());
        let len = data.len();
        Box::new(DataExecution::new(data, len as u32))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        let data = find_tx(tx, &self.index_name, &self.index_value).unwrap_or_else(|_| Vec::new());
        let len = data.len();
        (Box::new(DataExecution::new(data, len as u32)), tx)
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
    field: Field<T, Vec<V>>,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionSingleFilter<V, T> {
    fn new(field: Field<T, Vec<V>>, value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(ConditionSingleFilter { field, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionSingleFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| (self.field.access)(x).contains(&self.value))
    }
}

struct ConditionFilter<V, T> {
    value: V,
    field: Field<T, V>,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionFilter<V, T> {
    fn new(field: Field<T, V>, value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(ConditionFilter { field, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| *(self.field.access)(x) == self.value)
    }
}

struct RangeSingleConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, Vec<V>>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeSingleConditionFilter<V, T> {
    fn new(field: Field<T, Vec<V>>, values: (Bound<V>, Bound<V>)) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeSingleConditionFilter { field, values })
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeSingleConditionFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| {
            for el in (self.field.access)(x) {
                if self.values.contains(el) {
                    return true;
                }
            }
            false
        })
    }
}

struct RangeConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, V>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeConditionFilter<V, T> {
    fn new(field: Field<T, V>, values: (Bound<V>, Bound<V>)) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeConditionFilter { field, values })
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeConditionFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| self.values.contains((self.field.access)(x)))
    }
}

struct RangeIndexFilter<V, T> {
    index_name: String,
    field: Field<T, V>,
    values: (Bound<V>, Bound<V>),
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> RangeIndexFilter<V, T> {
    fn new(
        index_name: String,
        field: Field<T, V>,
        values: (Bound<V>, Bound<V>),
    ) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeIndexFilter {
            index_name,
            field,
            values,
        })
    }
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> FilterBuilderStep for RangeIndexFilter<V, T> {
    type Target = T;
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        if let Ok(iter) = find_range_tx(tx, &self.index_name, self.values.clone()) {
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
                let len = vec.len();
                return (Box::new(DataExecution::new(vec, len as u32)), tx);
            }
        }
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn prepare(self: Box<Self>, structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        if let Ok(iter) = find_range(&structsy, &self.index_name, self.values.clone()) {
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
                let len = vec.len();
                return Box::new(DataExecution::new(vec, len as u32));
            }
        }
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }

    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| self.values.contains((self.field.access)(x)))
    }
}

struct RangeSingleIndexFilter<V, T> {
    index_name: String,
    field: Field<T, Vec<V>>,
    values: (Bound<V>, Bound<V>),
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> RangeSingleIndexFilter<V, T> {
    fn new(
        index_name: String,
        field: Field<T, Vec<V>>,
        values: (Bound<V>, Bound<V>),
    ) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeSingleIndexFilter {
            index_name,
            field,
            values,
        })
    }
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> FilterBuilderStep for RangeSingleIndexFilter<V, T> {
    type Target = T;
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        if let Ok(iter) = find_range_tx(tx, &self.index_name, self.values.clone()) {
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
                let len = vec.len();
                return (Box::new(DataExecution::new(vec, len as u32)), tx);
            }
        }
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn prepare(self: Box<Self>, structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        if let Ok(iter) = find_range(&structsy, &self.index_name, self.values.clone()) {
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
                let len = vec.len();
                return Box::new(DataExecution::new(vec, len as u32));
            }
        }
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        Box::new(move |_, x| {
            for el in (self.field.access)(x) {
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

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeOptionConditionFilter<V, T> {
    fn new(
        field: Field<T, Option<V>>,
        values: (Bound<Option<V>>, Bound<Option<V>>),
    ) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(RangeOptionConditionFilter { field, values })
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeOptionConditionFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
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
        Box::new(move |_, x| {
            if let Some(z) = (self.field.access)(x) {
                val.contains(z)
            } else {
                include_none
            }
        })
    }
}

pub struct EmbeddedFieldFilter<V, T> {
    filter: EmbeddedFilter<V>,
    field: Field<T, V>,
}

impl<V: 'static, T: Persistent + 'static> EmbeddedFieldFilter<V, T> {
    fn new(filter: EmbeddedFilter<V>, field: Field<T, V>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(EmbeddedFieldFilter { filter, field })
    }
}

impl<V: 'static, T: Persistent + 'static> FilterBuilderStep for EmbeddedFieldFilter<V, T> {
    type Target = T;

    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition<'a>(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let mut condition = self.filter.condition();
        let access = self.field.access;
        Box::new(move |_, x| condition((access)(x)))
    }
}

pub struct QueryFilter<V: Persistent + 'static, T: Persistent> {
    query: StructsyQuery<V>,
    field: Field<T, Ref<V>>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> QueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, field: Field<T, Ref<V>>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(QueryFilter { query, field })
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for QueryFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        // TODO: replace with a query based execution
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        // TODO: replace with a query based execution
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition<'a>(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let st = self.query.structsy.clone();
        let mut condition = self.query.builder().condition();
        let access = self.field.access;
        Box::new(move |_, x| {
            let id = (access)(&x);
            if let Some(r) = st.read(id).unwrap_or(None) {
                condition(&id, &r)
            } else {
                false
            }
        })
    }
}

pub struct VecQueryFilter<V: Persistent + 'static, T: Persistent> {
    query: StructsyQuery<V>,
    field: Field<T, Vec<Ref<V>>>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> VecQueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, field: Field<T, Vec<Ref<V>>>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(VecQueryFilter { query, field })
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for VecQueryFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition<'a>(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let st = self.query.structsy.clone();
        let mut condition = self.query.builder().condition();
        let access = self.field.access;
        Box::new(move |_, x| {
            for id in (access)(&x) {
                if let Some(r) = st.read(id).unwrap_or(None) {
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
    field: Field<T, Option<Ref<V>>>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> OptionQueryFilter<V, T> {
    fn new(query: StructsyQuery<V>, field: Field<T, Option<Ref<V>>>) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(OptionQueryFilter { query, field })
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for OptionQueryFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition<'a>(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let st = self.query.structsy.clone();
        let mut condition = self.query.builder().condition();
        let access = self.field.access;
        Box::new(move |_, x| {
            if let Some(id) = (access)(&x) {
                if let Some(r) = st.read(id).unwrap_or(None) {
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
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
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
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
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
    fn prepare(self: Box<Self>, _structsy: &Structsy) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        Box::new(FilterExecution::new(self.condition(), u32::MAX))
    }
    fn prepare_tx<'a>(
        self: Box<Self>,
        tx: &'a mut OwnedSytx,
    ) -> (Box<dyn ExecutionStep<Target = Self::Target>>, &'a mut OwnedSytx) {
        (Box::new(FilterExecution::new(self.condition(), u32::MAX)), tx)
    }
    fn condition(self: Box<Self>) -> Box<dyn FnMut(&Ref<Self::Target>, &Self::Target) -> bool> {
        let mut condition = self.filters.condition();
        Box::new(move |id, r| !condition(id, r))
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
        filter.add(RangeConditionFilter::new(field, (start, end)))
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        let start = clone_bound_ref(&range.start_bound());
        let end = clone_bound_ref(&range.end_bound());
        filter.add(RangeSingleConditionFilter::new(field, (start, end)))
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
        filter.add(RangeOptionConditionFilter::new(field, (start, end)))
    }
}
pub trait SimpleCondition<T: Persistent + 'static, V: Clone + PartialEq + 'static> {
    fn equal(filter: &mut FilterBuilder<T>, field: Field<T, V>, value: V) {
        filter.add(ConditionFilter::new(field, value))
    }

    fn contains(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter.add(ConditionSingleFilter::new(field, value))
    }

    fn is(filter: &mut FilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.add(ConditionFilter::new(field, Some(value)))
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
                    filter.add(RangeIndexFilter::new(index_name, field, (start, end)))
                } else {
                    filter.add(RangeConditionFilter::new(field,( start, end)))
                }
            }

            fn range_contains<R: RangeBounds<$t>>(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, range: R) {
                let start = clone_bound_ref(&range.start_bound());
                let end = clone_bound_ref(&range.end_bound());
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(RangeSingleIndexFilter::new(index_name, field, (start, end)))
                } else {
                    filter.add(RangeSingleConditionFilter::new(field, (start, end)))
                }
            }
        }

        impl<T: Persistent + 'static> SimpleCondition<T, $t>  for $t {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, $t>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field, value))
                }
            }
            fn contains(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionSingleFilter::new(field, value))
                }
            }
            fn is(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, value: $t) {
                if let Some(index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field, Some(value)))
                }
            }
        }

        impl<T: Persistent + 'static> SimpleCondition<T, Vec<$t>> for Vec<$t> {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, Vec<$t>>, value: Vec<$t>) {
                if let Some(_index_name) = FilterBuilder::<T>::is_indexed(field.name) {
                    // TODO: support index search for vec types
                    filter.add(ConditionFilter::new(field, value))
                //filter.add(IndexFilter::new(index_name, value))
                } else {
                    filter.add(ConditionFilter::new(field, value))
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
                    filter.add(ConditionFilter::<Option<$t>, T>::new(field, value));
                }
            }
        }

        impl<T: Persistent + 'static> RangeCondition<T, Option<$t>> for Option<$t> {
            fn range<R: RangeBounds<Option<$t>>>(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, range: R) {
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

trait OrderStep {}

struct FieldOrder<T, V> {
    field: Field<T, V>,
}
impl<T: 'static, V: 'static> FieldOrder<T, V> {
    fn new(field: Field<T, V>) -> Box<dyn OrderStep> {
        Box::new(Self { field })
    }
}

impl<T, V> OrderStep for FieldOrder<T, V> {}

pub struct FilterBuilder<T> {
    steps: Vec<Box<dyn FilterBuilderStep<Target = T>>>,
    order: Vec<Box<dyn OrderStep>>,
}

impl<T> FilterBuilder<T> {
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder {
            steps: Vec::new(),
            order: Vec::new(),
        }
    }

    fn add(&mut self, filter: Box<dyn FilterBuilderStep<Target = T>>) {
        self.steps.push(filter);
    }

    fn add_order(&mut self, order: Box<dyn OrderStep>) {
        self.order.push(order)
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
    fn fill_conditions(executions: Vec<Box<dyn ExecutionStep<Target = T>>>) -> Box<dyn FnMut(&Ref<T>, &T) -> bool> {
        let mut conditions = Vec::new();
        for filter in executions {
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
    pub fn finish<'a>(self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            if let Ok(ok) = structsy.scan::<T>() {
                Box::new(ok)
            } else {
                Box::new(Vec::new().into_iter())
            }
        } else {
            let mut executions = self.steps.into_iter().map(|e| e.prepare(structsy)).collect::<Vec<_>>();
            executions.sort_by_key(|x| x.get_score());
            let res = executions.pop().unwrap().first().start(structsy);
            let mut cond = Self::fill_conditions(executions);
            Box::new(res.filter(move |(id, r)| cond(id, r)))
        }
    }

    pub fn finish_tx<'a>(self, mut tx: &'a mut OwnedSytx) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            if let Ok(ok) = tx.scan::<T>() {
                Box::new(ok)
            } else {
                Box::new(Vec::new().into_iter())
            }
        } else {
            let mut executions = Vec::new();
            for x in self.steps {
                let (e, r_tx) = x.prepare_tx(tx);
                tx = r_tx;
                executions.push(e);
            }
            executions.sort_by_key(|x| x.get_score());
            let res = executions.pop().unwrap().first().start_tx(tx);
            let mut cond = Self::fill_conditions(executions);
            Box::new(res.filter(move |(id, r)| cond(id, r)))
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
        self.add(EmbeddedFieldFilter::new(filter, field))
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: StructsyQuery<V>)
    where
        V: Persistent + 'static,
    {
        self.add(QueryFilter::new(query, field))
    }

    pub fn ref_vec_query<V>(&mut self, field: Field<T, Vec<Ref<V>>>, query: StructsyQuery<V>)
    where
        V: Persistent + 'static,
    {
        self.add(VecQueryFilter::new(query, field))
    }

    pub fn ref_option_query<V>(&mut self, field: Field<T, Option<Ref<V>>>, query: StructsyQuery<V>)
    where
        V: Persistent + 'static,
    {
        self.add(OptionQueryFilter::new(query, field))
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

    pub fn order<V: 'static>(&mut self, field: Field<T, V>) {
        self.add_order(FieldOrder::new(field))
    }
}
