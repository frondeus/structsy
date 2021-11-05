use crate::{
    filter_builder::{
        embedded_filter_builder::{build_condition, EmbeddedFilterBuilderStep},
        execution_step::{DataExecution, ExecutionStep, FilterExecution},
        filter_builder::{FilterBuilder, Item},
        reader::Reader,
    },
    internal::Field,
    format::PersistentEmbedded,
    Persistent, Ref,
};
use persy::IndexType;
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

pub(crate) trait FilterBuilderStep {
    type Target: 'static;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>>;
}

pub(crate) struct IndexFilter<V, T> {
    index_name: String,
    index_value: V,
    phantom: PhantomData<T>,
}

impl<V: 'static, T: Persistent + 'static> IndexFilter<V, T> {
    pub(crate) fn new(index_name: String, index_value: V) -> Self {
        IndexFilter {
            index_name,
            index_value,
            phantom: PhantomData,
        }
    }
}

impl<V: PersistentEmbedded+Clone + 'static, T: Persistent + 'static> FilterBuilderStep for IndexFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let data = reader
            .find(&self.index_name, &self.index_value)
            .unwrap_or_else(|_| Vec::new());
        let len = data.len();
        Box::new(DataExecution::new(data, len as u32))
    }
}

pub(crate) struct ConditionSingleFilter<V, T> {
    value: V,
    field: Field<T, Vec<V>>,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionSingleFilter<V, T> {
    pub(crate) fn new(field: Field<T, Vec<V>>, value: V) -> Self {
        ConditionSingleFilter { field, value }
    }
}
impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionSingleFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = move |it: &Item<T>, _: &mut Reader| (self.field.access)(&it.record).contains(&self.value);
        Box::new(FilterExecution::new(condition, u32::MAX))
    }
}

pub(crate) struct RangeConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, V>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeConditionFilter<V, T> {
    pub(crate) fn new(field: Field<T, V>, values: (Bound<V>, Bound<V>)) -> Self {
        RangeConditionFilter { field, values }
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeConditionFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = move |it: &Item<T>, _: &mut Reader| self.values.contains((self.field.access)(&it.record));
        Box::new(FilterExecution::new(condition, u32::MAX))
    }
}

pub(crate) struct RangeIndexFilter<V, T> {
    index_name: String,
    field: Field<T, V>,
    values: (Bound<V>, Bound<V>),
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> RangeIndexFilter<V, T> {
    pub(crate) fn new(index_name: String, field: Field<T, V>, values: (Bound<V>, Bound<V>)) -> Self {
        RangeIndexFilter {
            index_name,
            field,
            values,
        }
    }
}

impl<V: PersistentEmbedded+Clone + PartialOrd + 'static, T: Persistent + 'static> FilterBuilderStep for RangeIndexFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        if let Ok(Some(values)) = reader.find_range_first(&self.index_name, self.values.clone()) {
            let len = values.len();
            return Box::new(DataExecution::new(values, len as u32));
        }
        let condition = move |it: &Item<T>, _: &mut Reader| self.values.contains((self.field.access)(&it.record));
        Box::new(FilterExecution::new(condition, u32::MAX))
    }
}

pub(crate) struct RangeSingleIndexFilter<V, T> {
    index_name: String,
    field: Field<T, Vec<V>>,
    values: (Bound<V>, Bound<V>),
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> RangeSingleIndexFilter<V, T> {
    pub(crate) fn new(index_name: String, field: Field<T, Vec<V>>, values: (Bound<V>, Bound<V>)) -> Self {
        RangeSingleIndexFilter {
            index_name,
            field,
            values,
        }
    }
}

impl<V: PersistentEmbedded+Clone + PartialOrd + 'static, T: Persistent + 'static> FilterBuilderStep for RangeSingleIndexFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        if let Ok(Some(values)) = reader.find_range_first(&self.index_name, self.values.clone()) {
            let len = values.len();
            Box::new(DataExecution::new(values, len as u32))
        } else {
            let condition = move |it: &Item<T>, _: &mut Reader| {
                for el in (self.field.access)(&it.record) {
                    if self.values.contains(el) {
                        return true;
                    }
                }
                false
            };
            Box::new(FilterExecution::new(condition, u32::MAX))
        }
    }
}

pub(crate) struct RangeSingleConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, Vec<V>>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeSingleConditionFilter<V, T> {
    pub(crate) fn new(field: Field<T, Vec<V>>, values: (Bound<V>, Bound<V>)) -> Self {
        RangeSingleConditionFilter { field, values }
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeSingleConditionFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = move |it: &Item<T>, _: &mut Reader| {
            for el in (self.field.access)(&it.record) {
                if self.values.contains(el) {
                    return true;
                }
            }
            false
        };
        Box::new(FilterExecution::new(condition, u32::MAX))
    }
}
pub(crate) struct ConditionFilter<V, T> {
    value: V,
    field: Field<T, V>,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionFilter<V, T> {
    pub(crate) fn new(field: Field<T, V>, value: V) -> Self {
        ConditionFilter { field, value }
    }
}
impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = move |it: &Item<T>, _: &mut Reader| *(self.field.access)(&it.record) == self.value;
        Box::new(FilterExecution::new(condition, u32::MAX))
    }
}

pub(crate) struct RangeOptionConditionFilter<V, T> {
    values: (Bound<Option<V>>, Bound<Option<V>>),
    field: Field<T, Option<V>>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeOptionConditionFilter<V, T> {
    pub(crate) fn new(field: Field<T, Option<V>>, values: (Bound<Option<V>>, Bound<Option<V>>)) -> Self {
        RangeOptionConditionFilter { field, values }
    }
}
impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for RangeOptionConditionFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, _reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
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
        let condition = move |it: &Item<T>, _: &mut Reader| {
            if let Some(z) = (self.field.access)(&it.record) {
                val.contains(z)
            } else {
                include_none
            }
        };
        Box::new(FilterExecution::new(condition, u32::MAX))
    }
}

pub struct EmbeddedFieldFilter<V, T> {
    steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = V>>>,
    field: Field<T, V>,
}

impl<V: 'static, T: Persistent + 'static> EmbeddedFieldFilter<V, T> {
    pub(crate) fn new(steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = V>>>, field: Field<T, V>) -> Self {
        EmbeddedFieldFilter { steps, field }
    }
}

impl<V: 'static, T: Persistent + 'static> FilterBuilderStep for EmbeddedFieldFilter<V, T> {
    type Target = T;

    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = build_condition(self.steps, reader);
        let access = self.field.access;
        let cond = move |it: &Item<T>, reader: &mut Reader| condition((access)(&it.record), reader);
        Box::new(FilterExecution::new(cond, u32::MAX))
    }
}

pub struct QueryFilter<V: Persistent + 'static, T: Persistent> {
    query: FilterBuilder<V>,
    field: Field<T, Ref<V>>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> QueryFilter<V, T> {
    pub(crate) fn new(query: FilterBuilder<V>, field: Field<T, Ref<V>>) -> Self {
        QueryFilter { query, field }
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for QueryFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = self.query.fill_conditions_step(reader);
        let access = self.field.access;
        let cond = move |it: &Item<T>, reader: &mut Reader| {
            let id = (access)(&it.record);
            if let Some(r) = reader.read(id).unwrap_or(None) {
                condition.check(&Item::new((id.clone(), r)), reader)
            } else {
                false
            }
        };
        Box::new(FilterExecution::new(cond, u32::MAX))
    }
}

pub struct VecQueryFilter<V: Persistent + 'static, T: Persistent> {
    query: FilterBuilder<V>,
    field: Field<T, Vec<Ref<V>>>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> VecQueryFilter<V, T> {
    pub(crate) fn new(query: FilterBuilder<V>, field: Field<T, Vec<Ref<V>>>) -> Self {
        VecQueryFilter { query, field }
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for VecQueryFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = self.query.fill_conditions_step(reader);
        let access = self.field.access;
        let cond = move |it: &Item<T>, reader: &mut Reader| {
            for id in (access)(&it.record) {
                if let Some(r) = reader.read(id).unwrap_or(None) {
                    if condition.check(&Item::new((id.clone(), r)), reader) {
                        return true;
                    }
                }
            }
            false
        };
        Box::new(FilterExecution::new(cond, u32::MAX))
    }
}

pub struct OptionQueryFilter<V: Persistent + 'static, T: Persistent> {
    query: FilterBuilder<V>,
    field: Field<T, Option<Ref<V>>>,
}

impl<V: Persistent + 'static, T: Persistent + 'static> OptionQueryFilter<V, T> {
    pub(crate) fn new(query: FilterBuilder<V>, field: Field<T, Option<Ref<V>>>) -> Self {
        OptionQueryFilter { query, field }
    }
}

impl<V: Persistent + 'static, T: Persistent + 'static> FilterBuilderStep for OptionQueryFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = self.query.fill_conditions_step(reader);
        let access = self.field.access;
        let cond = move |it: &Item<T>, reader: &mut Reader| {
            if let Some(id) = (access)(&it.record) {
                if let Some(r) = reader.read(id).unwrap_or(None) {
                    condition.check(&Item::new((id.clone(), r)), reader)
                } else {
                    false
                }
            } else {
                false
            }
        };
        Box::new(FilterExecution::new(cond, u32::MAX))
    }
}

pub struct OrFilter<T> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> OrFilter<T> {
    pub(crate) fn new(filters: FilterBuilder<T>) -> Self {
        OrFilter { filters }
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for OrFilter<T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let mut conditions = Vec::new();
        for step in self.filters.steps {
            conditions.push(step.prepare(reader));
        }
        let cond = move |it: &Item<T>, reader: &mut Reader| {
            for condition in &conditions {
                if condition.check(it, reader) {
                    return true;
                }
            }
            false
        };
        Box::new(FilterExecution::new(cond, u32::MAX))
    }
}

pub struct AndFilter<T> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> AndFilter<T> {
    pub(crate) fn new(filters: FilterBuilder<T>) -> Self {
        AndFilter { filters }
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for AndFilter<T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = self.filters.fill_conditions_step(reader);
        Box::new(FilterExecution::new(
            move |it: &Item<T>, reader: &mut Reader| condition.check(it, reader),
            u32::MAX,
        ))
    }
}

pub struct NotFilter<T> {
    filters: FilterBuilder<T>,
}

impl<T: Persistent + 'static> NotFilter<T> {
    pub(crate) fn new(filters: FilterBuilder<T>) -> Self {
        NotFilter { filters }
    }
}

impl<T: Persistent + 'static> FilterBuilderStep for NotFilter<T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let condition = self.filters.fill_conditions_step(reader);
        let cond = move |it: &Item<T>, reader: &mut Reader| !condition.check(it, reader);
        Box::new(FilterExecution::new(cond, u32::MAX))
    }
}
