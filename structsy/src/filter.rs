use crate::{
    embedded_filter::{build_condition, EmbeddedFilterBuilder, EmbeddedFilterBuilderStep},
    index::{find, find_range, find_range_snap, find_range_tx, find_snap, find_tx},
    internal::{Description, EmbeddedDescription, Field},
    queries::StructsyFilter,
    structsy::SnapshotIterator,
    transaction::{RefSytx, TxIterator},
    Order, OwnedSytx, Persistent, PersistentEmbedded, Ref, SRes, Snapshot, Structsy, StructsyTx,
};
use persy::IndexType;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

pub(crate) struct Item<P> {
    id: Ref<P>,
    record: P,
}

impl<P> Item<P> {
    pub(crate) fn new((id, record): (Ref<P>, P)) -> Self {
        Self { id, record }
    }
}

struct RevTx<X> {
    base: X,
}

impl<X, P, K> Iterator for RevTx<X>
where
    X: Iterator<Item = (Ref<P>, P, K)>,
    X: DoubleEndedIterator,
{
    type Item = (Ref<P>, P, K);
    fn next(&mut self) -> Option<Self::Item> {
        self.base.next_back()
    }
}

impl<'a, X, P, K> TxIterator<'a> for RevTx<X>
where
    X: TxIterator<'a>,
    X: Iterator<Item = (Ref<P>, P, K)>,
    X: DoubleEndedIterator,
{
    fn tx(&mut self) -> RefSytx {
        self.base.tx()
    }
}

struct MapTx<'a, P, K> {
    base: Box<dyn TxIterator<'a, Item = (Ref<P>, P, K)> + 'a>,
}

impl<'a, P, K> MapTx<'a, P, K> {
    fn new<T: TxIterator<'a, Item = (Ref<P>, P, K)> + 'a>(base: T) -> Self {
        MapTx { base: Box::new(base) }
    }
}

impl<'a, P, K> Iterator for MapTx<'a, P, K> {
    type Item = (Ref<P>, P);
    fn next(&mut self) -> Option<Self::Item> {
        self.base.next().map(|(id, rec, _)| (id, rec))
    }
}

impl<'a, P, K> TxIterator<'a> for MapTx<'a, P, K> {
    fn tx(&mut self) -> RefSytx {
        self.base.tx()
    }
}

pub enum Iter<'a, P> {
    TxIter(Box<dyn TxIterator<'a, Item = (Ref<P>, P)> + 'a>),
    SnapshotIter(Box<dyn SnapshotIterator<Item = (Ref<P>, P)>>),
    Iter(Box<dyn Iterator<Item = (Ref<P>, P)>>),
}

struct ExecutionIterator<'a, P> {
    base: Iter<'a, P>,
    conditions: Conditions<P>,
    structsy: Structsy,
    buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
}
impl<'a, P: 'static> ExecutionIterator<'a, P> {
    fn new_raw(
        base: Iter<'a, P>,
        conditions: Conditions<P>,
        structsy: Structsy,
        buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
    ) -> Self {
        ExecutionIterator {
            base,
            conditions,
            structsy,
            buffered,
        }
    }
    fn new(
        base: Box<dyn Iterator<Item = (Ref<P>, P)>>,
        conditions: Conditions<P>,
        structsy: Structsy,
        buffered: Option<Box<dyn BufferedExection<P> + 'a>>,
    ) -> Self {
        ExecutionIterator {
            base: Iter::Iter(base),
            conditions,
            structsy,
            buffered,
        }
    }
}

impl<'a, P: Persistent + 'static> ExecutionIterator<'a, P> {
    fn filtered_next(base: &mut Iter<P>, conditions: &mut Conditions<P>, structsy: &Structsy) -> Option<Item<P>> {
        while let Some(read) = match base {
            Iter::Iter(ref mut it) => it.next(),
            Iter::SnapshotIter(ref mut it) => it.next(),
            Iter::TxIter(ref mut it) => it.next(),
        } {
            let mut reader = match base {
                Iter::Iter(_) => Reader::Structsy(structsy.clone()),
                Iter::SnapshotIter(it) => Reader::Snapshot(it.snapshot().clone()),
                Iter::TxIter(ref mut it) => Reader::Tx(it.tx()),
            };
            let item = Item::new(read);
            if conditions.check(&item, &mut reader) {
                return Some(item);
            }
        }

        None
    }

    fn buffered_next(&mut self) -> Option<Item<P>> {
        let mut source = (&mut self.base, &mut self.conditions, &self.structsy);
        if let Some(buffered) = &mut self.buffered {
            buffered.next(&mut source)
        } else {
            ExecutionIterator::filtered_next(&mut self.base, &mut self.conditions, &self.structsy)
        }
    }
}
trait Source<T> {
    fn next_item(&mut self) -> Option<Item<T>>;
}
impl<'a, T: Persistent + 'static> Source<T> for (&mut Iter<'a, T>, &mut Conditions<T>, &Structsy) {
    fn next_item(&mut self) -> Option<Item<T>> {
        ExecutionIterator::filtered_next(self.0, self.1, self.2)
    }
}

impl<'a, P: Persistent + 'static> Iterator for ExecutionIterator<'a, P> {
    type Item = (Ref<P>, P);
    fn next(&mut self) -> Option<Self::Item> {
        self.buffered_next().map(|i| (i.id, i.record))
    }
}

trait StartStep<'a, T> {
    fn start(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        structsy: Structsy,
    ) -> ExecutionIterator<'static, T>;
    fn start_tx(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        tx: &'a mut OwnedSytx,
    ) -> ExecutionIterator<T>;
    fn start_snapshot(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        snapshot: &Snapshot,
    ) -> ExecutionIterator<'static, T>;
}

struct ScanStartStep {}
impl ScanStartStep {
    fn new() -> Self {
        ScanStartStep {}
    }
}
impl<'a, T: Persistent + 'static> StartStep<'a, T> for ScanStartStep {
    fn start(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        structsy: Structsy,
    ) -> ExecutionIterator<'static, T> {
        let (buffered, iter) = order.scan(structsy.clone());
        if let Some(it) = iter {
            ExecutionIterator::new_raw(it, conditions, structsy, buffered)
        } else if let Ok(found) = structsy.scan::<T>() {
            ExecutionIterator::new(Box::new(found), conditions, structsy, buffered)
        } else {
            ExecutionIterator::new(Box::new(Vec::new().into_iter()), conditions, structsy, buffered)
        }
    }
    fn start_tx(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        tx: &'a mut OwnedSytx,
    ) -> ExecutionIterator<'a, T> {
        let structsy = Structsy {
            structsy_impl: tx.structsy_impl.clone(),
        };
        if order.index_order() {
            let (buffered, iter) = order.scan_tx(tx);
            ExecutionIterator::new_raw(iter.unwrap(), conditions, structsy, buffered)
        } else if let Ok(found) = tx.scan::<T>() {
            ExecutionIterator::new_raw(Iter::TxIter(Box::new(found)), conditions, structsy, order.buffered())
        } else {
            ExecutionIterator::new(Box::new(Vec::new().into_iter()), conditions, structsy, order.buffered())
        }
    }
    fn start_snapshot(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        snapshot: &Snapshot,
    ) -> ExecutionIterator<'static, T> {
        let (buffered, iter) = order.scan_snapshot(snapshot);
        if let Some(it) = iter {
            ExecutionIterator::new_raw(it, conditions, snapshot.structsy(), buffered)
        } else if let Ok(found) = snapshot.scan::<T>() {
            ExecutionIterator::new(Box::new(found), conditions, snapshot.structsy(), buffered)
        } else {
            ExecutionIterator::new(
                Box::new(Vec::new().into_iter()),
                conditions,
                snapshot.structsy(),
                buffered,
            )
        }
    }
}

struct DataStartStep<T> {
    data: Box<dyn Iterator<Item = (Ref<T>, T)>>,
}
impl<'a, T> DataStartStep<T> {
    fn new(data: Box<dyn Iterator<Item = (Ref<T>, T)>>) -> Self {
        Self { data }
    }
}
impl<'a, T: 'static> StartStep<'a, T> for DataStartStep<T> {
    fn start(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        structsy: Structsy,
    ) -> ExecutionIterator<'static, T> {
        ExecutionIterator::new(Box::new(self.data), conditions, structsy, order.buffered())
    }
    fn start_tx(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        tx: &'a mut OwnedSytx,
    ) -> ExecutionIterator<T> {
        let structsy = Structsy {
            structsy_impl: tx.structsy_impl.clone(),
        };
        ExecutionIterator::new(Box::new(self.data), conditions, structsy, order.buffered())
    }
    fn start_snapshot(
        self: Box<Self>,
        conditions: Conditions<T>,
        order: Orders<T>,
        snapshot: &Snapshot,
    ) -> ExecutionIterator<'static, T> {
        ExecutionIterator::new(Box::new(self.data), conditions, snapshot.structsy(), order.buffered())
    }
}

trait ExecutionStep {
    type Target: 'static;
    fn get_score(&self) -> u32;

    fn as_start<'a>(
        self: Box<Self>,
    ) -> (
        Option<Box<dyn ExecutionStep<Target = Self::Target>>>,
        Box<dyn StartStep<'a, Self::Target>>,
    );

    fn check(&self, item: &Item<Self::Target>, reader: &mut Reader) -> bool;
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

impl<T: 'static> ExecutionStep for DataExecution<T> {
    type Target = T;
    fn get_score(&self) -> u32 {
        self.score
    }

    fn as_start<'a>(
        self: Box<Self>,
    ) -> (
        Option<Box<dyn ExecutionStep<Target = Self::Target>>>,
        Box<dyn StartStep<'a, Self::Target>>,
    ) {
        (None, Box::new(DataStartStep::new(Box::new(self.data.into_iter()))))
    }

    fn check(&self, item: &Item<Self::Target>, _reader: &mut Reader) -> bool {
        for (id, _) in &self.data {
            if id == &item.id {
                return true;
            }
        }
        false
    }
}
struct FilterExecution<T, F>
where
    F: Fn(&Item<T>, &mut Reader) -> bool + 'static,
{
    condition: F,
    phantom: PhantomData<T>,
    score: u32,
}
impl<T, F> FilterExecution<T, F>
where
    F: Fn(&Item<T>, &mut Reader) -> bool + 'static,
{
    fn new(condition: F, score: u32) -> Self {
        Self {
            score,
            condition,
            phantom: PhantomData,
        }
    }
}

impl<T: 'static + Persistent, F> ExecutionStep for FilterExecution<T, F>
where
    F: Fn(&Item<T>, &mut Reader) -> bool + 'static,
{
    type Target = T;
    fn get_score(&self) -> u32 {
        self.score
    }

    fn as_start<'a>(
        self: Box<Self>,
    ) -> (
        Option<Box<dyn ExecutionStep<Target = Self::Target>>>,
        Box<dyn StartStep<'a, Self::Target>>,
    ) {
        (Some(self), Box::new(ScanStartStep::new()))
    }

    fn check(&self, item: &Item<Self::Target>, reader: &mut Reader) -> bool {
        (self.condition)(item, reader)
    }
}

pub enum Reader<'a> {
    Structsy(Structsy),
    Snapshot(Snapshot),
    Tx(RefSytx<'a>),
}
impl<'a> Reader<'a> {
    pub(crate) fn read<T: Persistent>(&mut self, id: &Ref<T>) -> SRes<Option<T>> {
        match self {
            Reader::Structsy(st) => st.read(id),
            Reader::Snapshot(snap) => snap.read(id),
            Reader::Tx(tx) => tx.read(id),
        }
    }
    pub(crate) fn find<K: IndexType, P: Persistent>(&mut self, name: &str, k: &K) -> SRes<Vec<(Ref<P>, P)>> {
        Ok(match self {
            Reader::Structsy(st) => find(st, name, k),
            Reader::Snapshot(st) => find_snap(st, name, k),
            Reader::Tx(tx) => find_tx(tx, name, k),
        }?
        .into_iter()
        .collect())
    }

    pub(crate) fn find_range_first<K: IndexType + 'static, P: Persistent + 'static, R: RangeBounds<K> + 'static>(
        &mut self,
        name: &str,
        range: R,
    ) -> SRes<Option<Vec<(Ref<P>, P)>>> {
        let mut vec = Vec::new();
        match self {
            Reader::Structsy(st) => {
                let iter = find_range(st, name, range)?;
                let no_key = iter.map(|(r, e, _)| (r, e));
                for el in no_key {
                    vec.push(el);
                    if vec.len() == 1000 {
                        break;
                    }
                }
            }
            Reader::Snapshot(snap) => {
                let iter = find_range_snap(snap, name, range)?;
                let no_key = iter.map(|(r, e, _)| (r, e));
                for el in no_key {
                    vec.push(el);
                    if vec.len() == 1000 {
                        break;
                    }
                }
            }
            Reader::Tx(tx) => {
                let iter = find_range_tx(tx, name, range)?;
                let no_key = iter.map(|(r, e, _)| (r, e));
                for el in no_key {
                    vec.push(el);
                    if vec.len() == 1000 {
                        break;
                    }
                }
            }
        };
        if vec.len() < 1000 {
            Ok(Some(vec))
        } else {
            Ok(None)
        }
    }
}

trait FilterBuilderStep {
    type Target: 'static;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>>;
}

struct IndexFilter<V, T> {
    index_name: String,
    index_value: V,
    phantom: PhantomData<T>,
}

impl<V: IndexType + 'static, T: Persistent + 'static> IndexFilter<V, T> {
    fn new(index_name: String, index_value: V) -> Self {
        IndexFilter {
            index_name,
            index_value,
            phantom: PhantomData,
        }
    }
}

impl<V: IndexType + 'static, T: Persistent + 'static> FilterBuilderStep for IndexFilter<V, T> {
    type Target = T;
    fn prepare(self: Box<Self>, reader: &mut Reader) -> Box<dyn ExecutionStep<Target = Self::Target>> {
        let data = reader
            .find(&self.index_name, &self.index_value)
            .unwrap_or_else(|_| Vec::new());
        let len = data.len();
        Box::new(DataExecution::new(data, len as u32))
    }
}

struct ConditionSingleFilter<V, T> {
    value: V,
    field: Field<T, Vec<V>>,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionSingleFilter<V, T> {
    fn new(field: Field<T, Vec<V>>, value: V) -> Self {
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

struct ConditionFilter<V, T> {
    value: V,
    field: Field<T, V>,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionFilter<V, T> {
    fn new(field: Field<T, V>, value: V) -> Self {
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

struct RangeSingleConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, Vec<V>>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeSingleConditionFilter<V, T> {
    fn new(field: Field<T, Vec<V>>, values: (Bound<V>, Bound<V>)) -> Self {
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

struct RangeConditionFilter<V, T> {
    values: (Bound<V>, Bound<V>),
    field: Field<T, V>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeConditionFilter<V, T> {
    fn new(field: Field<T, V>, values: (Bound<V>, Bound<V>)) -> Self {
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

struct RangeIndexFilter<V, T> {
    index_name: String,
    field: Field<T, V>,
    values: (Bound<V>, Bound<V>),
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> RangeIndexFilter<V, T> {
    fn new(index_name: String, field: Field<T, V>, values: (Bound<V>, Bound<V>)) -> Self {
        RangeIndexFilter {
            index_name,
            field,
            values,
        }
    }
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> FilterBuilderStep for RangeIndexFilter<V, T> {
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

struct RangeSingleIndexFilter<V, T> {
    index_name: String,
    field: Field<T, Vec<V>>,
    values: (Bound<V>, Bound<V>),
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> RangeSingleIndexFilter<V, T> {
    fn new(index_name: String, field: Field<T, Vec<V>>, values: (Bound<V>, Bound<V>)) -> Self {
        RangeSingleIndexFilter {
            index_name,
            field,
            values,
        }
    }
}

impl<V: IndexType + PartialOrd + 'static, T: Persistent + 'static> FilterBuilderStep for RangeSingleIndexFilter<V, T> {
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

struct RangeOptionConditionFilter<V, T> {
    values: (Bound<Option<V>>, Bound<Option<V>>),
    field: Field<T, Option<V>>,
}

impl<V: PartialOrd + Clone + 'static, T: Persistent + 'static> RangeOptionConditionFilter<V, T> {
    fn new(field: Field<T, Option<V>>, values: (Bound<Option<V>>, Bound<Option<V>>)) -> Self {
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
    fn new(steps: Vec<Box<dyn EmbeddedFilterBuilderStep<Target = V>>>, field: Field<T, V>) -> Self {
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
    fn new(query: FilterBuilder<V>, field: Field<T, Ref<V>>) -> Self {
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
    fn new(query: FilterBuilder<V>, field: Field<T, Vec<Ref<V>>>) -> Self {
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
    fn new(query: FilterBuilder<V>, field: Field<T, Option<Ref<V>>>) -> Self {
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
    fn new(filters: FilterBuilder<T>) -> Self {
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
    fn new(filters: FilterBuilder<T>) -> Self {
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
    fn new(filters: FilterBuilder<T>) -> Self {
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

        impl<P:Persistent + 'static> Scan<P> for $t {
            fn scan_impl<'a>( field:&Field<P,Self>, structsy:Structsy, order:&Order) -> Option<Iter<'a,P>> {
                if let Some(index_name) = FilterBuilder::<P>::is_indexed(field.name) {
                    if let Ok(iter) = find_range::<Self,P,_>(&structsy, &index_name, ..) {
                        let it:Box<dyn Iterator<Item = (Ref<P>, P)>> = if order == &Order::Desc {
                            Box::new(iter.rev().map(|(r, e, _)| (r, e)))
                        } else {
                            Box::new(iter.map(|(r, e, _)| (r, e)))
                        };
                        Some(Iter::Iter(it))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            fn scan_tx_impl<'a>(field:&Field<P,Self>, tx:&'a mut OwnedSytx, order:&Order) -> Option<Iter<'a,P>> {
                if let Some(index_name) = FilterBuilder::<P>::is_indexed(field.name) {
                    if let Ok(iter) = find_range_tx::<Self,P,_>(tx, &index_name, ..) {
                        let it = if order == &Order::Desc {
                            MapTx::new(RevTx{base:iter})
                        } else {
                            MapTx::new(iter)
                        };
                        Some(Iter::<'a>::TxIter(Box::new(it)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            fn scan_snapshot_impl<'a>( field:&Field<P,Self>, snapshot:&Snapshot, order:&Order) -> Option<Iter<'a,P>> {
                if let Some(index_name) = FilterBuilder::<P>::is_indexed(field.name) {
                    if let Ok(iter) = find_range_snap::<Self,P,_>(snapshot, &index_name, ..) {
                        let it:Box<dyn Iterator<Item = (Ref<P>, P)>> = if order == &Order::Desc {
                            Box::new(iter.rev().map(|(r, e, _)| (r, e)))
                        } else {
                            Box::new(iter.map(|(r, e, _)| (r, e)))
                        };
                        Some(Iter::Iter(it))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            fn is_indexed_impl(field:&Field<P,Self>) -> bool {
                if let Some(_) = FilterBuilder::<P>::is_indexed(field.name) {
                    true
                } else {
                    false
                }
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

impl<P, V: EmbeddedDescription> Scan<P> for V {}

pub(crate) struct Conditions<T> {
    conditions: Vec<Box<dyn ExecutionStep<Target = T>>>,
}
impl<T: 'static> Conditions<T> {
    pub(crate) fn check(&self, item: &Item<T>, reader: &mut Reader) -> bool {
        for condition in &self.conditions {
            if !condition.check(item, reader) {
                return false;
            }
        }
        true
    }
}

pub(crate) struct FieldOrder<T, V> {
    field: Field<T, V>,
    order: Order,
}

impl<T: 'static, V: Ord + 'static + Scan<T>> FieldOrder<T, V> {
    pub(crate) fn new(field: Field<T, V>, order: Order) -> Self {
        Self { field, order }
    }
}

impl<T: 'static, V: Ord + 'static> FieldOrder<T, V> {
    pub(crate) fn new_emb(field: Field<T, V>, order: Order) -> Self {
        Self { field, order }
    }
}

pub(crate) trait OrderStep<P> {
    fn compare(&self, first: &P, second: &P) -> std::cmp::Ordering;
}

pub(crate) trait ScanOrderStep<P>: OrderStep<P> {
    fn scan(&self, structsy: Structsy) -> Option<Iter<'static, P>>;
    fn scan_tx<'a>(&self, tx: &'a mut OwnedSytx) -> Option<Iter<'a, P>>;
    fn scan_snapshot(&self, snapshot: &Snapshot) -> Option<Iter<'static, P>>;
    fn is_indexed(&self) -> bool;
}

impl<P, V: Ord> OrderStep<P> for FieldOrder<P, V> {
    fn compare(&self, first: &P, second: &P) -> std::cmp::Ordering {
        let ord = (self.field.access)(first).cmp((self.field.access)(second));
        if self.order == Order::Asc {
            ord
        } else {
            ord.reverse()
        }
    }
}

impl<P, V: Ord + Scan<P>> ScanOrderStep<P> for FieldOrder<P, V> {
    fn scan(&self, structsy: Structsy) -> Option<Iter<'static, P>> {
        Scan::scan_impl(&self.field, structsy, &self.order)
    }
    fn scan_tx<'a>(&self, tx: &'a mut OwnedSytx) -> Option<Iter<'a, P>> {
        Scan::scan_tx_impl(&self.field, tx, &self.order)
    }
    fn scan_snapshot(&self, snapshot: &Snapshot) -> Option<Iter<'static, P>> {
        Scan::scan_snapshot_impl(&self.field, snapshot, &self.order)
    }
    fn is_indexed(&self) -> bool {
        Scan::is_indexed_impl(&self.field)
    }
}
pub trait Scan<P>: Sized {
    fn scan_impl<'a>(_field: &Field<P, Self>, _structsy: Structsy, _order: &Order) -> Option<Iter<'a, P>> {
        None
    }
    fn scan_tx_impl<'a>(_field: &Field<P, Self>, _tx: &'a mut OwnedSytx, _order: &Order) -> Option<Iter<'a, P>> {
        None
    }
    fn scan_snapshot_impl<'a>(_field: &Field<P, Self>, _snapshot: &Snapshot, _order: &Order) -> Option<Iter<'a, P>> {
        None
    }
    fn is_indexed_impl(_field: &Field<P, Self>) -> bool {
        false
    }
}

pub(crate) struct EmbeddedOrder<T, V> {
    field: Field<T, V>,
    orders: Vec<Box<dyn OrderStep<V>>>,
}

impl<T: 'static, V: 'static> EmbeddedOrder<T, V> {
    pub(crate) fn new(field: Field<T, V>, orders: Vec<Box<dyn OrderStep<V>>>) -> Self {
        Self { field, orders }
    }
    pub(crate) fn new_emb(field: Field<T, V>, orders: Vec<Box<dyn OrderStep<V>>>) -> Self {
        Self { field, orders }
    }
}

impl<P, V> OrderStep<P> for EmbeddedOrder<P, V> {
    fn compare(&self, first: &P, second: &P) -> std::cmp::Ordering {
        let emb_first = (self.field.access)(first);
        let emb_second = (self.field.access)(second);
        for order in &self.orders {
            let ord = order.compare(emb_first, emb_second);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    }
}
impl<P, V> ScanOrderStep<P> for EmbeddedOrder<P, V> {
    fn scan(&self, _structsy: Structsy) -> Option<Iter<'static, P>> {
        None
    }
    fn scan_tx<'a>(&self, _tx: &'a mut OwnedSytx) -> Option<Iter<'a, P>> {
        None
    }
    fn scan_snapshot(&self, _snapshot: &Snapshot) -> Option<Iter<'static, P>> {
        None
    }
    fn is_indexed(&self) -> bool {
        false
    }
}

trait BufferedExection<P> {
    fn next(&mut self, source: &mut dyn Source<P>) -> Option<Item<P>>;
}
struct BufferedOrderExecution<T> {
    buffer: Option<std::vec::IntoIter<Item<T>>>,
    orders: Vec<Box<dyn ScanOrderStep<T>>>,
}
impl<T: 'static> BufferedOrderExecution<T> {
    fn new(orders: Vec<Box<dyn ScanOrderStep<T>>>) -> Box<dyn BufferedExection<T>> {
        Box::new(BufferedOrderExecution { buffer: None, orders })
    }
}

impl<T> BufferedOrderExecution<T> {
    fn order_item(&self, first: &Item<T>, second: &Item<T>) -> std::cmp::Ordering {
        for order in &self.orders {
            let ord = order.compare(&first.record, &second.record);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl<T> BufferedExection<T> for BufferedOrderExecution<T> {
    fn next(&mut self, source: &mut dyn Source<T>) -> Option<Item<T>> {
        if let Some(b) = &mut self.buffer {
            b.next()
        } else {
            let mut buffer = Vec::<Item<T>>::new();
            while let Some(item) = source.next_item() {
                let index = match buffer.binary_search_by(|e| self.order_item(e, &item)) {
                    Ok(index) => index,
                    Err(index) => index,
                };
                buffer.insert(index, item);
            }
            self.buffer = Some(buffer.into_iter());
            self.buffer.as_mut().unwrap().next()
        }
    }
}

struct Orders<T> {
    order: Vec<Box<dyn ScanOrderStep<T>>>,
}
impl<T: 'static> Orders<T> {
    fn buffered(self) -> Option<Box<dyn BufferedExection<T>>> {
        if self.order.is_empty() {
            None
        } else {
            Some(BufferedOrderExecution::new(self.order))
        }
    }
}
impl<T: Persistent + 'static> Orders<T> {
    fn scan(self, structsy: Structsy) -> (Option<Box<dyn BufferedExection<T>>>, Option<Iter<'static, T>>) {
        if self.order.is_empty() {
            return (None, None);
        }
        let mut orders = self.order;
        let first_entry = orders.remove(0);
        let scan = first_entry.scan(structsy);
        if let Some(iter) = scan {
            if orders.is_empty() {
                (None, Some(iter))
            } else {
                (Some(BufferedOrderExecution::new(orders)), Some(iter))
            }
        } else {
            orders.insert(0, first_entry);
            (Some(BufferedOrderExecution::new(orders)), None)
        }
    }

    fn index_order(&self) -> bool {
        if let Some(first) = self.order.first() {
            first.is_indexed()
        } else {
            false
        }
    }

    fn scan_tx(self, tx: &mut OwnedSytx) -> (Option<Box<dyn BufferedExection<T>>>, Option<Iter<T>>) {
        if self.order.is_empty() {
            return (None, None);
        }
        let mut orders = self.order;
        let first_entry = orders.remove(0);
        let scan = first_entry.scan_tx(tx);
        if let Some(iter) = scan {
            if orders.is_empty() {
                (None, Some(iter))
            } else {
                (Some(BufferedOrderExecution::new(orders)), Some(iter))
            }
        } else {
            orders.insert(0, first_entry);
            (Some(BufferedOrderExecution::new(orders)), None)
        }
    }
    fn scan_snapshot(self, snapshot: &Snapshot) -> (Option<Box<dyn BufferedExection<T>>>, Option<Iter<'static, T>>) {
        if self.order.is_empty() {
            return (None, None);
        }
        let mut orders = self.order;
        let first_entry = orders.remove(0);
        let scan = first_entry.scan_snapshot(snapshot);
        if let Some(iter) = scan {
            if orders.is_empty() {
                (None, Some(iter))
            } else {
                (Some(BufferedOrderExecution::new(orders)), Some(iter))
            }
        } else {
            orders.insert(0, first_entry);
            (Some(BufferedOrderExecution::new(orders)), None)
        }
    }
}

pub struct FilterBuilder<T> {
    steps: Vec<Box<dyn FilterBuilderStep<Target = T>>>,
    order: Orders<T>,
}
impl<T: 'static> Default for FilterBuilder<T> {
    fn default() -> Self {
        FilterBuilder::new()
    }
}

impl<T: 'static> FilterBuilder<T> {
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder {
            steps: Vec::new(),
            order: Orders { order: Vec::new() },
        }
    }

    fn add<F: FilterBuilderStep<Target = T> + 'static>(&mut self, filter: F) {
        self.steps.push(Box::new(filter));
    }

    fn add_order<O: ScanOrderStep<T> + 'static>(&mut self, order: O) {
        self.order.order.push(Box::new(order))
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

    pub(crate) fn fill_conditions_step(self, reader: &mut Reader) -> Conditions<T> {
        let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
        executions.sort_by_key(|x| x.get_score());
        Self::fill_conditions(executions)
    }

    fn fill_conditions(executions: Vec<Box<dyn ExecutionStep<Target = T>>>) -> Conditions<T> {
        Conditions { conditions: executions }
    }

    pub fn finish<'a>(self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            let start = Box::new(ScanStartStep::new());
            let cond = Self::fill_conditions(Vec::new());
            Box::new(start.start(cond, self.order, structsy.clone()))
        } else {
            let reader = &mut Reader::Structsy(structsy.clone());
            let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
            executions.sort_by_key(|x| x.get_score());
            let (step, start) = executions.pop().unwrap().as_start();
            if let Some(es) = step {
                executions.insert(0, es);
            }
            let cond = Self::fill_conditions(executions);
            Box::new(start.start(cond, self.order, structsy.clone()))
        }
    }

    pub fn finish_tx<'a>(self, tx: &'a mut OwnedSytx) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        if self.steps.is_empty() {
            let start = Box::new(ScanStartStep::new());
            let cond = Self::fill_conditions(Vec::new());
            Box::new(start.start_tx(cond, self.order, tx))
        } else {
            let reader = &mut Reader::Tx(tx.reference());
            let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
            let (step, start) = executions.pop().unwrap().as_start();
            if let Some(es) = step {
                executions.insert(0, es);
            }
            let cond = Self::fill_conditions(executions);
            Box::new(start.start_tx(cond, self.order, tx))
        }
    }

    pub fn finish_snap(self, tx: &Snapshot) -> Box<dyn Iterator<Item = (Ref<T>, T)>> {
        if self.steps.is_empty() {
            let start = Box::new(ScanStartStep::new());
            let cond = Self::fill_conditions(Vec::new());
            Box::new(start.start_snapshot(cond, self.order, tx))
        } else {
            let reader = &mut Reader::Snapshot(tx.clone());
            let mut executions = self.steps.into_iter().map(|e| e.prepare(reader)).collect::<Vec<_>>();
            let (step, start) = executions.pop().unwrap().as_start();
            if let Some(es) = step {
                executions.insert(0, es);
            }
            let cond = Self::fill_conditions(executions);
            Box::new(start.start_snapshot(cond, self.order, tx))
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

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilterBuilder<V>)
    where
        V: PersistentEmbedded + 'static,
    {
        let (conditions, order) = filter.components();
        self.add_order(EmbeddedOrder::new(field.clone(), order));
        self.add(EmbeddedFieldFilter::new(conditions, field))
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        self.add(QueryFilter::new(query, field))
    }

    pub fn ref_vec_query<V>(&mut self, field: Field<T, Vec<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        self.add(VecQueryFilter::new(query, field))
    }

    pub fn ref_option_query<V>(&mut self, field: Field<T, Option<Ref<V>>>, query: FilterBuilder<V>)
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

    pub fn and_filter(&mut self, mut filters: FilterBuilder<T>) {
        for order in filters.order.order.drain(..) {
            self.order.order.push(order);
        }
        self.add(AndFilter::new(filters));
    }

    pub fn not(&mut self, filters: StructsyFilter<T>) {
        self.add(NotFilter::new(filters.filter()))
    }

    pub fn order<V: Ord + 'static + Scan<T>>(&mut self, field: Field<T, V>, order: Order) {
        self.add_order(FieldOrder::new(field, order))
    }
}
