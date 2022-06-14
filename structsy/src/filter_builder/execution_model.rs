use crate::{
    filter_builder::{
        plan_model::{
            FieldPathPlan, FilterByPlan, FilterPlan, FilterPlanItem, FilterPlanMode, OrderPlanItem, QueryPlan,
            QueryValuePlan, Source,
        },
        query_model::RawRef,
        reader::{Reader, ReaderIterator},
        value_compare::{ValueCompare, ValueRange},
    },
    internal::{Field, FieldInfo},
    Persistent, Ref, SRes,
};
use std::{collections::HashMap, ops::Bound};

fn start<'a, T: Persistent + 'static>(
    source: Source,
    reader: Reader<'a>,
) -> SRes<Box<dyn ReaderIterator<Item = (Ref<T>, T)> + 'a>> {
    Ok(match source {
        Source::Index(index) => reader.find_range_from_info(index)?,
        Source::Scan(_scan) => Box::new(reader.scan()?),
    })
}

fn field_to_compare_operations<T>(
    field: &FieldPathPlan,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> Rc<dyn CompareOperations<T>> {
    access.nested_compare_operations(field.field_path_names())
}

fn filter_plan_field_to_execution<T>(
    plan: FilterFieldPlanItem,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> FilterExecutionField<T> {
    FilterExecutionField {
        field: field_to_compare_operations(&plan.field, access.clone()),
        filter_by: filter_by_to_execution(plan.filter_by, &plan.field, access),
    }
}
fn filter_by_to_execution<T>(
    filter: FilterByPlan,
    field: &FieldPathPlan,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> FilterExecutionByPlan {
    match filter {
        FilterByPlan::Equal(v) => FilterExecutionByPlan::Equal(v),
        FilterByPlan::Is(v) => FilterExecutionByPlan::Is(v),
        FilterByPlan::Contains(v) => FilterExecutionByPlan::Contains(v),
        FilterByPlan::Range(v) => FilterExecutionByPlan::Range(v),
        FilterByPlan::RangeIs(v) => FilterExecutionByPlan::RangeIs(v),
        FilterByPlan::RangeContains(v) => FilterExecutionByPlan::RangeContains(v),
        FilterByPlan::LoadAndEqual(v) => {
            FilterExecutionByPlan::LoadAndEqual(filter_by_query_to_execution(v, field, access))
        }
        FilterByPlan::LoadAndIs(v) => FilterExecutionByPlan::LoadAndIs(filter_by_query_to_execution(v, field, access)),
        FilterByPlan::LoadAndContains(v) => {
            FilterExecutionByPlan::LoadAndContains(filter_by_query_to_execution(v, field, access))
        }
    }
}
fn filter_by_query_to_execution<T>(
    fp: FilterPlan,
    field: &FieldPathPlan,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> LoadExecution {
    LoadExecution {
        ops: access.nested_ref_operations(field.field_path_names(), fp),
    }
}

fn filter_plan_item_to_execution<T>(
    plan: FilterPlanItem,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> FilterExecutionItem<T> {
    match plan {
        FilterPlanItem::Field(f) => FilterExecutionItem::Field(filter_plan_field_to_execution(f, access)),
        FilterPlanItem::Group(plan) => FilterExecutionItem::Group(filter_plan_to_execution(plan, access)),
    }
}
fn filter_plan_to_execution<T>(plan: FilterPlan, access: Rc<dyn IntoCompareOperations<T>>) -> FilterExecutionGroup<T> {
    let values = plan
        .filters
        .into_iter()
        .map(move |v| filter_plan_item_to_execution(v, access.clone()))
        .collect();
    FilterExecutionGroup {
        conditions: values,
        mode: plan.mode,
    }
}
fn execute<'a, T: Persistent + 'static>(
    plan: QueryPlan,
    fields: Rc<dyn IntoCompareOperations<T>>,
    reader: Reader<'a>,
) -> SRes<Box<dyn ReaderIterator<Item = (Ref<T>, T)> + 'a>> {
    let QueryPlan {
        source,
        filter,
        orders,
        projections,
    } = plan;

    let iter = start::<T>(source, reader)?;
    let iter = if let Some(f) = filter {
        Box::new(FilterExecution {
            source: iter,
            filter: filter_plan_to_execution(f, fields),
        })
    } else {
        iter
    };
    let iter = if let Some(o) = orders {
        if !o.orders.is_empty() {
            Box::new(Accumulator::new(iter, o.orders))
        } else {
            iter
        }
    } else {
        iter
    };

    Ok(iter)
}

struct FilterExecution<'a, T> {
    source: Box<dyn ReaderIterator<Item = (Ref<T>, T)> + 'a>,
    filter: FilterExecutionGroup<T>,
}

impl<'b, T> ReaderIterator for FilterExecution<'b, T> {
    fn reader<'a>(&'a mut self) -> Reader<'a> {
        self.source.reader()
    }
}

trait FilterCheck<T> {
    fn check(&self, value: &T, reader: &mut Reader) -> bool;
}

struct FilterExecutionGroup<T> {
    conditions: Vec<FilterExecutionItem<T>>,
    mode: FilterPlanMode,
}
impl<T> FilterCheck<T> for FilterExecutionGroup<T> {
    fn check(&self, value: &T, reader: &mut Reader) -> bool {
        match self.mode {
            FilterPlanMode::And => {
                let mut cond = true;
                for con in &self.conditions {
                    if !con.check(value, reader) {
                        cond = false;
                        break;
                    }
                }
                cond
            }
            FilterPlanMode::Or => {
                let mut cond = false;
                for con in &self.conditions {
                    if con.check(value, reader) {
                        cond = true;
                        break;
                    }
                }
                cond
            }
            FilterPlanMode::Not => {
                let mut cond = true;
                for con in &self.conditions {
                    if !con.check(value, reader) {
                        cond = false;
                        break;
                    }
                }
                !cond
            }
        }
    }
}

enum FilterExecutionItem<T> {
    Field(FilterExecutionField<T>),
    Group(FilterExecutionGroup<T>),
}
struct FilterExecutionField<T> {
    field: Rc<dyn CompareOperations<T>>,
    filter_by: FilterExecutionByPlan,
}
struct LoadExecution {
    ops: Rc<dyn RefOperations>,
}
enum FilterExecutionByPlan {
    Equal(QueryValuePlan),
    Contains(QueryValuePlan),
    Is(QueryValuePlan),
    Range((Bound<QueryValuePlan>, Bound<QueryValuePlan>)),
    RangeContains((Bound<QueryValuePlan>, Bound<QueryValuePlan>)),
    RangeIs((Bound<QueryValuePlan>, Bound<QueryValuePlan>)),
    LoadAndEqual(LoadExecution),
    LoadAndContains(LoadExecution),
    LoadAndIs(LoadExecution),
}
impl<T> FilterCheck<T> for FilterExecutionItem<T> {
    fn check(&self, value: &T, reader: &mut Reader) -> bool {
        match self {
            Self::Field(f) => f.check(value, reader),
            Self::Group(g) => g.check(value, reader),
        }
    }
}

impl<T> FilterCheck<T> for FilterExecutionField<T> {
    fn check(&self, rec: &T, reader: &mut Reader) -> bool {
        match &self.filter_by {
            FilterExecutionByPlan::Equal(value) => self.field.equals(rec, value.clone()),
            FilterExecutionByPlan::Contains(value) => self.field.contains(rec, value.clone()),
            FilterExecutionByPlan::Is(value) => self.field.is(rec, value.clone()),
            FilterExecutionByPlan::Range(value) => self.field.range(rec, value.clone()),
            FilterExecutionByPlan::RangeContains(value) => self.field.range_contains(rec, value.clone()),
            FilterExecutionByPlan::RangeIs(value) => self.field.range_is(rec, value.clone()),
            FilterExecutionByPlan::LoadAndEqual(value) => self.field.query_equals(rec, &*value.ops, reader),
            FilterExecutionByPlan::LoadAndContains(value) => self.field.query_contains(rec, &*value.ops, reader),
            FilterExecutionByPlan::LoadAndIs(value) => self.field.query_is(rec, &*value.ops, reader),
        }
    }
}

impl<'a, T> Iterator for FilterExecution<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.source
            .next()
            .filter(|(_id, rec)| self.filter.check(&rec, &mut self.source.reader()))
    }
}

struct Accumulator<'a, T> {
    source: Box<dyn ReaderIterator<Item = (Ref<T>, T)> + 'a>,
    orders: Vec<OrderPlanItem>,
    buffer: Option<Box<dyn Iterator<Item = (Ref<T>, T)>>>,
}
impl<'a, T> Accumulator<'a, T> {
    fn new(source: Box<dyn ReaderIterator<Item = (Ref<T>, T)> + 'a>, orders: Vec<OrderPlanItem>) -> Self {
        Self {
            source,
            orders,
            buffer: Default::default(),
        }
    }
    fn order_item(&self, first: &T, second: &T) -> std::cmp::Ordering {
        for order in &self.orders {
            let ord = todo!(); //order.compare(&first, &second);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    }
}
impl<'b, T: 'static> ReaderIterator for Accumulator<'b, T> {
    fn reader<'a>(&'a mut self) -> Reader<'a> {
        self.source.reader()
    }
}
impl<'a, T: 'static> Iterator for Accumulator<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.buffer {
            iter.next()
        } else {
            let mut buffer = Vec::<(Ref<T>, T)>::new();
            while let Some(item) = self.source.next() {
                let index = match buffer.binary_search_by(|(_, e)| self.order_item(e, &item.1)) {
                    Ok(index) => index,
                    Err(index) => index,
                };
                buffer.insert(index, item);
            }
            self.buffer = Some(Box::new(buffer.into_iter()));
            self.buffer.as_mut().unwrap().next()
        }
    }
}

use std::rc::Rc;

use super::plan_model::FilterFieldPlanItem;
struct PathStep<T, V> {
    field: Field<T, V>,
    next: Rc<dyn CompareOperations<V>>,
}

trait IntoCompareOperations<T> {
    fn nested_compare_operations(&self, fields: Vec<String>) -> Rc<dyn CompareOperations<T>>;
    fn nested_ref_operations(&self, fields: Vec<String>, filter_plan: FilterPlan) -> Rc<dyn RefOperations>;
}

trait IntoFieldStep<T> {
    fn build_step(&self, next: Rc<dyn CompareOperations<T>>) -> Rc<dyn CompareOperations<T>>;
}

struct FieldEmbedded<T, V> {
    field: Field<T, V>,
    embeedded: FieldsHolder<V>,
}
impl<T: 'static, V: 'static> IntoCompareOperations<T> for FieldEmbedded<T, V> {
    fn nested_compare_operations(&self, fields: Vec<String>) -> Rc<dyn CompareOperations<T>> {
        Rc::new(PathStep {
            field: self.field.clone(),
            next: self.embeedded.nested_compare_operations(fields),
        })
    }
    fn nested_ref_operations(&self, fields: Vec<String>, filter_plan: FilterPlan) -> Rc<dyn RefOperations> {
        self.embeedded.nested_ref_operations(fields, filter_plan)
    }
}

enum TypedField<T> {
    Embedded(Rc<dyn IntoCompareOperations<T>>),
    EmbeddedCompare((Rc<dyn IntoCompareOperations<T>>, Rc<dyn CompareOperations<T>>)),
    EmbeddedRange((Rc<dyn IntoCompareOperations<T>>, Rc<dyn CompareOperations<T>>)),
    SimpleCompare(Rc<dyn CompareOperations<T>>),
    SimpleRange(Rc<dyn CompareOperations<T>>),
    Ref((Rc<dyn Query<T>>, Rc<dyn CompareOperations<T>>)),
}
impl<T> Clone for TypedField<T> {
    fn clone(&self) -> Self {
        match self {
            Self::SimpleCompare(eq) => Self::SimpleCompare(eq.clone()),
            Self::SimpleRange(or) => Self::SimpleRange(or.clone()),
            Self::Embedded(v) => Self::Embedded(v.clone()),
            Self::EmbeddedCompare(v) => Self::EmbeddedCompare(v.clone()),
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v.clone()),
            Self::Ref(v) => Self::Ref(v.clone()),
        }
    }
}

impl<T: 'static> TypedField<T> {
    fn embedded<V: 'static>(group: FieldEmbedded<T, V>) -> Self {
        Self::Embedded(Rc::new(group))
    }
    fn simple_compare(field: Rc<dyn CompareOperations<T>>) -> Self {
        Self::SimpleCompare(field)
    }
    fn simple_range(field: Rc<dyn CompareOperations<T>>) -> Self {
        Self::SimpleRange(field)
    }
    fn simple_ref(r: Rc<dyn Query<T>>, c: Rc<dyn CompareOperations<T>>) -> Self {
        Self::Ref((r, c))
    }
    fn replace_embedded<V: 'static>(&mut self, group: FieldEmbedded<T, V>) {
        *self = match self.clone() {
            Self::SimpleCompare(eq) => Self::EmbeddedCompare((Rc::new(group), eq)),
            Self::SimpleRange(or) => Self::EmbeddedRange((Rc::new(group), or)),
            Self::Embedded(v) => Self::Embedded(v),
            Self::EmbeddedCompare(v) => Self::EmbeddedCompare(v),
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
            Self::Ref(_) => unreachable!(),
        };
    }
    fn replace_simple_compare(&mut self, field: Rc<dyn CompareOperations<T>>) {
        *self = match self.clone() {
            Self::SimpleCompare(eq) => Self::SimpleCompare(eq),
            Self::SimpleRange(or) => Self::SimpleRange(or),
            Self::Embedded(n) => Self::EmbeddedCompare((n, field)),
            Self::EmbeddedCompare(v) => Self::EmbeddedCompare(v),
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
            Self::Ref(v) => Self::Ref(v),
        };
    }
    fn replace_simple_range(&mut self, field: Rc<dyn CompareOperations<T>>) {
        *self = match self.clone() {
            Self::SimpleCompare(_) => Self::SimpleRange(field),
            Self::SimpleRange(or) => Self::SimpleRange(or),
            Self::Embedded(g) => Self::EmbeddedRange((g, field)),
            Self::EmbeddedCompare((g, _)) => Self::EmbeddedRange((g, field)),
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
            Self::Ref(v) => Self::Ref(v),
        };
    }
    fn replace_simple_ref(&mut self, field: Rc<dyn Query<T>>, c: Rc<dyn CompareOperations<T>>) {
        *self = Self::Ref((field, c))
    }
    fn merge(&mut self, other: Self) {
        *self = match self.clone() {
            Self::SimpleCompare(v) => match other {
                Self::SimpleCompare(_) => Self::SimpleCompare(v),
                Self::SimpleRange(or) => Self::SimpleRange(or),
                Self::Embedded(g) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedCompare((g, _)) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
                Self::Ref(v) => Self::Ref(v),
            },
            Self::SimpleRange(or) => match other {
                Self::SimpleCompare(_) => Self::SimpleRange(or),
                Self::SimpleRange(_) => Self::SimpleRange(or),
                Self::Embedded(g) => Self::EmbeddedRange((g, or)),
                Self::EmbeddedCompare((g, _)) => Self::EmbeddedRange((g, or)),
                Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
                Self::Ref(v) => Self::Ref(v),
            },
            Self::Embedded(g) => match other {
                Self::SimpleCompare(v) => Self::EmbeddedCompare((g, v)),
                Self::SimpleRange(r) => Self::EmbeddedRange((g, r)),
                Self::Embedded(_) => Self::Embedded(g),
                Self::EmbeddedCompare((_, v)) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
                Self::Ref(_) => unreachable!(),
            },
            Self::EmbeddedCompare((g, v)) => match other {
                Self::SimpleCompare(_) => Self::EmbeddedCompare((g, v)),
                Self::SimpleRange(r) => Self::EmbeddedRange((g, r)),
                Self::Embedded(_) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedCompare((_, _)) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
                Self::Ref(_) => unreachable!(),
            },
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
            Self::Ref(v) => Self::Ref(v),
        };
    }
}

impl<T: 'static> IntoCompareOperations<T> for TypedField<T> {
    fn nested_compare_operations(&self, fields: Vec<String>) -> Rc<dyn CompareOperations<T>> {
        match &self {
            Self::Embedded(n) => n.nested_compare_operations(fields),
            Self::EmbeddedCompare((n, l)) => {
                if fields.is_empty() {
                    l.clone()
                } else {
                    n.nested_compare_operations(fields)
                }
            }
            Self::EmbeddedRange((n, l)) => {
                if fields.is_empty() {
                    l.clone()
                } else {
                    n.nested_compare_operations(fields)
                }
            }
            Self::SimpleCompare(l) => {
                assert!(fields.is_empty());
                l.clone()
            }
            Self::SimpleRange(l) => {
                assert!(fields.is_empty());
                l.clone()
            }
            Self::Ref((_q, c)) => {
                assert!(fields.is_empty());
                c.clone()
            }
        }
    }
    fn nested_ref_operations(&self, _fields: Vec<String>, filter_plan: FilterPlan) -> Rc<dyn RefOperations> {
        match &self {
            Self::Ref((q, _c)) => q.query(filter_plan),
            _ => unreachable!(),
        }
    }
}

pub(crate) struct FieldsHolder<V> {
    fields: HashMap<String, TypedField<V>>,
}
impl<V> Clone for FieldsHolder<V> {
    fn clone(&self) -> Self {
        Self {
            fields: self.fields.clone(),
        }
    }
}

impl<T: 'static> FieldsHolder<T> {
    pub(crate) fn add_field_ref<X: Persistent + 'static>(&mut self, field: Field<T, Ref<X>>, holder: FieldsHolder<X>) {
        use std::collections::hash_map::Entry;
        match self.fields.entry(field.name().to_owned()) {
            Entry::Vacant(v) => {
                v.insert(TypedField::<T>::simple_ref(
                    Rc::new(FieldValueRef(field.clone(), holder.clone())),
                    Rc::new(FieldValueRef(field, holder)),
                ));
            }
            Entry::Occupied(mut o) => o.get_mut().replace_simple_ref(
                Rc::new(FieldValueRef(field.clone(), holder.clone())),
                Rc::new(FieldValueRef(field, holder)),
            ),
        }
    }
    pub(crate) fn add_field_vec_ref<X: Persistent + 'static>(
        &mut self,
        field: Field<T, Vec<Ref<X>>>,
        holder: FieldsHolder<X>,
    ) {
        use std::collections::hash_map::Entry;
        match self.fields.entry(field.name().to_owned()) {
            Entry::Vacant(v) => {
                v.insert(TypedField::<T>::simple_ref(
                    Rc::new(FieldValueVecRef(field.clone(), holder.clone())),
                    Rc::new(FieldValueVecRef(field, holder)),
                ));
            }
            Entry::Occupied(mut o) => o.get_mut().replace_simple_ref(
                Rc::new(FieldValueVecRef(field.clone(), holder.clone())),
                Rc::new(FieldValueVecRef(field, holder)),
            ),
        }
    }
    pub(crate) fn add_field_option_ref<X: Persistent + 'static>(
        &mut self,
        field: Field<T, Option<Ref<X>>>,
        holder: FieldsHolder<X>,
    ) {
        use std::collections::hash_map::Entry;
        match self.fields.entry(field.name().to_owned()) {
            Entry::Vacant(v) => {
                v.insert(TypedField::<T>::simple_ref(
                    Rc::new(FieldValueOptionRef(field.clone(), holder.clone())),
                    Rc::new(FieldValueOptionRef(field, holder)),
                ));
            }
            Entry::Occupied(mut o) => o.get_mut().replace_simple_ref(
                Rc::new(FieldValueOptionRef(field.clone(), holder.clone())),
                Rc::new(FieldValueOptionRef(field, holder)),
            ),
        }
    }
    pub(crate) fn add_field<V: ValueCompare + 'static>(&mut self, field: Field<T, V>) {
        use std::collections::hash_map::Entry;
        match self.fields.entry(field.name().to_owned()) {
            Entry::Vacant(v) => {
                v.insert(TypedField::<T>::simple_compare(Rc::new(FieldValueCompare(field))));
            }
            Entry::Occupied(mut o) => o.get_mut().replace_simple_compare(Rc::new(FieldValueCompare(field))),
        }
    }

    pub(crate) fn add_field_ord<V: ValueRange + 'static>(&mut self, field: Field<T, V>) {
        use std::collections::hash_map::Entry;
        match self.fields.entry(field.name().to_owned()) {
            Entry::Vacant(v) => {
                v.insert(TypedField::<T>::simple_range(Rc::new(FieldValueRange(field))));
            }
            Entry::Occupied(mut o) => o.get_mut().replace_simple_range(Rc::new(FieldValueRange(field))),
        }
    }
    pub(crate) fn add_nested_field<V: 'static>(&mut self, field: Field<T, V>, holder: FieldsHolder<V>) {
        use std::collections::hash_map::Entry;
        match self.fields.entry(field.name().to_owned()) {
            Entry::Vacant(v) => {
                v.insert(TypedField::<T>::embedded(FieldEmbedded {
                    field,
                    embeedded: holder,
                }));
            }
            Entry::Occupied(mut o) => o.get_mut().replace_embedded(FieldEmbedded {
                field,
                embeedded: holder,
            }),
        }
    }
    pub(crate) fn merge(&mut self, other: FieldsHolder<T>) {
        use std::collections::hash_map::Entry;
        for (name, field) in other.fields {
            match self.fields.entry(name) {
                Entry::Vacant(v) => {
                    v.insert(field);
                }
                Entry::Occupied(mut o) => o.get_mut().merge(field),
            }
        }
    }
}

impl<V> Default for FieldsHolder<V> {
    fn default() -> Self {
        Self {
            fields: Default::default(),
        }
    }
}

impl<T: 'static> IntoCompareOperations<T> for FieldsHolder<T> {
    fn nested_compare_operations(&self, mut fields: Vec<String>) -> Rc<dyn CompareOperations<T>> {
        let field = fields.pop();
        if let Some(f) = field {
            self.fields.get(&f).unwrap().nested_compare_operations(fields)
        } else {
            unreachable!()
        }
    }
    fn nested_ref_operations(&self, mut fields: Vec<String>, filter_plan: FilterPlan) -> Rc<dyn RefOperations> {
        let field = fields.pop();
        if let Some(f) = field {
            self.fields.get(&f).unwrap().nested_ref_operations(fields, filter_plan)
        } else {
            unreachable!()
        }
    }
}

impl<T, V> CompareOperations<T> for PathStep<T, V> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool {
        self.next.equals((self.field.access)(t), value)
    }
    fn contains(&self, t: &T, value: QueryValuePlan) -> bool {
        self.next.contains((self.field.access)(t), value)
    }
    fn is(&self, t: &T, value: QueryValuePlan) -> bool {
        self.next.is((self.field.access)(t), value)
    }
    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        self.next.range((self.field.access)(t), value)
    }
    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        self.next.range_contains((self.field.access)(t), value)
    }
    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        self.next.range_is((self.field.access)(t), value)
    }
    fn query_equals(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool {
        self.next.query_equals((self.field.access)(t), value, reader)
    }
    fn query_contains(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool {
        self.next.query_contains((self.field.access)(t), value, reader)
    }
    fn query_is(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool {
        self.next.query_is((self.field.access)(t), value, reader)
    }
}

pub(crate) trait CompareOperations<T> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool;
    fn contains(&self, t: &T, value: QueryValuePlan) -> bool;
    fn is(&self, t: &T, value: QueryValuePlan) -> bool;
    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn query_equals(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool;
    fn query_contains(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool;
    fn query_is(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool;
}

pub(crate) trait RefOperations {
    fn equals(&self, value: RawRef, reader: &mut Reader) -> bool;
}
pub(crate) trait RefBuildOperations<T>: CompareOperations<T> {
    fn operation(&self, filter: &FilterPlan) -> dyn RefOperations;
}

struct FieldValueRef<T, X>(Field<T, Ref<X>>, FieldsHolder<X>);
struct FieldValueVecRef<T, X>(Field<T, Vec<Ref<X>>>, FieldsHolder<X>);
struct FieldValueOptionRef<T, X>(Field<T, Option<Ref<X>>>, FieldsHolder<X>);
struct FieldValueCompare<T, V>(Field<T, V>);
struct FieldValueRange<T, V>(Field<T, V>);

impl<T, V: ValueCompare> CompareOperations<T> for FieldValueCompare<T, V> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).equals(value)
    }

    fn contains(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).contains_value(value)
    }
    fn is(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).is(value)
    }

    fn range(&self, _t: &T, _value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        false
    }

    fn range_contains(&self, _t: &T, _value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        false
    }

    fn range_is(&self, _t: &T, _value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        false
    }

    fn query_equals(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
    fn query_contains(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
    fn query_is(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
}

impl<T, V: ValueRange> CompareOperations<T> for FieldValueRange<T, V> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).equals(value)
    }

    fn contains(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).contains_value(value)
    }
    fn is(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).is(value)
    }

    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range(value)
    }

    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range_contains(value)
    }

    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range_is(value)
    }

    fn query_equals(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
    fn query_contains(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
    fn query_is(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
}

impl<T, X: Persistent> CompareOperations<T> for FieldValueRef<T, X> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).equals(value)
    }

    fn contains(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).contains_value(value)
    }
    fn is(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).is(value)
    }

    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range(value)
    }

    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range_contains(value)
    }

    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range_is(value)
    }

    fn query_equals(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool {
        value.equals(RawRef::from((self.0.access)(t)), reader)
    }
    fn query_contains(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
    fn query_is(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
}

impl<T, X: Persistent> CompareOperations<T> for FieldValueVecRef<T, X> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).equals(value)
    }

    fn contains(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).contains_value(value)
    }
    fn is(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).is(value)
    }

    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range(value)
    }

    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range_contains(value)
    }

    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range_is(value)
    }

    fn query_equals(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
    fn query_contains(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool {
        for r in (self.0.access)(t) {
            if value.equals(RawRef::from(r), reader) {
                return true;
            }
        }
        false
    }
    fn query_is(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
}

impl<T, X: Persistent> CompareOperations<T> for FieldValueOptionRef<T, X> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).equals(value)
    }

    fn contains(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).contains_value(value)
    }
    fn is(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.0.access)(t).is(value)
    }

    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range(value)
    }

    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range_contains(value)
    }

    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.0.access)(t).range_is(value)
    }

    fn query_equals(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
    fn query_contains(&self, _t: &T, _value: &dyn RefOperations, _reader: &mut Reader) -> bool {
        false
    }
    fn query_is(&self, t: &T, value: &dyn RefOperations, reader: &mut Reader) -> bool {
        if let Some(r) = (self.0.access)(t) {
            value.equals(RawRef::from(r), reader)
        } else {
            false
        }
    }
}

trait Query<T>: CompareOperations<T> {
    fn query(&self, fp: FilterPlan) -> Rc<dyn RefOperations>;
}
impl<T, X: Persistent + 'static> Query<T> for FieldValueRef<T, X> {
    fn query(&self, fp: FilterPlan) -> Rc<dyn RefOperations> {
        let access: Rc<dyn IntoCompareOperations<X>> = Rc::new(self.1.clone());
        Rc::new(LinkQuery {
            filter: filter_plan_to_execution(fp, access),
        })
    }
}
impl<T, X: Persistent + 'static> Query<T> for FieldValueVecRef<T, X> {
    fn query(&self, fp: FilterPlan) -> Rc<dyn RefOperations> {
        let access: Rc<dyn IntoCompareOperations<X>> = Rc::new(self.1.clone());
        Rc::new(LinkQuery {
            filter: filter_plan_to_execution(fp, access),
        })
    }
}
impl<T, X: Persistent + 'static> Query<T> for FieldValueOptionRef<T, X> {
    fn query(&self, fp: FilterPlan) -> Rc<dyn RefOperations> {
        let access: Rc<dyn IntoCompareOperations<X>> = Rc::new(self.1.clone());
        Rc::new(LinkQuery {
            filter: filter_plan_to_execution(fp, access),
        })
    }
}

struct LinkQuery<T> {
    filter: FilterExecutionGroup<T>,
}

impl<T: Persistent> RefOperations for LinkQuery<T> {
    fn equals(&self, value: RawRef, reader: &mut Reader) -> bool {
        if let Ok(Some(record)) = reader.read(&value.into_ref::<T>()) {
            self.filter.check(&record, reader)
        } else {
            false
        }
    }
}
