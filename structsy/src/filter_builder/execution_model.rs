use crate::{
    filter_builder::{
        plan_model::{
            FieldPathPlan, FilterByPlan, FilterPlan, FilterPlanItem, FilterPlanMode, QueryPlan, QueryValuePlan, Source,
        },
        reader::Reader,
        value_compare::{ValueCompare, ValueRange},
    },
    internal::{Field, FieldInfo},
    Persistent, Ref, SRes,
};
use std::{collections::HashMap, ops::Bound};

fn start<'a, T: Persistent + 'static>(
    source: Source,
    reader: Reader<'a>,
) -> SRes<Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>> {
    Ok(match source {
        Source::Index(index) => reader.find_range_from_info(index)?,
        Source::Scan(_scan) => Box::new(reader.scan()?),
    })
}

fn field_to_compare_operations<T>(
    field: FieldPathPlan,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> Rc<dyn CompareOperations<T>> {
    access.nested_compare_operations(field.field_path_names())
}

fn filter_plan_field_to_execution<T>(
    plan: FilterFieldPlanItem,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> FilterExecutionField<T> {
    FilterExecutionField {
        field: field_to_compare_operations(plan.field, access),
        filter_by: plan.filter_by,
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
) -> SRes<Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>> {
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

    Ok(iter)
}

struct FilterExecution<'a, T> {
    source: Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>,
    filter: FilterExecutionGroup<T>,
}

trait FilterCheck<T> {
    fn check(&self, value: &T) -> bool;
}

struct FilterExecutionGroup<T> {
    conditions: Vec<FilterExecutionItem<T>>,
    mode: FilterPlanMode,
}
impl<T> FilterCheck<T> for FilterExecutionGroup<T> {
    fn check(&self, value: &T) -> bool {
        match self.mode {
            FilterPlanMode::And => {
                let mut cond = true;
                for con in &self.conditions {
                    if !con.check(value) {
                        cond = false;
                        break;
                    }
                }
                cond
            }
            FilterPlanMode::Or => {
                let mut cond = false;
                for con in &self.conditions {
                    if con.check(value) {
                        cond = true;
                        break;
                    }
                }
                cond
            }
            FilterPlanMode::Not => {
                let mut cond = true;
                for con in &self.conditions {
                    if !con.check(value) {
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
    filter_by: FilterByPlan,
}
impl<T> FilterCheck<T> for FilterExecutionItem<T> {
    fn check(&self, value: &T) -> bool {
        match self {
            Self::Field(f) => f.check(value),
            Self::Group(g) => g.check(value),
        }
    }
}

impl<T> FilterCheck<T> for FilterExecutionField<T> {
    fn check(&self, rec: &T) -> bool {
        match &self.filter_by {
            FilterByPlan::Equal(value) => self.field.equals(rec, value.clone()),
            FilterByPlan::Contains(value) => self.field.contains(rec, value.clone()),
            FilterByPlan::Is(value) => self.field.is(rec, value.clone()),
            FilterByPlan::Range(value) => self.field.range(rec, value.clone()),
            FilterByPlan::RangeContains(value) => self.field.range_contains(rec, value.clone()),
            FilterByPlan::RangeIs(value) => self.field.range_is(rec, value.clone()),
            FilterByPlan::LoadAndEqual(value) => todo!(),
            FilterByPlan::LoadAndContains(value) => todo!(),
            FilterByPlan::LoadAndIs(value) => todo!(),
        }
    }
}

impl<'a, T> Iterator for FilterExecution<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.source.next().filter(|(_id, rec)| self.filter.check(&rec))
    }
}

struct Accumulator {}

use std::rc::Rc;

use super::plan_model::FilterFieldPlanItem;
struct PathStep<T, V> {
    field: Field<T, V>,
    next: Rc<dyn CompareOperations<V>>,
}

trait IntoCompareOperations<T> {
    fn nested_compare_operations(&self, fields: Vec<String>) -> Rc<dyn CompareOperations<T>>;
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
}

enum TypedField<T> {
    Embedded(Rc<dyn IntoCompareOperations<T>>),
    EmbeddedCompare((Rc<dyn IntoCompareOperations<T>>, Rc<dyn CompareOperations<T>>)),
    EmbeddedRange((Rc<dyn IntoCompareOperations<T>>, Rc<dyn CompareOperations<T>>)),
    SimpleCompare(Rc<dyn CompareOperations<T>>),
    SimpleRange(Rc<dyn CompareOperations<T>>),
}
impl<T> Clone for TypedField<T> {
    fn clone(&self) -> Self {
        match self {
            Self::SimpleCompare(eq) => Self::SimpleCompare(eq.clone()),
            Self::SimpleRange(or) => Self::SimpleRange(or.clone()),
            Self::Embedded(v) => Self::Embedded(v.clone()),
            Self::EmbeddedCompare(v) => Self::EmbeddedCompare(v.clone()),
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v.clone()),
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
    fn replace_embedded<V: 'static>(&mut self, group: FieldEmbedded<T, V>) {
        *self = match self.clone() {
            Self::SimpleCompare(eq) => Self::EmbeddedCompare((Rc::new(group), eq)),
            Self::SimpleRange(or) => Self::EmbeddedRange((Rc::new(group), or)),
            Self::Embedded(v) => Self::Embedded(v),
            Self::EmbeddedCompare(v) => Self::EmbeddedCompare(v),
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
        };
    }
    fn replace_simple_compare(&mut self, field: Rc<dyn CompareOperations<T>>) {
        *self = match self.clone() {
            Self::SimpleCompare(eq) => Self::SimpleCompare(eq),
            Self::SimpleRange(or) => Self::SimpleRange(or),
            Self::Embedded(n) => Self::EmbeddedCompare((n, field)),
            Self::EmbeddedCompare(v) => Self::EmbeddedCompare(v),
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
        };
    }
    fn replace_simple_range(&mut self, field: Rc<dyn CompareOperations<T>>) {
        *self = match self.clone() {
            Self::SimpleCompare(_) => Self::SimpleRange(field),
            Self::SimpleRange(or) => Self::SimpleRange(or),
            Self::Embedded(g) => Self::EmbeddedRange((g, field)),
            Self::EmbeddedCompare((g, _)) => Self::EmbeddedRange((g, field)),
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
        };
    }
    fn merge(&mut self, other: Self) {
        *self = match self.clone() {
            Self::SimpleCompare(v) => match other {
                Self::SimpleCompare(_) => Self::SimpleCompare(v),
                Self::SimpleRange(or) => Self::SimpleRange(or),
                Self::Embedded(g) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedCompare((g, _)) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
            },
            Self::SimpleRange(or) => match other {
                Self::SimpleCompare(_) => Self::SimpleRange(or),
                Self::SimpleRange(_) => Self::SimpleRange(or),
                Self::Embedded(g) => Self::EmbeddedRange((g, or)),
                Self::EmbeddedCompare((g, _)) => Self::EmbeddedRange((g, or)),
                Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
            },
            Self::Embedded(g) => match other {
                Self::SimpleCompare(v) => Self::EmbeddedCompare((g, v)),
                Self::SimpleRange(r) => Self::EmbeddedRange((g, r)),
                Self::Embedded(_) => Self::Embedded(g),
                Self::EmbeddedCompare((_, v)) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
            },
            Self::EmbeddedCompare((g, v)) => match other {
                Self::SimpleCompare(_) => Self::EmbeddedCompare((g, v)),
                Self::SimpleRange(r) => Self::EmbeddedRange((g, r)),
                Self::Embedded(_) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedCompare((_, _)) => Self::EmbeddedCompare((g, v)),
                Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
            },
            Self::EmbeddedRange(v) => Self::EmbeddedRange(v),
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
}

pub(crate) trait CompareOperations<T> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool;
    fn contains(&self, t: &T, value: QueryValuePlan) -> bool;
    fn is(&self, t: &T, value: QueryValuePlan) -> bool;
    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
}

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
}
