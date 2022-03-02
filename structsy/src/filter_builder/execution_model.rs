use crate::{
    filter_builder::{
        plan_model::{FieldPathPlan, FilterPlan, QueryPlan, QueryValuePlan, Source},
        reader::Reader,
    },
    internal::Field,
    Order, Persistent, Ref, SRes,
};
use std::{collections::HashMap, ops::Bound};

fn start<'a, T: Persistent + 'static>(
    source: Source,
    reader: Reader<'a>,
) -> SRes<Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>> {
    Ok(match source {
        Source::Index(index) => {
            // TODO: get bound out :index.index_range.unwrap_or(..);
            let found = reader.find_range(&index.index_name, (0u8)..todo!())?;
            if Order::Desc == index.ordering_mode {
                Box::new(found.reader_rev())
            } else {
                Box::new(found)
            }
        }
        Source::Scan(_scan) => Box::new(reader.scan()?),
    })
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
            filter: f,
        })
    } else {
        iter
    };

    Ok(iter)
}

struct FilterExecution<'a, T> {
    source: Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>,
    filter: FilterPlan,
}

trait FilterCheck<T> {
    fn check(&self, value: &T) -> bool;
}

impl<T> FilterCheck<T> for FilterPlan {
    fn check(&self, value: &T) -> bool {
        false
    }
}

impl<'a, T> Iterator for FilterExecution<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.source.next().filter(|v| self.filter.check(&v))
    }
}

struct Accumulator {}

use std::rc::Rc;
struct PathStep<T, V> {
    field: Field<T, V>,
    next: Rc<dyn CompareOperations<V>>,
}
enum FieldPath<T, V> {
    Step(PathStep<T, V>),
    Last(Rc<dyn CompareOperations<T>>),
}

impl<T, V> FieldPath<T, V> {
    fn step(field: Field<T, V>, next: Rc<dyn CompareOperations<V>>) -> Self {
        Self::Step(PathStep { field, next })
    }
    fn last(last: Rc<dyn CompareOperations<T>>) -> Self {
        Self::Last(last)
    }
}

trait IntoCompareOperations<T> {
    fn nested_compare_operations(&self, fields: Vec<String>) -> Rc<dyn CompareOperations<T>>;
}

enum TypedFields<T, V> {
    Holder((Field<T, V>, FieldsHolder<V>)),
    Leaf(Rc<dyn CompareOperations<T>>),
}
impl<T, V> TypedFields<T, V> {
    fn group(field: Field<T, V>, holder: FieldsHolder<V>) -> Self {
        Self::Holder((field, holder))
    }
    fn leaf(field: Rc<dyn CompareOperations<T>>) -> Self {
        Self::Leaf(field)
    }
}

impl<T: 'static, V: 'static> IntoCompareOperations<T> for TypedFields<T, V> {
    fn nested_compare_operations(&self, fields: Vec<String>) -> Rc<dyn CompareOperations<T>> {
        match &self {
            Self::Holder((field, holder)) => {
                Rc::new(FieldPath::step(field.clone(), holder.nested_compare_operations(fields)))
            }
            Self::Leaf(l) => {
                assert!(fields.is_empty());
                l.clone()
            }
        }
    }
}

struct FieldsHolder<V> {
    fields: HashMap<String, Rc<dyn IntoCompareOperations<V>>>,
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

impl<T, V> CompareOperations<T> for FieldPath<T, V> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool {
        match self {
            Self::Step(ps) => ps.next.equals((ps.field.access)(t), value),
            Self::Last(c) => c.equals(t, value),
        }
    }
    fn contains(&self, t: &T, value: QueryValuePlan) -> bool {
        match self {
            Self::Step(ps) => ps.next.contains((ps.field.access)(t), value),
            Self::Last(c) => c.contains(t, value),
        }
    }
    fn is(&self, t: &T, value: QueryValuePlan) -> bool {
        match self {
            Self::Step(ps) => ps.next.is((ps.field.access)(t), value),
            Self::Last(c) => c.is(t, value),
        }
    }
    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        match self {
            Self::Step(ps) => ps.next.range((ps.field.access)(t), value),
            Self::Last(c) => c.range(t, value),
        }
    }
    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        match self {
            Self::Step(ps) => ps.next.range_contains((ps.field.access)(t), value),
            Self::Last(c) => c.range_contains(t, value),
        }
    }
    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        match self {
            Self::Step(ps) => ps.next.range_is((ps.field.access)(t), value),
            Self::Last(c) => c.range_is(t, value),
        }
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

impl<T, V: ValueCompare> CompareOperations<T> for Field<T, V> {
    fn equals(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.access)(t).equals(value)
    }

    fn contains(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.access)(t).contains(value)
    }
    fn is(&self, t: &T, value: QueryValuePlan) -> bool {
        (self.access)(t).is(value)
    }

    fn range(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.access)(t).range(value)
    }

    fn range_contains(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.access)(t).range_contains(value)
    }

    fn range_is(&self, t: &T, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (self.access)(t).range_is(value)
    }
}

pub(crate) trait ValueCompare {
    fn equals(&self, value: QueryValuePlan) -> bool;
    fn contains(&self, value: QueryValuePlan) -> bool;
    fn is(&self, value: QueryValuePlan) -> bool;
    fn range(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_contains(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_is(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
}
