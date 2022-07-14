use crate::{
    filter_builder::{
        fields_holder::{CompareOperations, IntoCompareOperations, RefOperations},
        plan_model::{
            FieldPathPlan, FilterByPlan, FilterFieldPlanItem, FilterPlan, FilterPlanItem, FilterPlanMode,
            OrderPlanItem, OrdersPlan, QueryPlan, QueryValuePlan, Source,
        },
        reader::{Reader, ReaderIterator},
    },
    Order, Persistent, Ref, SRes,
};
use std::{cmp::Ordering, ops::Bound, rc::Rc};

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
    access.nested_compare_operations(field.reversed_field_path_names())
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
        ops: access.nested_ref_operations(field.reversed_field_path_names(), fp),
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
pub(crate) fn filter_plan_to_execution<T>(
    plan: FilterPlan,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> FilterExecutionGroup<T> {
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

struct OrderItemExcution<T> {
    compare: Rc<dyn CompareOperations<T>>,
    order: Order,
}

fn order_plan_item_to_execution<T>(
    order: OrderPlanItem,
    access: Rc<dyn IntoCompareOperations<T>>,
) -> OrderItemExcution<T> {
    match order {
        OrderPlanItem::Field(f) => OrderItemExcution {
            compare: access.nested_compare_operations(f.field_path.reversed_field_path_names()),
            order: f.mode,
        },
        _ => todo!(),
    }
}

fn order_plan_to_excution<T>(order: OrdersPlan, access: Rc<dyn IntoCompareOperations<T>>) -> Vec<OrderItemExcution<T>> {
    order
        .orders
        .into_iter()
        .map(|item| order_plan_item_to_execution(item, access.clone()))
        .collect()
}

pub(crate) fn execute<'a, T: Persistent + 'static>(
    plan: QueryPlan,
    fields: Rc<dyn IntoCompareOperations<T>>,
    reader: Reader<'a>,
) -> SRes<Box<dyn ReaderIterator<Item = (Ref<T>, T)> + 'a>> {
    let QueryPlan {
        source,
        filter,
        orders,
        //This is not used for now because the projections are based on code generation
        //and do not have algorithms in them yet
        projections: _projections,
    } = plan;

    let iter = start::<T>(source, reader)?;
    let iter = if let Some(f) = filter {
        Box::new(FilterExecution {
            source: iter,
            filter: filter_plan_to_execution(f, fields.clone()),
        })
    } else {
        iter
    };
    let iter = if let Some(o) = orders {
        if !o.orders.is_empty() {
            Box::new(Accumulator::new(iter, order_plan_to_excution(o, fields)))
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

pub(crate) trait FilterCheck<T> {
    fn check(&self, value: &T, reader: &mut Reader) -> bool;
}

pub(crate) struct FilterExecutionGroup<T> {
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
        while let Some((id, rec)) = self.source.next() {
            if self.filter.check(&rec, &mut self.source.reader()) {
                return Some((id, rec));
            }
        }
        None
    }
}

struct Accumulator<'a, T> {
    source: Box<dyn ReaderIterator<Item = (Ref<T>, T)> + 'a>,
    orders: Vec<OrderItemExcution<T>>,
    buffer: Option<Box<dyn Iterator<Item = (Ref<T>, T)>>>,
}
impl<'a, T> Accumulator<'a, T> {
    fn new(source: Box<dyn ReaderIterator<Item = (Ref<T>, T)> + 'a>, orders: Vec<OrderItemExcution<T>>) -> Self {
        Self {
            source,
            orders,
            buffer: Default::default(),
        }
    }
    fn order_item(&self, first: &T, second: &T) -> Ordering {
        for order in &self.orders {
            let ord = order.compare.compare(&first, &second);
            let ord = match order.order {
                Order::Asc => ord,
                Order::Desc => match ord {
                    Ordering::Greater => Ordering::Less,
                    Ordering::Less => Ordering::Greater,
                    Ordering::Equal => Ordering::Equal,
                },
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
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
