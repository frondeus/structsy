use crate::{
    error::SRes,
    filter_builder::query_model::{
        FilterFieldItem, FilterItem, FilterMode, FilterType, Orders, OrdersFilters, Query, QueryValue, SimpleQueryValue,
    },
    Order,
};
use std::ops::Bound;

use super::query_model::FilterHolder;

struct IndexSource {
    name: String,
    bounds: (Bound<QueryValue>, Bound<QueryValue>),
}
struct SegmentSource {
    name: String,
}

enum Source {
    Index(IndexSource),
    Segment(SegmentSource),
}

pub(crate) struct FilterFieldPlanItem {
    field: Vec<String>,
    filter_by: FilterByPlan,
}
pub(crate) enum FilterPlanItem {
    Field(FilterFieldPlanItem),
    Group(FilterPlan),
}

impl FilterPlanItem {
    fn group(filters: Vec<FilterPlanItem>, mode: FilterPlanMode) -> Self {
        FilterPlanItem::Group(FilterPlan { filters, mode })
    }
    fn field(path: Vec<String>, filter_by: FilterByPlan) -> Self {
        FilterPlanItem::Field(FilterFieldPlanItem { field: path, filter_by })
    }
}

pub(crate) struct FilterPlan {
    filters: Vec<FilterPlanItem>,
    mode: FilterPlanMode,
}
pub(crate) enum FilterPlanMode {
    And,
    Or,
    Not,
}
impl From<FilterMode> for FilterPlanMode {
    fn from(mode: FilterMode) -> Self {
        match mode {
            FilterMode::Or => FilterPlanMode::Or,
            FilterMode::And => FilterPlanMode::And,
            FilterMode::Not => FilterPlanMode::Not,
        }
    }
}

pub(crate) enum QueryValuePlan {
    Single(SimpleQueryValue),
    Option(Option<SimpleQueryValue>),
    Vec(Vec<SimpleQueryValue>),
    OptionVec(Option<Vec<SimpleQueryValue>>),
}

impl QueryValuePlan {
    fn translate(qv: QueryValue) -> Self {
        match qv {
            QueryValue::Single(s) => QueryValuePlan::Single(s),
            QueryValue::Option(s) => QueryValuePlan::Option(s),
            QueryValue::Vec(v) => QueryValuePlan::Vec(v),
            QueryValue::OptionVec(s) => QueryValuePlan::OptionVec(s),
        }
    }
    fn translate_bounds((first, second): (Bound<QueryValue>, Bound<QueryValue>)) -> (Bound<Self>, Bound<Self>) {
        (
            match first {
                Bound::Included(v) => Bound::Included(Self::translate(v)),
                Bound::Excluded(v) => Bound::Excluded(Self::translate(v)),
                Bound::Unbounded => Bound::Unbounded,
            },
            match second {
                Bound::Included(v) => Bound::Included(Self::translate(v)),
                Bound::Excluded(v) => Bound::Excluded(Self::translate(v)),
                Bound::Unbounded => Bound::Unbounded,
            },
        )
    }
}

pub(crate) enum FilterByPlan {
    Equal(QueryValuePlan),
    Contains(QueryValuePlan),
    Is(QueryValuePlan),
    Range((Bound<QueryValuePlan>, Bound<QueryValuePlan>)),
    RangeContains((Bound<QueryValuePlan>, Bound<QueryValuePlan>)),
    RangeIs((Bound<QueryValuePlan>, Bound<QueryValuePlan>)),
    LoadAndEqual(FilterPlan),
    LoadAndContains(FilterPlan),
    LoadAndIs(FilterPlan),
}

pub(crate) struct FieldOrderPlan {
    field_path: Vec<String>,
    mode: Order,
}
pub(crate) struct BufferedOrder {
    orders: Vec<FieldOrderPlan>,
}
pub(crate) struct IndexOrderPlan {
    name: String,
    range: Option<(Bound<QueryValue>, Bound<QueryValue>)>,
    mode: Order,
}
pub(crate) enum OrderPlanItem {
    Field(FieldOrderPlan),
    Index(IndexOrderPlan),
}

pub(crate) struct OrdersPlan {
    orders: Vec<OrderPlanItem>,
}

pub(crate) struct QueryPlan {
    source: Source,
    filter: FilterPlan,
    orders: OrdersPlan,
    projections: ProjectionsPlan,
}

pub(crate) struct ProjectionsPlan {
    projections: Vec<ProjectionPlan>,
}
pub(crate) struct ProjectionPlan {
    field: String,
}

fn flat_or_deep_filter(
    x: FilterHolder,
    parent_mode: &FilterMode,
    field_path: Vec<String>,
    elements: &mut Vec<FilterPlanItem>,
) {
    let FilterHolder { filters, mode } = x;
    if mode == *parent_mode {
        rationalize_filters_deep(field_path, filters, &mode, elements)
    } else {
        let mut child_elements = Vec::<FilterPlanItem>::with_capacity(filters.len());
        rationalize_filters_deep(field_path, filters, &mode, &mut child_elements);
        let item = FilterPlanItem::group(child_elements, mode.into());
        elements.push(item);
    }
}

fn rationalize_filters_deep(
    field_path: Vec<String>,
    filters: Vec<FilterItem>,
    parent_mode: &FilterMode,
    elements: &mut Vec<FilterPlanItem>,
) {
    for filter in filters {
        match filter {
            FilterItem::Field(field) => {
                let FilterFieldItem { field, filter_type } = field;
                let mut f_path = field_path.clone();
                f_path.push(field);
                let type_plan = match filter_type {
                    FilterType::Equal(val) => Some(FilterByPlan::Equal(QueryValuePlan::translate(val))),
                    FilterType::Contains(val) => Some(FilterByPlan::Contains(QueryValuePlan::translate(val))),
                    FilterType::Is(val) => Some(FilterByPlan::Is(QueryValuePlan::translate(val))),
                    FilterType::Range(bound) => Some(FilterByPlan::Range(QueryValuePlan::translate_bounds(bound))),
                    FilterType::RangeContains(bound) => {
                        Some(FilterByPlan::RangeContains(QueryValuePlan::translate_bounds(bound)))
                    }
                    FilterType::RangeIs(bound) => Some(FilterByPlan::RangeIs(QueryValuePlan::translate_bounds(bound))),
                    FilterType::Embedded(x) => {
                        flat_or_deep_filter(x, parent_mode, f_path.clone(), elements);
                        None
                    }
                    FilterType::QueryEqual(filter) => Some(FilterByPlan::LoadAndEqual(rationalize_filters(filter))),
                    FilterType::QueryContains(filter) => {
                        Some(FilterByPlan::LoadAndContains(rationalize_filters(filter)))
                    }
                    FilterType::QueryIs(filter) => Some(FilterByPlan::LoadAndIs(rationalize_filters(filter))),
                };
                if let Some(type_plan) = type_plan {
                    elements.push(FilterPlanItem::field(f_path, type_plan));
                }
            }
            FilterItem::Group(group) => {
                flat_or_deep_filter(group, parent_mode, field_path.clone(), elements);
            }
        };
    }
}

fn rationalize_filters(filter: FilterHolder) -> FilterPlan {
    let FilterHolder { filters, mode } = filter;
    let mut elements = Vec::<FilterPlanItem>::with_capacity(filters.len());
    rationalize_filters_deep(vec![], filters, &mode, &mut elements);
    FilterPlan {
        filters: elements,
        mode: mode.into(),
    }
}
fn rationalize_orders(orders: Vec<Orders>) -> OrdersPlan {
    todo!()
}

fn plan_from_query(query: Query) -> SRes<QueryPlan> {
    let Query {
        projections,
        builder: OrdersFilters { filter, orders },
    } = query;

    let filter = rationalize_filters(filter);
    let orders = rationalize_orders(orders);

    todo!("not yet here")
}

mod tests {}
