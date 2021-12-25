use crate::{
    error::SRes,
    filter_builder::{
        query_model::{BuilderQuery, FilterFieldItem, FilterItem, FilterMode, FilterType, Orders, Query, QueryValue},
        FilterBuilder,
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
    field: String,
    filter_by: FilterByPlan,
}
pub(crate) enum FilterPlanItem {
    Field(FilterFieldPlanItem),
    Group(FilterPlan),
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

pub(crate) enum FilterByPlan {
    Equal(QueryValue),
    Contains(QueryValue),
    Is(QueryValue),
    Range((Bound<QueryValue>, Bound<QueryValue>)),
    RangeContains((Bound<QueryValue>, Bound<QueryValue>)),
    RangeIs((Bound<QueryValue>, Bound<QueryValue>)),
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

fn rationalize_filters_deep(filters: Vec<FilterItem>, parent_mode: &FilterMode, elements: &mut Vec<FilterPlanItem>) {
    for filter in filters {
        match filter {
            FilterItem::Field(field) => {
                let FilterFieldItem { field, filter_type } = field;
                let type_plan = match filter_type {
                    FilterType::Equal(val) => FilterByPlan::Equal(val),
                    FilterType::Contains(val) => FilterByPlan::Contains(val),
                    FilterType::Is(val) => FilterByPlan::Is(val),
                    FilterType::Range(bound) => FilterByPlan::Range(bound),
                    FilterType::RangeContains(bound) => FilterByPlan::RangeContains(bound),
                    FilterType::RangeIs(bound) => FilterByPlan::RangeIs(bound),
                };
                let item = FilterPlanItem::Field(FilterFieldPlanItem {
                    field,
                    filter_by: type_plan,
                });
                elements.push(item);
            }
            FilterItem::Group(group) => {
                let FilterHolder { filters, mode } = group;
                if mode == *parent_mode {
                    rationalize_filters_deep(filters, &mode, elements)
                } else {
                    let mut child_elements = Vec::<FilterPlanItem>::with_capacity(filters.len());
                    rationalize_filters_deep(filters, &mode, &mut child_elements);
                    let item = FilterPlanItem::Group(FilterPlan {
                        filters: child_elements,
                        mode: match mode {
                            FilterMode::Or => FilterPlanMode::Or,
                            FilterMode::And => FilterPlanMode::And,
                            FilterMode::Not => FilterPlanMode::Not,
                        },
                    });
                    elements.push(item);
                }
            }
        };
    }
}

fn rationalize_filters(filter: FilterHolder) -> (FilterPlan, Vec<Orders>) {
    let FilterHolder { filters, mode } = filter;
    let mut elements = Vec::<FilterPlanItem>::with_capacity(filters.len());
    rationalize_filters_deep(filters, &mode, &mut elements);
    (
        FilterPlan {
            filters: elements,
            mode: match mode {
                FilterMode::Or => FilterPlanMode::Or,
                FilterMode::And => FilterPlanMode::And,
                FilterMode::Not => FilterPlanMode::Not,
            },
        },
        Vec::new(),
    )
}
fn rationalize_orders(orders: Vec<Orders>) -> OrdersPlan {
    todo!()
}

fn plan_from_query(query: Query) -> SRes<QueryPlan> {
    let Query {
        projections,
        builder: BuilderQuery { filter, orders },
    } = query;

    let (filter, nested_orders) = rationalize_filters(filter);
    let orders = rationalize_orders(orders);

    todo!("not yet here")
}
