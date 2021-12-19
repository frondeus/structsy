use crate::{filter_builder::query_model::QueryValue, Order};
use std::ops::Bound;

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

pub(crate) struct FilterPlanItem {
    field: String,
    filter_by: FilterByPlan,
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
