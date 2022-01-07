use crate::{
    error::SRes,
    filter_builder::query_model::{
        FieldOrder, FilterFieldItem, FilterItem, FilterMode, FilterType, Orders, OrdersFilters, Projection, Query,
        QueryValue, SimpleQueryValue,
    },
    Order,
};
use std::ops::Bound;

use super::query_model::{FieldNestedOrders, FilterHolder};

struct TypeSource {
    name: String,
}

enum Source {
    Index(IndexInfo),
    Scan(TypeSource),
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

impl FilterPlan {
    fn find_possible_indexes(&self, type_name: &str, index_finder: &dyn IndexFinder) -> Vec<IndexInfo> {
        let mut vec = Vec::new();
        match self.mode {
            FilterPlanMode::And => {
                for filter in &self.filters {
                    if let FilterPlanItem::Field(f) = filter {
                        if let Some(info) = index_finder.find_index(&type_name, &f.field) {
                            vec.push(info);
                        }
                    }
                }
            }
            FilterPlanMode::Or => {}
            FilterPlanMode::Not => {}
        }
        vec
    }
}

#[derive(Debug, PartialEq)]
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
    pre_ordered: bool,
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
    LoadEqual(FieldNestedOrdersPlan),
    LoadIs(FieldNestedOrdersPlan),
    LoadContains(FieldNestedOrdersPlan),
}
pub(crate) struct FieldNestedOrdersPlan {
    field_path: Vec<String>,
    orders: OrdersPlan,
}

impl OrderPlanItem {
    fn field(mut path: Vec<String>, FieldOrder { field, mode }: FieldOrder) -> OrderPlanItem {
        path.push(field);
        OrderPlanItem::Field(FieldOrderPlan {
            field_path: path,
            mode,
            pre_ordered: false,
        })
    }
    fn load_equal(path: Vec<String>, orders: OrdersPlan) -> OrderPlanItem {
        OrderPlanItem::LoadEqual(FieldNestedOrdersPlan {
            field_path: path,
            orders,
        })
    }
    fn load_contains(path: Vec<String>, orders: OrdersPlan) -> OrderPlanItem {
        OrderPlanItem::LoadContains(FieldNestedOrdersPlan {
            field_path: path,
            orders,
        })
    }
    fn load_is(path: Vec<String>, orders: OrdersPlan) -> OrderPlanItem {
        OrderPlanItem::LoadIs(FieldNestedOrdersPlan {
            field_path: path,
            orders,
        })
    }
}

pub(crate) struct OrdersPlan {
    orders: Vec<OrderPlanItem>,
}
impl OrdersPlan {
    fn find_possible_indexes(&self, type_name: &str, index_finder: &dyn IndexFinder) -> Vec<IndexInfo> {
        let mut vec = Vec::new();
        for order in &self.orders {
            match order {
                OrderPlanItem::Field(field) => {
                    if let Some(info) = index_finder.find_index(&type_name, &field.field_path) {
                        vec.push(info);
                    }
                }
                _ => {}
            }
        }
        vec
    }
    fn consider_index(&mut self, index: &IndexInfo) {
        if self.orders.len() == 1 {
            self.orders.retain(|o|{
                match o {
                    OrderPlanItem::Field(f) => {
                        if f.field_path == index.field_path {
                            false
                        } else {
                            true
                        }
                    }
                    _ => {true}
                }
            });
        } else 
        if let Some(o) = self.orders.first_mut() {
            match o {
                OrderPlanItem::Field(f) => {
                    if f.field_path == index.field_path {
                        f.pre_ordered = true;
                    }
                }
                _ => {}
            }
        }
    }
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
fn recursive_rationalize_orders(path: Vec<String>, orders: Vec<Orders>, elements: &mut Vec<OrderPlanItem>) {
    for order in orders {
        match order {
            Orders::Field(f) => elements.push(OrderPlanItem::field(path.clone(), f)),
            Orders::Embeeded(FieldNestedOrders { field, orders: to_flat }) => {
                let mut new_path = path.clone();
                new_path.push(field);
                recursive_rationalize_orders(new_path, to_flat, elements);
            }
            Orders::QueryIs(FieldNestedOrders { field, orders: nested }) => {
                let mut new_path = path.clone();
                new_path.push(field);
                let o = rationalize_orders(nested);
                elements.push(OrderPlanItem::load_is(new_path, o));
            }
            Orders::QueryEqual(FieldNestedOrders { field, orders: nested }) => {
                let mut new_path = path.clone();
                new_path.push(field);
                let o = rationalize_orders(nested);
                elements.push(OrderPlanItem::load_equal(new_path, o));
            }
            Orders::QueryContains(FieldNestedOrders { field, orders: nested }) => {
                let mut new_path = path.clone();
                new_path.push(field);
                let o = rationalize_orders(nested);
                elements.push(OrderPlanItem::load_contains(new_path, o));
            }
        }
    }
}
fn rationalize_orders(orders: Vec<Orders>) -> OrdersPlan {
    let mut elements = Vec::new();
    recursive_rationalize_orders(vec![], orders, &mut elements);
    OrdersPlan { orders: elements }
}

struct IndexInfo {
    field_path: Vec<String>,
    index_name: String,
    index_range: Option<(Bound<QueryValue>, Bound<QueryValue>)>,
    //TODO: add mode, strait or reverse
}
impl IndexInfo {
    fn score(&self) -> usize {
        todo!()
    }
}

trait IndexFinder {
    fn find_index(&self, type_name: &str, field_path: &[String]) -> Option<IndexInfo>;
}

fn choose_index(mut filter_indexes: Vec<IndexInfo>, mut orders_indexes: Vec<IndexInfo>) -> Option<IndexInfo> {
    if let Some(index_info) = orders_indexes.pop() {
        for filter in filter_indexes {
            if index_info.field_path == filter.field_path {
                return Some(filter);
            }
        }
        Some(index_info)
    } else {
        filter_indexes.sort_by_key(|x| x.score());
        filter_indexes.pop()
    }
}

fn rationalize_projections(projections: Vec<Projection>) -> ProjectionsPlan {
    ProjectionsPlan {
        projections: projections
            .into_iter()
            .map(|prj| ProjectionPlan { field: prj.field })
            .collect(),
    }
}

fn plan_from_query(query: Query, index_finder: &dyn IndexFinder) -> SRes<QueryPlan> {
    let Query {
        type_name,
        projections,
        builder: OrdersFilters { filter, orders },
    } = query;

    let filter = rationalize_filters(filter);
    let mut orders = rationalize_orders(orders);
    let projections = rationalize_projections(projections);

    // The found index need to have inside the criteria for iterate trough them
    let filter_indexes = filter.find_possible_indexes(&type_name, index_finder);
    let orders_indexes = orders.find_possible_indexes(&type_name, index_finder);

    //TODO: select a way to choose an index
    let index = choose_index(filter_indexes, orders_indexes);
    if let Some(idx) = index {
        orders.consider_index(&idx);

        Ok(QueryPlan {
            source: Source::Index(idx),
            filter,
            orders,
            projections,
        })
    } else {
        Ok(QueryPlan {
            source: Source::Scan(TypeSource { name: type_name }),
            filter,
            orders,
            projections,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::{rationalize_filters, rationalize_orders};
    use crate::{
        filter_builder::{
            plan_model::{FilterPlanItem, FilterPlanMode, OrderPlanItem},
            query_model::{FilterHolder, FilterMode, Orders},
        },
        Order,
    };

    #[test]
    fn test_filter_rationalize_collapse() {
        let mut fh = FilterHolder::new(FilterMode::And);
        fh.add_field_equal("test", 10);
        let mut fhe = FilterHolder::new(FilterMode::And);
        fhe.add_field_equal("test1", 20);
        fhe.add_field_equal("test2", 30);
        fh.add_group(fhe);
        let mut fhe = FilterHolder::new(FilterMode::Or);
        fhe.add_field_equal("test3", 20);
        fhe.add_field_equal("test4", 30);
        fh.add_group(fhe);

        let fp = rationalize_filters(fh);
        assert_eq!(fp.mode, FilterPlanMode::And);
        assert_eq!(fp.filters.len(), 4);
        match &fp.filters[0] {
            FilterPlanItem::Field(f) => assert_eq!(f.field, vec!["test"]),
            _ => panic!("expected field"),
        }
        match &fp.filters[1] {
            FilterPlanItem::Field(f) => assert_eq!(f.field, vec!["test1"]),
            _ => panic!("expected field"),
        }
        match &fp.filters[2] {
            FilterPlanItem::Field(f) => assert_eq!(f.field, vec!["test2"]),
            _ => panic!("expected field"),
        }
        match &fp.filters[3] {
            FilterPlanItem::Group(g) => {
                assert_eq!(g.mode, FilterPlanMode::Or);
                match &g.filters[0] {
                    FilterPlanItem::Field(f) => assert_eq!(f.field, vec!["test3"]),
                    _ => panic!("expected field"),
                }
                match &g.filters[1] {
                    FilterPlanItem::Field(f) => assert_eq!(f.field, vec!["test4"]),
                    _ => panic!("expected field"),
                }
            }
            _ => panic!("expected group"),
        }
    }

    #[test]
    fn rationalize_orders_test() {
        let mut orders = Vec::new();
        orders.push(Orders::new_field("field", Order::Asc));
        let mut nested_orders = Vec::new();
        nested_orders.push(Orders::new_field("field1", Order::Asc));
        orders.push(Orders::new_embedded("field2", nested_orders));
        let mut nested_orders = Vec::new();
        nested_orders.push(Orders::new_field("field3", Order::Asc));
        orders.push(Orders::new_query_equal("field4", nested_orders));

        let translated_orders = rationalize_orders(orders);
        match &translated_orders.orders[0] {
            OrderPlanItem::Field(field) => assert_eq!(field.field_path, vec!["field"]),
            _ => panic!("expected field"),
        }
        match &translated_orders.orders[1] {
            OrderPlanItem::Field(field) => assert_eq!(field.field_path, vec!["field2", "field1"]),
            _ => panic!("expected field"),
        }
        match &translated_orders.orders[2] {
            OrderPlanItem::LoadEqual(load) => {
                assert_eq!(load.field_path, vec!["field4"]);
                match &load.orders.orders[0] {
                    OrderPlanItem::Field(field) => assert_eq!(field.field_path, vec!["field3"]),
                    _ => panic!("expected field"),
                }
            }
            _ => panic!("expected load equal"),
        }
    }
}
