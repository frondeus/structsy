use crate::{
    desc::ValueType,
    error::SRes,
    filter_builder::query_model::{
        FieldOrder, FilterFieldItem, FilterItem, FilterMode, FilterType, Orders, OrdersFilters, Projection, Query,
        QueryValue, SimpleQueryValue,
    },
    internal::FieldInfo,
    Order,
};
use std::{ops::Bound, rc::Rc};

use super::query_model::{FieldNestedOrders, FilterHolder};

#[derive(Clone)]
pub(crate) struct FieldPath {
    pub(crate) path: Vec<Rc<dyn FieldInfo>>,
}
impl FieldPath {
    fn new() -> Self {
        Self { path: Vec::new() }
    }
    pub fn field_path_names(&self) -> Vec<String> {
        self.path.iter().map(|f| f.name().to_owned()).collect()
    }
    pub fn field_path_names_str(&self) -> Vec<&'static str> {
        self.path.iter().map(|f| f.name()).collect()
    }
    fn push(&mut self, f: Rc<dyn FieldInfo>) {
        self.path.push(f)
    }
}

pub(crate) struct TypeSource {
    name: String,
}

pub(crate) enum Source {
    Index(IndexInfo),
    Scan(TypeSource),
}

pub(crate) struct FilterFieldPlanItem {
    field: FieldPath,
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
    fn field(path: FieldPath, filter_by: FilterByPlan) -> Self {
        FilterPlanItem::Field(FilterFieldPlanItem { field: path, filter_by })
    }
}

pub(crate) struct FilterPlan {
    filters: Vec<FilterPlanItem>,
    mode: FilterPlanMode,
}

impl FilterPlan {
    fn find_possible_indexes(&self, type_name: &str, info_finder: &dyn InfoFinder) -> Vec<IndexInfo> {
        let mut vec = Vec::new();
        match self.mode {
            FilterPlanMode::And => {
                for filter in &self.filters {
                    if let FilterPlanItem::Field(f) = filter {
                        if let Some(range) = f.filter_by.solve_range() {
                            if let Some(info) = info_finder.find_index(&type_name, &f.field, Some(range), Order::Asc) {
                                vec.push(info);
                            }
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

#[derive(Clone)]
pub(crate) enum QueryValuePlan {
    Single(SimpleQueryValue),
    Option(Option<SimpleQueryValue>),
    Array(Vec<SimpleQueryValue>),
    OptionArray(Option<Vec<SimpleQueryValue>>),
}

impl QueryValuePlan {
    fn translate(qv: QueryValue) -> Self {
        match qv {
            QueryValue::Single(s) => QueryValuePlan::Single(s),
            QueryValue::Option(s) => QueryValuePlan::Option(s),
            QueryValue::Vec(v) => QueryValuePlan::Array(v),
            QueryValue::OptionVec(s) => QueryValuePlan::OptionArray(s),
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
    fn extract_bound(bound: Bound<Self>) -> Option<Bound<SimpleQueryValue>> {
        match bound {
            Bound::Included(Self::Single(v)) => Some(Bound::Included(v)),
            Bound::Included(Self::Option(Some(v))) => Some(Bound::Included(v)),
            Bound::Included(Self::Option(None)) => Some(Bound::Unbounded),
            Bound::Included(Self::Array(v)) => None,
            Bound::Included(Self::OptionArray(v)) => None,
            Bound::Excluded(Self::Single(v)) => Some(Bound::Excluded(v)),
            Bound::Excluded(Self::Option(Some(v))) => Some(Bound::Excluded(v)),
            Bound::Excluded(Self::Option(None)) => Some(Bound::Unbounded),
            Bound::Excluded(Self::Array(v)) => None,
            Bound::Excluded(Self::OptionArray(v)) => None,
            Bound::Unbounded => Some(Bound::Unbounded),
        }
    }
    pub(crate) fn extract_bounds(
        (first, second): (Bound<Self>, Bound<Self>),
    ) -> Option<(Bound<SimpleQueryValue>, Bound<SimpleQueryValue>)> {
        let f = Self::extract_bound(first);
        let v = Self::extract_bound(second);
        if let (Some(ff), Some(ss)) = (f, v) {
            Some((ff, ss))
        } else {
            None
        }
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
impl FilterByPlan {
    fn solve_range(&self) -> Option<(Bound<QueryValuePlan>, Bound<QueryValuePlan>)> {
        match self {
            Self::Equal(e) => Some((Bound::Included(e.clone()), Bound::Excluded(e.clone()))),
            Self::Contains(e) => Some((Bound::Included(e.clone()), Bound::Excluded(e.clone()))),
            Self::Is(e) => Some((Bound::Included(e.clone()), Bound::Excluded(e.clone()))),
            Self::Range(e) => Some(e.clone()),
            Self::RangeContains(e) => Some(e.clone()),
            Self::RangeIs(e) => Some(e.clone()),
            Self::LoadAndEqual(_) => None,
            Self::LoadAndContains(_) => None,
            Self::LoadAndIs(_) => None,
        }
    }
}

pub(crate) struct FieldOrderPlan {
    field_path: FieldPath,
    mode: Order,
    pre_ordered: bool,
}
impl FieldOrderPlan {
    fn field_path_names(&self) -> Vec<String> {
        self.field_path.field_path_names()
    }
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
    field_path: FieldPath,
    orders: OrdersPlan,
}
impl FieldNestedOrdersPlan {
    fn field_path_names(&self) -> Vec<String> {
        self.field_path.field_path_names()
    }
}

impl OrderPlanItem {
    fn field(mut path: FieldPath, FieldOrder { field, mode }: FieldOrder) -> OrderPlanItem {
        path.push(field);
        OrderPlanItem::Field(FieldOrderPlan {
            field_path: path,
            mode,
            pre_ordered: false,
        })
    }
    fn load_equal(path: FieldPath, orders: OrdersPlan) -> OrderPlanItem {
        OrderPlanItem::LoadEqual(FieldNestedOrdersPlan {
            field_path: path,
            orders,
        })
    }
    fn load_contains(path: FieldPath, orders: OrdersPlan) -> OrderPlanItem {
        OrderPlanItem::LoadContains(FieldNestedOrdersPlan {
            field_path: path,
            orders,
        })
    }
    fn load_is(path: FieldPath, orders: OrdersPlan) -> OrderPlanItem {
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
    fn find_possible_indexes(&self, type_name: &str, info_finder: &dyn InfoFinder) -> Vec<IndexInfo> {
        let mut vec = Vec::new();
        for order in &self.orders {
            match order {
                OrderPlanItem::Field(field) => {
                    if let Some(info) = info_finder.find_index(&type_name, &field.field_path, None, field.mode.clone())
                    {
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
            self.orders.retain(|o| match o {
                OrderPlanItem::Field(f) => {
                    if f.field_path_names() == index.field_path_names() {
                        false
                    } else {
                        true
                    }
                }
                _ => true,
            });
        } else if let Some(o) = self.orders.first_mut() {
            match o {
                OrderPlanItem::Field(f) => {
                    if f.field_path_names() == index.field_path_names() {
                        f.pre_ordered = true;
                    }
                }
                _ => {}
            }
        }
    }
}

pub(crate) struct QueryPlan {
    pub(crate) source: Source,
    pub(crate) filter: Option<FilterPlan>,
    pub(crate) orders: Option<OrdersPlan>,
    pub(crate) projections: Option<ProjectionsPlan>,
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
    field_path: FieldPath,
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
    field_path: FieldPath,
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
                    FilterType::QueryEqual(filter) => {
                        rationalize_filters(filter).map(|v| FilterByPlan::LoadAndEqual(v))
                    }
                    FilterType::QueryContains(filter) => {
                        rationalize_filters(filter).map(|v| FilterByPlan::LoadAndContains(v))
                    }
                    FilterType::QueryIs(filter) => rationalize_filters(filter).map(|v| FilterByPlan::LoadAndIs(v)),
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

fn rationalize_filters(filter: FilterHolder) -> Option<FilterPlan> {
    let FilterHolder { filters, mode } = filter;
    let mut elements = Vec::<FilterPlanItem>::with_capacity(filters.len());
    rationalize_filters_deep(FieldPath::new(), filters, &mode, &mut elements);
    if elements.is_empty() {
        None
    } else {
        Some(FilterPlan {
            filters: elements,
            mode: mode.into(),
        })
    }
}
fn recursive_rationalize_orders(path: FieldPath, orders: Vec<Orders>, elements: &mut Vec<OrderPlanItem>) {
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
                if let Some(o) = rationalize_orders(nested) {
                    elements.push(OrderPlanItem::load_is(new_path, o));
                }
            }
            Orders::QueryEqual(FieldNestedOrders { field, orders: nested }) => {
                let mut new_path = path.clone();
                new_path.push(field);
                if let Some(o) = rationalize_orders(nested) {
                    elements.push(OrderPlanItem::load_equal(new_path, o));
                }
            }
            Orders::QueryContains(FieldNestedOrders { field, orders: nested }) => {
                let mut new_path = path.clone();
                new_path.push(field);
                if let Some(o) = rationalize_orders(nested) {
                    elements.push(OrderPlanItem::load_contains(new_path, o));
                }
            }
        }
    }
}
fn rationalize_orders(orders: Vec<Orders>) -> Option<OrdersPlan> {
    let mut elements = Vec::new();
    recursive_rationalize_orders(FieldPath::new(), orders, &mut elements);
    if elements.is_empty() {
        None
    } else {
        Some(OrdersPlan { orders: elements })
    }
}

pub(crate) struct IndexInfo {
    pub(crate) field_path: FieldPath,
    pub(crate) index_name: String,
    pub(crate) index_range: Option<(Bound<QueryValuePlan>, Bound<QueryValuePlan>)>,
    pub(crate) ordering_mode: Order,
    pub(crate) value_type: ValueType,
}
impl IndexInfo {
    pub(crate) fn new(
        field_path: FieldPath,
        index_name: String,
        index_range: Option<(Bound<QueryValuePlan>, Bound<QueryValuePlan>)>,
        ordering_mode: Order,
        value_type: ValueType,
    ) -> IndexInfo {
        IndexInfo {
            field_path,
            index_name,
            index_range,
            ordering_mode,
            value_type,
        }
    }
    fn field_path_names(&self) -> Vec<String> {
        self.field_path.field_path_names()
    }
}

pub(crate) trait InfoFinder {
    fn find_index(
        &self,
        type_name: &str,
        field_path: &FieldPath,
        range: Option<(Bound<QueryValuePlan>, Bound<QueryValuePlan>)>,
        mode: Order,
    ) -> Option<IndexInfo>;
    fn score_index(&self, index: &IndexInfo) -> SRes<usize>;
}

fn choose_index(
    mut filter_indexes: Option<Vec<IndexInfo>>,
    mut orders_indexes: Option<Vec<IndexInfo>>,
    finder: &dyn InfoFinder,
) -> Option<IndexInfo> {
    if let Some(index_info) = orders_indexes.as_mut().map(|v| v.pop()).flatten() {
        if let Some(fi) = filter_indexes {
            for filter in fi {
                if index_info.field_path_names() == filter.field_path_names() {
                    return Some(filter);
                }
            }
        }
        Some(index_info)
    } else if let Some(fi) = &mut filter_indexes {
        fi.sort_by_key(|x| finder.score_index(x).unwrap_or(usize::MAX));
        fi.pop()
    } else {
        None
    }
}

fn rationalize_projections(projections: Vec<Projection>) -> Option<ProjectionsPlan> {
    if projections.is_empty() {
        None
    } else {
        Some(ProjectionsPlan {
            projections: projections
                .into_iter()
                .map(|prj| ProjectionPlan { field: prj.field })
                .collect(),
        })
    }
}

fn plan_from_query(query: Query, info_finder: &dyn InfoFinder) -> SRes<QueryPlan> {
    let Query {
        type_name,
        projections,
        builder: OrdersFilters { filter, orders },
    } = query;

    let filter = rationalize_filters(filter);
    let mut orders = rationalize_orders(orders);
    let projections = rationalize_projections(projections);

    // The found index need to have inside the criteria for iterate trough them
    let filter_indexes = if let Some(f) = &filter {
        Some(f.find_possible_indexes(&type_name, info_finder))
    } else {
        None
    };

    let orders_indexes = if let Some(or) = &orders {
        Some(or.find_possible_indexes(&type_name, info_finder))
    } else {
        None
    };

    //TODO: select a way to choose an index
    let index = choose_index(filter_indexes, orders_indexes, info_finder);
    if let Some(idx) = index {
        if let Some(orders) = &mut orders {
            orders.consider_index(&idx);
        }
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
        internal::Field,
        Order,
    };
    use std::rc::Rc;
    struct Test {}
    fn tf(name: &'static str) -> Rc<Field<Test, u8>> {
        Rc::new(Field::<Test, u8>::new(name, |_| unreachable!()))
    }
    #[test]
    fn test_filter_rationalize_collapse() {
        let mut fh = FilterHolder::new(FilterMode::And);
        fh.add_field_equal(tf("test"), 10);
        let mut fhe = FilterHolder::new(FilterMode::And);
        fhe.add_field_equal(tf("test1"), 20);
        fhe.add_field_equal(tf("test2"), 30);
        fh.add_group(fhe);
        let mut fhe = FilterHolder::new(FilterMode::Or);
        fhe.add_field_equal(tf("test3"), 20);
        fhe.add_field_equal(tf("test4"), 30);
        fh.add_group(fhe);

        let fp = rationalize_filters(fh).unwrap();
        assert_eq!(fp.mode, FilterPlanMode::And);
        assert_eq!(fp.filters.len(), 4);
        match &fp.filters[0] {
            FilterPlanItem::Field(f) => assert_eq!(f.field.path.first().unwrap().name(), "test"),
            _ => panic!("expected field"),
        }
        match &fp.filters[1] {
            FilterPlanItem::Field(f) => assert_eq!(f.field.path.first().unwrap().name(), "test1"),
            _ => panic!("expected field"),
        }
        match &fp.filters[2] {
            FilterPlanItem::Field(f) => assert_eq!(f.field.path.first().unwrap().name(), "test2"),
            _ => panic!("expected field"),
        }
        match &fp.filters[3] {
            FilterPlanItem::Group(g) => {
                assert_eq!(g.mode, FilterPlanMode::Or);
                match &g.filters[0] {
                    FilterPlanItem::Field(f) => assert_eq!(f.field.path.first().unwrap().name(), "test3"),
                    _ => panic!("expected field"),
                }
                match &g.filters[1] {
                    FilterPlanItem::Field(f) => assert_eq!(f.field.path.first().unwrap().name(), "test4"),
                    _ => panic!("expected field"),
                }
            }
            _ => panic!("expected group"),
        }
    }

    #[test]
    fn rationalize_orders_test() {
        let mut orders = Vec::new();
        orders.push(Orders::new_field(tf("field"), Order::Asc));
        let mut nested_orders = Vec::new();
        nested_orders.push(Orders::new_field(tf("field1"), Order::Asc));
        orders.push(Orders::new_embedded(tf("field2"), nested_orders));
        let mut nested_orders = Vec::new();
        nested_orders.push(Orders::new_field(tf("field3"), Order::Asc));
        orders.push(Orders::new_query_equal(tf("field4"), nested_orders));

        let translated_orders = rationalize_orders(orders).unwrap();
        match &translated_orders.orders[0] {
            OrderPlanItem::Field(field) => assert_eq!(field.field_path_names(), vec!["field"]),
            _ => panic!("expected field"),
        }
        match &translated_orders.orders[1] {
            OrderPlanItem::Field(field) => assert_eq!(field.field_path_names(), vec!["field2", "field1"]),
            _ => panic!("expected field"),
        }
        match &translated_orders.orders[2] {
            OrderPlanItem::LoadEqual(load) => {
                assert_eq!(load.field_path_names(), vec!["field4"]);
                match &load.orders.orders[0] {
                    OrderPlanItem::Field(field) => assert_eq!(field.field_path_names(), vec!["field3"]),
                    _ => panic!("expected field"),
                }
            }
            _ => panic!("expected load equal"),
        }
    }
}
