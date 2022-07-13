use crate::filter_builder::{
    execution_model::FieldsHolder,
    filter_builder::FilterBuilder,
    query_model::{FilterHolder, FilterMode, Orders as OrdersModel, SolveQueryValue},
    ValueCompare, ValueRange,
};
use crate::internal::Field;
use crate::{Order, Persistent, Ref};
use std::{
    ops::{Bound, RangeBounds},
    rc::Rc,
};

pub trait SimpleEmbeddedCondition<T: 'static, V: Clone + PartialEq + SolveQueryValue + ValueCompare + 'static> {
    fn equal(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, V>, value: V) {
        filter
            .get_filter()
            .add_field_equal(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
    }

    fn contains(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter
            .get_filter()
            .add_field_contains(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
    }

    fn is(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.get_filter().add_field_is(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
    }
}

pub trait EmbeddedRangeCondition<T: 'static, V: Clone + SolveQueryValue + PartialOrd + 'static + ValueRange> {
    fn range<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, V>, range: R) {
        filter
            .get_filter()
            .add_field_range(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        filter
            .get_filter()
            .add_field_range_contains(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
    }

    fn range_is<R: RangeBounds<V>>(filter: &mut EmbeddedFilterBuilder<T>, field: Field<T, Option<V>>, range: R) {
        filter
            .get_filter()
            .add_field_range_is(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
    }
}
impl<T: 'static, V: Clone + PartialOrd + 'static + SolveQueryValue + ValueRange> EmbeddedRangeCondition<T, V> for V {}

impl<T: 'static, V: Clone + PartialEq + 'static + SolveQueryValue + ValueCompare> SimpleEmbeddedCondition<T, V> for V {}

pub struct EmbeddedFilterBuilder<T> {
    filter: FilterHolder,
    fields_holder: FieldsHolder<T>,
    orders: Vec<OrdersModel>,
}
impl<T> Default for EmbeddedFilterBuilder<T> {
    fn default() -> Self {
        EmbeddedFilterBuilder::<T>::new()
    }
}

impl<T> EmbeddedFilterBuilder<T> {
    pub fn new() -> EmbeddedFilterBuilder<T> {
        EmbeddedFilterBuilder {
            filter: FilterHolder::new(FilterMode::And),
            fields_holder: FieldsHolder::default(),
            orders: Vec::new(),
        }
    }
    fn get_filter(&mut self) -> &mut FilterHolder {
        &mut self.filter
    }
    fn get_fields(&mut self) -> &mut FieldsHolder<T> {
        &mut self.fields_holder
    }
}

impl<T: 'static> EmbeddedFilterBuilder<T> {
    pub(crate) fn components(self) -> (FilterHolder, Vec<OrdersModel>, FieldsHolder<T>) {
        (self.filter, self.orders, self.fields_holder)
    }

    pub fn simple_range_str<'a, R>(&mut self, field: Field<T, String>, range: R)
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
        <String as EmbeddedRangeCondition<T, String>>::range(self, field, (start, end))
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilterBuilder<V>)
    where
        V: 'static,
    {
        let (filter, orders_model, fields_holder) = filter.components();
        self.fields_holder.add_nested_field(field.clone(), fields_holder);
        self.filter.add_field_embedded(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_embedded(Rc::new(field.clone()), orders_model));
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, mut query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        self.get_fields()
            .add_field_ref(field.clone(), query.move_out_fields_holder());
        self.filter
            .add_field_ref_query_equal(Rc::new(field.clone()), query.move_out_filter());
        self.orders.push(OrdersModel::new_query_equal(
            Rc::new(field.clone()),
            query.move_out_orders(),
        ));
    }

    pub fn or(&mut self, filters: EmbeddedFilterBuilder<T>) {
        let EmbeddedFilterBuilder {
            mut filter,
            fields_holder,
            orders,
        } = filters;
        filter.mode = FilterMode::Or;
        self.fields_holder.merge(fields_holder);
        self.filter.add_group(filter);
        self.orders.extend(orders);
    }

    pub fn and(&mut self, filters: EmbeddedFilterBuilder<T>) {
        let EmbeddedFilterBuilder {
            mut filter,
            fields_holder,
            orders,
        } = filters;
        filter.mode = FilterMode::And;
        self.fields_holder.merge(fields_holder);
        self.filter.add_group(filter);
        self.orders.extend(orders);
    }

    pub fn not(&mut self, filters: EmbeddedFilterBuilder<T>) {
        let EmbeddedFilterBuilder {
            mut filter,
            fields_holder,
            orders,
        } = filters;
        filter.mode = FilterMode::Not;
        self.fields_holder.merge(fields_holder);
        self.filter.add_group(filter);
        self.orders.extend(orders);
    }
    pub fn order<V: Ord + 'static + ValueRange>(&mut self, field: Field<T, V>, order: Order) {
        self.get_fields().add_field_ord(field.clone());
        self.orders
            .push(OrdersModel::new_field(Rc::new(field.clone()), order.clone()));
    }
}
