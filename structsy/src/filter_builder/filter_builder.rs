use crate::{
    filter_builder::{
        embedded_filter_builder::EmbeddedFilterBuilder,
        execution_model::execute,
        execution_model::FieldsHolder,
        plan_model::plan_from_query,
        query_model::{FilterHolder, FilterMode, Orders as OrdersModel, Query, SolveQueryValue, SolveSimpleQueryValue},
        reader::{Reader, ReaderIterator},
        ValueCompare, ValueRange,
    },
    internal::{EmbeddedDescription, Field},
    Order, Persistent, PersistentEmbedded, Ref,
};
use std::{
    ops::{Bound, RangeBounds},
    rc::Rc,
};

pub trait RangeCondition<
    T: Persistent + 'static,
    V: PersistentEmbedded + Clone + PartialOrd + 'static + SolveQueryValue + ValueRange,
>
{
    fn range<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, V>, range: R) {
        filter
            .get_filter()
            .add_field_range(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
    }

    fn range_contains<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, range: R) {
        filter
            .get_filter()
            .add_field_range_contains(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
    }

    fn range_is<R: RangeBounds<V>>(filter: &mut FilterBuilder<T>, field: Field<T, Option<V>>, range: R) {
        filter
            .get_filter()
            .add_field_range_is(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        filter.get_fields().add_field_ord(field.clone());
    }
}
pub trait SimpleCondition<
    T: Persistent + 'static,
    V: PersistentEmbedded + ValueCompare + Clone + PartialEq + 'static + SolveQueryValue,
>
{
    fn equal(filter: &mut FilterBuilder<T>, field: Field<T, V>, value: V) {
        filter
            .get_filter()
            .add_field_equal(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
    }

    fn contains(filter: &mut FilterBuilder<T>, field: Field<T, Vec<V>>, value: V) {
        filter
            .get_filter()
            .add_field_contains(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
    }

    fn is(filter: &mut FilterBuilder<T>, field: Field<T, Option<V>>, value: V) {
        filter.get_filter().add_field_is(Rc::new(field.clone()), value.clone());
        filter.get_fields().add_field(field.clone());
    }
}

impl<T: Persistent + 'static> RangeCondition<T, bool> for bool {}
impl<T: Persistent + 'static> SimpleCondition<T, bool> for bool {}
impl<T: Persistent + 'static> SimpleCondition<T, Vec<bool>> for Vec<bool> {}
impl<T: Persistent + 'static> RangeCondition<T, Vec<bool>> for Vec<bool> {}

impl<T: Persistent + 'static, R: Persistent + 'static> SimpleCondition<T, Ref<R>> for Ref<R> {}
impl<T: Persistent + 'static, R: Persistent + 'static> RangeCondition<T, Ref<R>> for Ref<R> {}
impl<T: Persistent + 'static, R: Persistent + 'static> SimpleCondition<T, Vec<Ref<R>>> for Vec<Ref<R>> {}
impl<T: Persistent + 'static, R: Persistent + 'static> RangeCondition<T, Vec<Ref<R>>> for Vec<Ref<R>> {}
impl<T: Persistent + 'static, R: Persistent + 'static> SimpleCondition<T, Option<Ref<R>>> for Option<Ref<R>> {}
impl<T: Persistent + 'static, R: Persistent + 'static> RangeCondition<T, Option<Ref<R>>> for Option<Ref<R>> {}
macro_rules! index_conditions {
    ($($t:ty),+) => {
        $(
        impl<T: Persistent + 'static> SimpleCondition<T, $t>  for $t {}

        impl<T: Persistent + 'static> SimpleCondition<T, Vec<$t>> for Vec<$t> {}

        impl<T: Persistent + 'static> RangeCondition<T, Vec<$t>> for Vec<$t>{}

        impl<T: Persistent + 'static> SimpleCondition<T, Option<$t>> for Option<$t> {
            fn equal(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, value: Option<$t>) {
                filter.get_filter().add_field_equal(
                    Rc::new(field.clone()),
                    value.clone()
                );
                filter.get_fields().add_field(field.clone());
            }
        }

        impl<T: Persistent + 'static> RangeCondition<T, $t> for $t {}

        impl< T: Persistent + 'static> RangeCondition<T, Option<$t>> for Option<$t> {
            fn range<R: RangeBounds<Option<$t>>>(filter: &mut FilterBuilder<T>, field: Field<T, Option<$t>>, range: R) {
                filter.get_filter().add_field_range(
                    Rc::new(field.clone()),
                    (&range.start_bound(),&range.end_bound())
                );
                filter.get_fields().add_field_ord(field.clone());
            }
        }

        )+
    };
}

index_conditions!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, String);

impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static + SolveQueryValue>
    SimpleCondition<T, V> for V
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static + SolveQueryValue>
    RangeCondition<T, V> for V
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static + SolveSimpleQueryValue>
    SimpleCondition<T, Vec<V>> for Vec<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static + SolveSimpleQueryValue>
    RangeCondition<T, Vec<V>> for Vec<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialEq + Clone + 'static + SolveSimpleQueryValue>
    SimpleCondition<T, Option<V>> for Option<V>
{
}
impl<T: Persistent + 'static, V: EmbeddedDescription + PartialOrd + Clone + 'static + SolveSimpleQueryValue>
    RangeCondition<T, Option<V>> for Option<V>
{
}

pub struct FilterBuilder<T> {
    pub(crate) filter: FilterHolder,
    fields_holder: FieldsHolder<T>,
    orders: Vec<OrdersModel>,
}
impl<T: 'static> Default for FilterBuilder<T> {
    fn default() -> Self {
        FilterBuilder::new()
    }
}

impl<T: 'static> FilterBuilder<T> {
    pub(crate) fn move_out_filter(&mut self) -> FilterHolder {
        std::mem::replace(&mut self.filter, FilterHolder::new(FilterMode::And))
    }
    pub(crate) fn move_out_fields_holder(&mut self) -> FieldsHolder<T> {
        std::mem::replace(&mut self.fields_holder, FieldsHolder::default())
    }
    pub(crate) fn move_out_orders(&mut self) -> Vec<OrdersModel> {
        std::mem::replace(&mut self.orders, Vec::new())
    }
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder {
            filter: FilterHolder::new(FilterMode::And),
            fields_holder: Default::default(),
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

struct ToIter<'a, T> {
    read_iterator: Box<dyn ReaderIterator<Item = T> + 'a>,
}
impl<'a, T> Iterator for ToIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.read_iterator.next()
    }
}

impl<T: Persistent + 'static> FilterBuilder<T> {
    pub fn finish<'a>(self, reader_inst: Reader<'a>) -> Box<dyn Iterator<Item = (Ref<T>, T)> + 'a> {
        let query = Query::new(T::get_name(), self.filter, self.orders, Vec::new());
        let plan = plan_from_query(query, &reader_inst.structsy()).unwrap();
        let iter = execute(plan, Rc::new(self.fields_holder), reader_inst);
        Box::new(ToIter {
            read_iterator: iter.unwrap(),
        })
    }

    pub fn indexable_range_str<'a, R>(&mut self, field: Field<T, String>, range: R)
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
        <String as RangeCondition<T, String>>::range(self, field, (start, end))
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: EmbeddedFilterBuilder<V>)
    where
        V: PersistentEmbedded + 'static,
    {
        let (filter, orders_model, fields_holder) = filter.components();

        self.get_fields().add_nested_field(field.clone(), fields_holder);
        self.filter.add_field_embedded(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_embedded(Rc::new(field.clone()), orders_model));
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        let FilterBuilder {
            filter,
            fields_holder,
            orders,
        } = query;
        //TODO: merge field holder
        self.get_fields().add_field_ref(field.clone(), fields_holder.clone());
        self.filter.add_field_ref_query_equal(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_query_equal(Rc::new(field.clone()), orders));
    }

    pub fn ref_vec_query<V>(&mut self, field: Field<T, Vec<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        let FilterBuilder {
            filter,
            fields_holder,
            orders,
        } = query;
        self.get_fields()
            .add_field_vec_ref(field.clone(), fields_holder.clone());
        self.filter.add_field_ref_query_contains(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_query_contains(Rc::new(field.clone()), orders));
    }

    pub fn ref_option_query<V>(&mut self, field: Field<T, Option<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        let FilterBuilder {
            filter,
            fields_holder,
            orders,
        } = query;
        self.get_fields()
            .add_field_option_ref(field.clone(), fields_holder.clone());
        self.orders
            .push(OrdersModel::new_query_is(Rc::new(field.clone()), orders));
        self.filter.add_field_ref_query_is(Rc::new(field.clone()), filter);
    }

    pub fn or(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            mut filter,
            orders,
            fields_holder,
        } = builder;
        filter.mode = FilterMode::Or;
        self.fields_holder.merge(fields_holder);
        self.filter.add_group(filter);
        self.orders.extend(orders);
    }

    pub fn and(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            mut filter,
            fields_holder,
            orders,
        } = builder;
        filter.mode = FilterMode::And;
        self.fields_holder.merge(fields_holder);
        self.filter.add_group(filter);
        self.orders.extend(orders);
    }

    pub fn and_filter(&mut self, filters: FilterBuilder<T>) {
        let FilterBuilder {
            mut filter,
            fields_holder,
            orders,
        } = filters;
        filter.mode = FilterMode::And;
        self.fields_holder.merge(fields_holder);
        self.filter.add_group(filter);
        self.orders.extend(orders);
    }

    pub fn not(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            mut filter,
            fields_holder,
            orders,
        } = builder;
        filter.mode = FilterMode::Not;
        self.filter.add_group(filter);
        self.fields_holder.merge(fields_holder);
        self.orders.extend(orders);
    }

    pub fn order<V: ValueRange + Ord + 'static>(&mut self, field: Field<T, V>, order: Order) {
        self.orders
            .push(OrdersModel::new_field(Rc::new(field.clone()), order.clone()));
        self.get_fields().add_field_ord(field.clone());
    }
}
