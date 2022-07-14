use crate::{
    filter_builder::{
        execution_model::execute,
        execution_model::FieldsHolder,
        plan_model::plan_from_query,
        query_model::{FilterHolder, FilterMode, Orders as OrdersModel, Query, SolveQueryValue},
        reader::{Reader, ReaderIterator},
        ValueCompare, ValueRange,
    },
    internal::Field,
    Order, Persistent, PersistentEmbedded, Ref,
};
use std::{
    ops::{Bound, RangeBounds},
    rc::Rc,
};

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
}

impl<T: 'static> FilterBuilder<T> {
    pub fn cond_range_str<'a, R>(&mut self, field: Field<T, String>, range: R)
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
        self.cond_range(field, (start, end));
    }

    pub fn cond_equal<V>(&mut self, field: Field<T, V>, value: V)
    where
        V: ValueCompare + SolveQueryValue + 'static,
    {
        self.get_filter().add_field_equal(Rc::new(field.clone()), value);
        self.get_fields().add_field(field.clone());
    }
    pub fn cond_is<V>(&mut self, field: Field<T, Option<V>>, value: V)
    where
        V: ValueCompare + SolveQueryValue + 'static,
    {
        self.get_filter().add_field_is(Rc::new(field.clone()), value);
        self.get_fields().add_field(field.clone());
    }
    pub fn cond_contains<V>(&mut self, field: Field<T, Vec<V>>, value: V)
    where
        V: ValueCompare + SolveQueryValue + 'static,
    {
        self.get_filter().add_field_contains(Rc::new(field.clone()), value);
        self.get_fields().add_field(field.clone());
    }

    pub fn cond_range<V, R: RangeBounds<V>>(&mut self, field: Field<T, V>, range: R)
    where
        V: ValueRange + SolveQueryValue + Clone + 'static,
    {
        self.get_filter()
            .add_field_range(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        self.get_fields().add_field_ord(field.clone());
    }

    pub fn cond_range_contains<V, R: RangeBounds<V>>(&mut self, field: Field<T, Vec<V>>, range: R)
    where
        V: ValueRange + SolveQueryValue + Clone + 'static,
    {
        self.get_filter()
            .add_field_range_contains(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        self.get_fields().add_field_ord(field.clone());
    }

    pub fn cond_range_is<V, R: RangeBounds<V>>(&mut self, field: Field<T, Option<V>>, range: R)
    where
        V: ValueRange + SolveQueryValue + Clone + 'static,
    {
        self.get_filter()
            .add_field_range_is(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        self.get_fields().add_field_ord(field.clone());
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: FilterBuilder<V>)
    where
        V: PersistentEmbedded + 'static,
    {
        let FilterBuilder {
            filter,
            orders,
            fields_holder,
        } = filter;

        self.get_fields().add_nested_field(field.clone(), fields_holder);
        self.filter.add_field_embedded(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_embedded(Rc::new(field.clone()), orders));
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
