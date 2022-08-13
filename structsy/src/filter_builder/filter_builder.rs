use crate::{
    filter_builder::{
        execution_model::execute,
        fields_holder::FieldsHolder,
        plan_model::plan_from_query,
        query_model::{FilterHolder, FilterMode, Orders as OrdersModel, Query, SolveQueryRange, SolveQueryValue},
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
    filters: FilterHolder,
    fields: FieldsHolder<T>,
    orders: Vec<OrdersModel>,
}
impl<T> Default for FilterBuilder<T> {
    fn default() -> Self {
        FilterBuilder::new()
    }
}

impl<T> FilterBuilder<T> {
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder {
            filters: FilterHolder::new(FilterMode::And),
            fields: Default::default(),
            orders: Vec::new(),
        }
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
        let query = Query::new(T::get_name(), self.filters, self.orders, Vec::new());
        let plan = plan_from_query(query, &reader_inst.structsy()).unwrap();
        let iter = execute(plan, Rc::new(self.fields), reader_inst);
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
        self.filters.add_field_equal(Rc::new(field.clone()), value);
        self.fields.add_field(field.clone());
    }
    pub fn cond_is<V>(&mut self, field: Field<T, Option<V>>, value: V)
    where
        V: ValueCompare + SolveQueryValue + 'static,
    {
        self.filters.add_field_is(Rc::new(field.clone()), value);
        self.fields.add_field(field.clone());
    }
    pub fn cond_contains<V>(&mut self, field: Field<T, Vec<V>>, value: V)
    where
        V: ValueCompare + SolveQueryValue + 'static,
    {
        self.filters.add_field_contains(Rc::new(field.clone()), value);
        self.fields.add_field(field.clone());
    }

    pub fn cond_range<V, R: RangeBounds<V>>(&mut self, field: Field<T, V>, range: R)
    where
        V: ValueRange + SolveQueryRange + Clone + 'static,
    {
        self.filters
            .add_field_range(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        self.fields.add_field_ord(field.clone());
    }

    pub fn cond_range_contains<V, R: RangeBounds<V>>(&mut self, field: Field<T, Vec<V>>, range: R)
    where
        V: ValueRange + SolveQueryRange + Clone + PartialOrd + 'static,
        Vec<V>: ValueRange,
    {
        self.filters
            .add_field_range_contains(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        self.fields.add_field_ord(field.clone());
    }

    pub fn cond_range_is<V, R: RangeBounds<V>>(&mut self, field: Field<T, Option<V>>, range: R)
    where
        V: ValueRange + SolveQueryRange + Clone + PartialOrd + 'static,
        Option<V>: ValueRange,
    {
        self.filters
            .add_field_range_is(Rc::new(field.clone()), (&range.start_bound(), &range.end_bound()));
        self.fields.add_field_ord(field.clone());
    }

    pub fn simple_persistent_embedded<V>(&mut self, field: Field<T, V>, filter: FilterBuilder<V>)
    where
        V: PersistentEmbedded + 'static,
    {
        let FilterBuilder {
            filters: filter,
            orders,
            fields: fields_holder,
        } = filter;

        self.fields.add_nested_field(field.clone(), fields_holder);
        self.filters.add_field_embedded(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_embedded(Rc::new(field.clone()), orders));
    }

    pub fn ref_query<V>(&mut self, field: Field<T, Ref<V>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        let FilterBuilder {
            filters: filter,
            fields: fields_holder,
            orders,
        } = query;
        self.fields.add_field_ref(field.clone(), fields_holder.clone());
        self.filters.add_field_ref_query_equal(Rc::new(field.clone()), filter);
        self.orders
            .push(OrdersModel::new_query_equal(Rc::new(field.clone()), orders));
    }

    pub fn ref_vec_query<V>(&mut self, field: Field<T, Vec<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        let FilterBuilder {
            filters,
            fields,
            orders,
        } = query;
        self.fields.add_field_vec_ref(field.clone(), fields.clone());
        self.filters
            .add_field_ref_query_contains(Rc::new(field.clone()), filters);
        self.orders
            .push(OrdersModel::new_query_contains(Rc::new(field.clone()), orders));
    }

    pub fn ref_option_query<V>(&mut self, field: Field<T, Option<Ref<V>>>, query: FilterBuilder<V>)
    where
        V: Persistent + 'static,
    {
        let FilterBuilder {
            filters,
            fields,
            orders,
        } = query;
        self.fields.add_field_option_ref(field.clone(), fields.clone());
        self.orders
            .push(OrdersModel::new_query_is(Rc::new(field.clone()), orders));
        self.filters.add_field_ref_query_is(Rc::new(field.clone()), filters);
    }

    pub fn or(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            mut filters,
            orders,
            fields,
        } = builder;
        filters.mode = FilterMode::Or;
        self.fields.merge(fields);
        self.filters.add_group(filters);
        self.orders.extend(orders);
    }

    pub fn and(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            mut filters,
            fields,
            orders,
        } = builder;
        filters.mode = FilterMode::And;
        self.fields.merge(fields);
        self.filters.add_group(filters);
        self.orders.extend(orders);
    }

    pub fn and_filter(&mut self, filters: FilterBuilder<T>) {
        let FilterBuilder {
            mut filters,
            fields,
            orders,
        } = filters;
        filters.mode = FilterMode::And;
        self.fields.merge(fields);
        self.filters.add_group(filters);
        self.orders.extend(orders);
    }

    pub fn not(&mut self, builder: FilterBuilder<T>) {
        let FilterBuilder {
            mut filters,
            fields,
            orders,
        } = builder;
        filters.mode = FilterMode::Not;
        self.filters.add_group(filters);
        self.fields.merge(fields);
        self.orders.extend(orders);
    }

    pub fn order<V: ValueRange + Ord + 'static>(&mut self, field: Field<T, V>, order: Order) {
        self.orders
            .push(OrdersModel::new_field(Rc::new(field.clone()), order.clone()));
        self.fields.add_field_ord(field.clone());
    }
}
