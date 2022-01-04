use crate::{error::SRes, internal::EmbeddedDescription, Order, Persistent, PersistentEmbedded, Ref};
use persy::PersyId;
use std::{fmt::Debug, ops::Bound};

pub trait MyOrd {}
pub trait MyEq: Debug {}
struct Value<T> {
    value: T,
}
impl<T: Ord> MyOrd for Value<T> {}
impl<T: PartialEq> MyEq for Value<T> {}

impl<T> std::fmt::Debug for Value<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "embedded value")
    }
}

#[derive(Debug)]
pub struct RawRef {
    id: PersyId,
    ty: String,
}

#[derive(Debug)]
pub enum SimpleQueryValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    F32(f32),
    F64(f64),
    Bool(bool),
    String(String),
    Ref(RawRef),
    Embedded(Box<dyn MyEq>),
}

pub trait SolveSimpleQueryValue {
    fn new(self) -> SRes<SimpleQueryValue>;
}

macro_rules! impl_query_type {
    ($t:ident,$v1:ident) => {
        impl SolveSimpleQueryValue for $t {
            fn new(self) -> SRes<SimpleQueryValue> {
                Ok(SimpleQueryValue::$v1(self))
            }
        }
    };
}

impl_query_type!(u8, U8);
impl_query_type!(u16, U16);
impl_query_type!(u32, U32);
impl_query_type!(u64, U64);
impl_query_type!(u128, U128);
impl_query_type!(i8, I8);
impl_query_type!(i16, I16);
impl_query_type!(i32, I32);
impl_query_type!(i64, I64);
impl_query_type!(i128, I128);
impl_query_type!(f32, F32);
impl_query_type!(f64, F64);
impl_query_type!(bool, Bool);
impl_query_type!(String, String);

impl<T: Persistent> SolveSimpleQueryValue for Ref<T> {
    fn new(self) -> SRes<SimpleQueryValue> {
        Ok(SimpleQueryValue::Ref(RawRef {
            ty: T::get_name().to_owned(),
            id: self.raw_id,
        }))
    }
}

impl<T: PartialEq + EmbeddedDescription + 'static> SolveSimpleQueryValue for T {
    fn new(self) -> SRes<SimpleQueryValue> {
        Ok(SimpleQueryValue::Embedded(Box::new(Value { value: self })))
    }
}

pub trait SolveQueryValue {
    fn new(self) -> SRes<QueryValue>;
}

impl<T: SolveSimpleQueryValue> SolveQueryValue for T {
    fn new(self) -> SRes<QueryValue> {
        Ok(QueryValue::Single(self.new()?))
    }
}

impl<T: SolveSimpleQueryValue> SolveQueryValue for Option<T> {
    fn new(self) -> SRes<QueryValue> {
        Ok(QueryValue::Option(self.map(|v| v.new()).transpose()?))
    }
}

impl<T: SolveSimpleQueryValue> SolveQueryValue for Option<Vec<T>> {
    fn new(self) -> SRes<QueryValue> {
        Ok(QueryValue::OptionVec(
            self.map(|vec| {
                vec.into_iter()
                    .map(|v| v.new())
                    .collect::<SRes<Vec<SimpleQueryValue>>>()
            })
            .transpose()?,
        ))
    }
}

impl<T: SolveSimpleQueryValue> SolveQueryValue for Vec<T> {
    fn new(self) -> SRes<QueryValue> {
        Ok(QueryValue::Vec(
            self.into_iter()
                .map(|v| v.new())
                .collect::<SRes<Vec<SimpleQueryValue>>>()?,
        ))
    }
}

#[derive(Debug)]
pub enum QueryValue {
    Single(SimpleQueryValue),
    Option(Option<SimpleQueryValue>),
    OptionVec(Option<Vec<SimpleQueryValue>>),
    Vec(Vec<SimpleQueryValue>),
}

#[derive(Debug)]
pub(crate) struct FilterFieldItem {
    pub(crate) field: String,
    pub(crate) filter_type: FilterType,
}

#[derive(Debug)]
pub(crate) enum FilterItem {
    Field(FilterFieldItem),
    Group(FilterHolder),
}

#[derive(Debug)]
pub(crate) struct FilterHolder {
    pub(crate) filters: Vec<FilterItem>,
    pub(crate) mode: FilterMode,
}
fn bound_value<X: Clone + SolveQueryValue>(bound: &Bound<&X>) -> Bound<QueryValue> {
    match bound {
        Bound::Included(x) => Bound::Included(SolveQueryValue::new((*x).clone()).unwrap()),
        Bound::Excluded(x) => Bound::Excluded(SolveQueryValue::new((*x).clone()).unwrap()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
impl FilterHolder {
    pub(crate) fn new(mode: FilterMode) -> Self {
        Self {
            filters: Vec::new(),
            mode,
        }
    }
    pub(crate) fn add_field_equal<T: SolveQueryValue>(&mut self, name: &str, value: T) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::Equal(value.new().unwrap()),
        }))
    }
    pub(crate) fn add_field_is<T: SolveQueryValue>(&mut self, name: &str, value: T) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::Is(value.new().unwrap()),
        }))
    }
    pub(crate) fn add_field_contains<T: SolveQueryValue>(&mut self, name: &str, value: T) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::Contains(value.new().unwrap()),
        }))
    }

    pub(crate) fn add_field_range<T: SolveQueryValue + Clone>(
        &mut self,
        name: &str,
        (first, second): (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::Range((bound_value(first), bound_value(second))),
        }))
    }

    pub(crate) fn add_field_range_is<T: SolveQueryValue + Clone>(
        &mut self,
        name: &str,
        (first, second): (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::RangeIs((bound_value(first), bound_value(second))),
        }))
    }

    pub(crate) fn add_field_range_contains<T: SolveQueryValue + Clone>(
        &mut self,
        name: &str,
        (first, second): (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::RangeContains((bound_value(first), bound_value(second))),
        }))
    }

    pub(crate) fn add_field_embedded(&mut self, name: &str, filter: FilterHolder) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::Embedded(filter),
        }))
    }
    pub(crate) fn add_field_ref_query_equal(&mut self, name: &str, filter: FilterHolder) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::QueryEqual(filter),
        }))
    }
    pub(crate) fn add_field_ref_query_contains(&mut self, name: &str, filter: FilterHolder) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::QueryContains(filter),
        }))
    }
    pub(crate) fn add_field_ref_query_is(&mut self, name: &str, filter: FilterHolder) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: FilterType::QueryIs(filter),
        }))
    }

    pub(crate) fn add_group(&mut self, filter: FilterHolder) {
        self.filters.push(FilterItem::Group(filter))
    }
}

#[derive(Ord, Eq, PartialEq, PartialOrd, Debug)]
pub(crate) enum FilterMode {
    And,
    Or,
    Not,
}

#[derive(Debug)]
pub(crate) enum FilterType {
    Equal(QueryValue),
    Contains(QueryValue),
    Is(QueryValue),
    Range((Bound<QueryValue>, Bound<QueryValue>)),
    RangeContains((Bound<QueryValue>, Bound<QueryValue>)),
    RangeIs((Bound<QueryValue>, Bound<QueryValue>)),
    Embedded(FilterHolder),
    QueryEqual(FilterHolder),
    QueryContains(FilterHolder),
    QueryIs(FilterHolder),
}

#[derive(Debug)]
pub(crate) struct OrdersFilters {
    pub(crate) orders: Vec<Orders>,
    pub(crate) filter: FilterHolder,
}
impl OrdersFilters {
    pub(crate) fn new(mode: FilterMode) -> Self {
        Self {
            orders: Vec::new(),
            filter: FilterHolder::new(mode),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Query {
    pub(crate) type_name: String,
    pub(crate) projections: Vec<Projection>,
    pub(crate) builder: OrdersFilters,
}
#[derive(Debug)]
pub(crate) struct Projection {
    field: String,
}
#[derive(Debug)]
pub(crate) enum Orders {
    Field(FieldOrder),
    Embeeded(FieldNestedOrders),
    QueryEqual(FieldNestedOrders),
    QueryIs(FieldNestedOrders),
    QueryContains(FieldNestedOrders),
}
impl Orders {
    pub(crate) fn new_field(name: &str, order: Order) -> Orders {
        Orders::Field(FieldOrder {
            field: name.to_owned(),
            mode: order,
        })
    }

    pub(crate) fn new_embedded(name: &str, orders: Vec<Orders>) -> Orders {
        Orders::Embeeded(FieldNestedOrders {
            field: name.to_owned(),
            orders,
        })
    }
    pub(crate) fn new_query_equal(name: &str, orders: Vec<Orders>) -> Orders {
        Orders::QueryEqual(FieldNestedOrders {
            field: name.to_owned(),
            orders,
        })
    }

    pub(crate) fn new_query_is(name: &str, orders: Vec<Orders>) -> Orders {
        Orders::QueryIs(FieldNestedOrders {
            field: name.to_owned(),
            orders,
        })
    }
    pub(crate) fn new_query_contains(name: &str, orders: Vec<Orders>) -> Orders {
        Orders::QueryContains(FieldNestedOrders {
            field: name.to_owned(),
            orders,
        })
    }
}

#[derive(Debug)]
pub(crate) struct FieldOrder {
    pub(crate) field: String,
    pub(crate) mode: Order,
}
#[derive(Debug)]
pub(crate) struct FieldNestedOrders {
    pub(crate) field: String,
    pub(crate) orders: Vec<Orders>,
}
