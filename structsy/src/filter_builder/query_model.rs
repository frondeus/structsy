use crate::{error::SRes, internal::EmbeddedDescription, Order, Persistent, PersistentEmbedded, Ref};
use persy::PersyId;
use std::{fmt::Debug, ops::Bound};

pub trait MyOrd: std::fmt::Debug {}
struct Value<T> {
    value: T,
}
impl<T: Ord> MyOrd for Value<T> {}

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
    Embedded(Box<dyn MyOrd>),
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

impl<T: MyOrd + PersistentEmbedded + 'static> SolveSimpleQueryValue for T {
    fn new(self) -> SRes<SimpleQueryValue> {
        Ok(SimpleQueryValue::Embedded(Box::new(self)))
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
    /*
    Query(Query),
    Embedded(OrdersFilters),
    OptionEmbedded(OrdersFilters),
    VecEmbedded(OrdersFilters),
    */
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
impl FilterHolder {
    pub(crate) fn new(mode: FilterMode) -> Self {
        Self {
            filters: Vec::new(),
            mode,
        }
    }
    pub(crate) fn add_field(&mut self, name: &str, filter: FilterType) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field: name.to_owned(),
            filter_type: filter,
        }))
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
    Embeeded(FieldOrderEmbedded),
}
#[derive(Debug)]
pub(crate) struct FieldOrder {
    field: String,
    mode: Order,
}
#[derive(Debug)]
pub(crate) struct FieldOrderEmbedded {
    field: String,
    orders: Vec<Orders>,
}
