use crate::Order;
use persy::PersyId;
use std::ops::Bound;

pub(crate) struct RawRef {
    id: PersyId,
    ty: String,
}

pub(crate) enum SimpleQueryValue {
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
    Embedded(Query),
}

pub(crate) enum QueryValue {
    Single(SimpleQueryValue),
    Option(Option<SimpleQueryValue>),
    OptionVec(Option<Vec<SimpleQueryValue>>),
    Vec(Vec<SimpleQueryValue>),
    Query(Query),
}

pub(crate) struct FilterItem {
    field: String,
    filter_type: FilterType,
}
pub(crate) struct FilterHolder {
    filters: Vec<FilterItem>,
    mode: FilterMode,
}
pub(crate) enum FilterMode {
    And,
    Or,
    Not,
}

pub(crate) enum FilterType {
    Equal(QueryValue),
    Contains(QueryValue),
    Is(QueryValue),
    Range((Bound<QueryValue>, Bound<QueryValue>)),
    RangeContains((Bound<QueryValue>, Bound<QueryValue>)),
    RangeIs((Bound<QueryValue>, Bound<QueryValue>)),
}

pub(crate) struct Query {
    projections: Vec<Projection>,
    orders: Vec<Orders>,
    filter: FilterHolder,
}
pub(crate) struct Projection {
    field: String,
}
pub(crate) enum Orders {
    Field(FieldOrder),
    Embeeded(FieldOrderEmbedded),
}
pub(crate) struct FieldOrder {
    field: String,
    mode: Order,
}
pub(crate) struct FieldOrderEmbedded {
    field: String,
    orders: Vec<Orders>,
}
