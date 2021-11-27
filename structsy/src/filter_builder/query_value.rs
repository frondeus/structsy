use persy::PersyId;
use std::ops::Bound;
struct RawRef {
    id: PersyId,
    ty: String,
}

enum SimpleQueryValue {
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
    Embedded(FilterHolder),
}

enum QueryValue {
    Single(SimpleQueryValue),
    Option(Option<SimpleQueryValue>),
    OptionVec(Option<Vec<SimpleQueryValue>>),
    Vec(Vec<SimpleQueryValue>),
    Filter(FilterHolder),
}

struct FilterItem {
    field: String,
    filter_type: FilterType,
}
struct FilterHolder {
    filters: Vec<FilterType>,
    mode: FilterMode,
}
enum FilterMode {
    And,
    Or,
    Not,
}

enum FilterType {
    Equal(QueryValue),
    Contains(QueryValue),
    Is(QueryValue),
    Range((Bound<QueryValue>, Bound<QueryValue>)),
    RangeContains((Bound<QueryValue>, Bound<QueryValue>)),
    RangeIs((Bound<QueryValue>, Bound<QueryValue>)),
}
