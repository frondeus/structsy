use crate::{
    error::SRes,
    internal::{EmbeddedDescription, FieldInfo},
    Order, Persistent, Ref,
};
use persy::PersyId;
use std::{any::Any, cmp::Ordering};
use std::{fmt::Debug, ops::Bound, rc::Rc};

pub trait MyEq {
    fn my_eq(&self, other: &dyn MyEq) -> bool;
    fn gen_ref(&self) -> &dyn Any;
}

impl<T: EmbeddedDescription + PartialEq + 'static> MyEq for T {
    fn my_eq(&self, other: &dyn MyEq) -> bool {
        if let Some(x) = other.gen_ref().downcast_ref::<T>() {
            self.eq(x)
        } else {
            false
        }
    }
    fn gen_ref(&self) -> &dyn Any {
        self
    }
}

pub trait MyOrd {
    fn my_cmp(&self, other: &dyn MyOrd) -> Option<Ordering>;
    fn my_eq(&self, other: &dyn MyOrd) -> bool;
    fn gen_ref(&self) -> &dyn Any;
}

impl<'a> PartialEq<EmbValue<'a>> for dyn MyOrd {
    fn eq(&self, other: &EmbValue) -> bool {
        other.eq(self)
    }
}
impl<'a> PartialOrd<EmbValue<'a>> for dyn MyOrd {
    fn partial_cmp(&self, other: &EmbValue) -> Option<Ordering> {
        other.partial_cmp(self).map(|r| match r {
            Ordering::Equal => Ordering::Equal,
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
        })
    }
}
impl<T: MyOrd + 'static> MyOrd for Option<T> {
    fn my_cmp(&self, other: &dyn MyOrd) -> Option<Ordering> {
        match (self, other.gen_ref().downcast_ref::<Option<T>>()) {
            (Some(f), Some(s)) => f.my_cmp(s),
            (None, Some(_)) => Some(Ordering::Less),
            (Some(_), None) => Some(Ordering::Greater),
            (None, None) => Some(Ordering::Equal),
        }
    }
    fn my_eq(&self, other: &dyn MyOrd) -> bool {
        match (self, other.gen_ref().downcast_ref::<Option<T>>()) {
            (Some(f), Some(s)) => f.my_eq(s),
            (None, Some(_)) => false,
            (Some(_), None) => false,
            (None, None) => true,
        }
    }
    fn gen_ref(&self) -> &dyn Any {
        self
    }
}

impl<T: EmbeddedDescription + PartialOrd + 'static> MyOrd for T {
    fn my_cmp(&self, other: &dyn MyOrd) -> Option<Ordering> {
        if let Some(x) = other.gen_ref().downcast_ref::<T>() {
            self.partial_cmp(x)
        } else {
            None
        }
    }
    fn my_eq(&self, other: &dyn MyOrd) -> bool {
        if let Some(x) = other.gen_ref().downcast_ref::<T>() {
            self.partial_cmp(x) == Some(Ordering::Equal)
        } else {
            false
        }
    }
    fn gen_ref(&self) -> &dyn Any {
        self
    }
}

impl<'a> PartialEq for EmbValue<'a> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::OrdType(r) => match other {
                Self::OrdType(or) => r.my_cmp(&**or) == Some(Ordering::Equal),
                Self::OrdRef(or) => r.my_cmp(&**or) == Some(Ordering::Equal),
                Self::EqType(_) => false,
            },
            Self::OrdRef(r) => match other {
                Self::OrdType(or) => r.my_cmp(&**or) == Some(Ordering::Equal),
                Self::OrdRef(or) => r.my_cmp(&**or) == Some(Ordering::Equal),
                Self::EqType(_) => false,
            },
            Self::EqType(e) => match other {
                Self::OrdType(_) => false,
                Self::OrdRef(_) => false,
                Self::EqType(eq) => e.my_eq(&**eq),
            },
        }
    }
}
impl<'a> PartialOrd for EmbValue<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Self::OrdType(r) => match other {
                Self::OrdType(or) => r.my_cmp(&**or),
                Self::OrdRef(or) => r.my_cmp(&**or),
                Self::EqType(_) => None,
            },
            Self::OrdRef(r) => match other {
                Self::OrdType(or) => r.my_cmp(&**or),
                Self::OrdRef(or) => r.my_cmp(&**or),
                Self::EqType(_) => None,
            },
            Self::EqType(_) => None,
        }
    }
}

impl<'a> PartialEq<dyn MyEq> for EmbValue<'a> {
    fn eq(&self, other: &dyn MyEq) -> bool {
        match self {
            Self::OrdType(r) => false, //r.my_eq(other),
            Self::OrdRef(r) => false,  // r.my_eq(other),
            Self::EqType(e) => e.my_eq(other),
        }
    }
}

impl<'a> PartialEq<dyn MyOrd> for EmbValue<'a> {
    fn eq(&self, other: &dyn MyOrd) -> bool {
        match self {
            Self::OrdType(r) => r.my_cmp(other) == Some(Ordering::Equal),
            Self::OrdRef(r) => r.my_cmp(other) == Some(Ordering::Equal),
            Self::EqType(_) => false,
        }
    }
}

impl<'a> PartialOrd<dyn MyOrd> for EmbValue<'a> {
    fn partial_cmp(&self, other: &dyn MyOrd) -> Option<Ordering> {
        match self {
            Self::OrdType(r) => r.my_cmp(other),
            Self::OrdRef(r) => r.my_cmp(other),
            Self::EqType(_) => None,
        }
    }
}
#[derive(Clone)]
pub enum EmbValue<'a> {
    //TODO: handle also the case of only eq and both eq and ord maybe with an enum
    OrdType(Rc<dyn MyOrd>),
    OrdRef(&'a dyn MyOrd),
    EqType(Rc<dyn MyEq>),
}

impl<'a> EmbValue<'a> {
    pub(crate) fn new<T: EmbeddedDescription + PartialOrd + 'static>(t: T) -> Self {
        Self::OrdType(Rc::new(t))
    }
    pub(crate) fn new_ref<T: EmbeddedDescription + PartialOrd + 'static>(t: &'a T) -> Self {
        Self::OrdRef(t)
    }
    pub(crate) fn new_eq<T: EmbeddedDescription + PartialEq + 'static>(t: T) -> Self {
        Self::EqType(Rc::new(t))
    }
    pub(crate) fn my_eq_ord(&self, other: &dyn MyOrd) -> bool {
        match self {
            Self::OrdType(r) => r.my_cmp(other) == Some(Ordering::Equal),
            Self::OrdRef(r) => r.my_cmp(other) == Some(Ordering::Equal),
            Self::EqType(_) => false,
        }
    }
    pub(crate) fn my_cmp_ord(&self, other: &dyn MyOrd) -> Option<Ordering> {
        match self {
            Self::OrdType(r) => r.my_cmp(other),
            Self::OrdRef(r) => r.my_cmp(other),
            Self::EqType(_) => None,
        }
    }
}

impl<'a> Debug for EmbValue<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OrdType(r) => {
                write!(f, "{:?}", r.gen_ref().type_id())
            }
            Self::OrdRef(r) => {
                write!(f, "{:?}", r.gen_ref().type_id())
            }
            Self::EqType(e) => {
                write!(f, "{:?}", e.gen_ref().type_id())
            }
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct RawRef {
    pub(crate) id: PersyId,
    pub(crate) ty: String,
}
impl RawRef {
    pub(crate) fn into_ref<T>(&self) -> Ref<T> {
        Ref {
            type_name: self.ty.clone(),
            raw_id: self.id.clone(),
            ph: std::marker::PhantomData,
        }
    }
}
impl<T> From<&Ref<T>> for RawRef {
    fn from(Ref { type_name, raw_id, .. }: &Ref<T>) -> Self {
        Self {
            id: raw_id.clone(),
            ty: type_name.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
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
    Embedded(EmbValue<'static>),
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
        Ok(SimpleQueryValue::Embedded(EmbValue::new_eq(self)))
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
    pub(crate) field: Rc<dyn FieldInfo>,
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
    pub(crate) fn add_field_equal<T: SolveQueryValue>(&mut self, field: Rc<dyn FieldInfo>, value: T) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::Equal(value.new().unwrap()),
        }))
    }
    pub(crate) fn add_field_is<T: SolveQueryValue>(&mut self, field: Rc<dyn FieldInfo>, value: T) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::Is(value.new().unwrap()),
        }))
    }
    pub(crate) fn add_field_contains<T: SolveQueryValue>(&mut self, field: Rc<dyn FieldInfo>, value: T) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::Contains(value.new().unwrap()),
        }))
    }

    pub(crate) fn add_field_range<T: SolveQueryValue + Clone>(
        &mut self,
        field: Rc<dyn FieldInfo>,
        (first, second): (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::Range((bound_value(first), bound_value(second))),
        }))
    }

    pub(crate) fn add_field_range_is<T: SolveQueryValue + Clone>(
        &mut self,
        field: Rc<dyn FieldInfo>,
        (first, second): (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::RangeIs((bound_value(first), bound_value(second))),
        }))
    }

    pub(crate) fn add_field_range_contains<T: SolveQueryValue + Clone>(
        &mut self,
        field: Rc<dyn FieldInfo>,
        (first, second): (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::RangeContains((bound_value(first), bound_value(second))),
        }))
    }

    pub(crate) fn add_field_embedded(&mut self, field: Rc<dyn FieldInfo>, filter: FilterHolder) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::Embedded(filter),
        }))
    }
    pub(crate) fn add_field_ref_query_equal(&mut self, field: Rc<dyn FieldInfo>, filter: FilterHolder) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::QueryEqual(filter),
        }))
    }
    pub(crate) fn add_field_ref_query_contains(&mut self, field: Rc<dyn FieldInfo>, filter: FilterHolder) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::QueryContains(filter),
        }))
    }
    pub(crate) fn add_field_ref_query_is(&mut self, field: Rc<dyn FieldInfo>, filter: FilterHolder) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
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
    pub(crate) field: String,
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
    pub(crate) fn new_field(name: Rc<dyn FieldInfo>, order: Order) -> Orders {
        Orders::Field(FieldOrder {
            field: name,
            mode: order,
        })
    }

    pub(crate) fn new_embedded(name: Rc<dyn FieldInfo>, orders: Vec<Orders>) -> Orders {
        Orders::Embeeded(FieldNestedOrders { field: name, orders })
    }
    pub(crate) fn new_query_equal(name: Rc<dyn FieldInfo>, orders: Vec<Orders>) -> Orders {
        Orders::QueryEqual(FieldNestedOrders { field: name, orders })
    }

    pub(crate) fn new_query_is(name: Rc<dyn FieldInfo>, orders: Vec<Orders>) -> Orders {
        Orders::QueryIs(FieldNestedOrders { field: name, orders })
    }
    pub(crate) fn new_query_contains(name: Rc<dyn FieldInfo>, orders: Vec<Orders>) -> Orders {
        Orders::QueryContains(FieldNestedOrders { field: name, orders })
    }
}

#[derive(Debug)]
pub(crate) struct FieldOrder {
    pub(crate) field: Rc<dyn FieldInfo>,
    pub(crate) mode: Order,
}
#[derive(Debug)]
pub(crate) struct FieldNestedOrders {
    pub(crate) field: Rc<dyn FieldInfo>,
    pub(crate) orders: Vec<Orders>,
}
