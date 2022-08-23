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
            Self::OrdType(_r) => false, //r.my_eq(other),
            Self::OrdRef(_r) => false,  // r.my_eq(other),
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
    pub(crate) fn new_eq<T: EmbeddedDescription + PartialEq + 'static>(t: T) -> Self {
        Self::EqType(Rc::new(t))
    }
    pub(crate) fn new_ord<T: EmbeddedDescription + PartialOrd + 'static>(t: T) -> Self {
        Self::OrdType(Rc::new(t))
    }
    pub(crate) fn new_ord_ref<T: EmbeddedDescription + PartialOrd + 'static>(t: &'a T) -> Self {
        Self::OrdRef(t)
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

impl SimpleQueryValue {
    pub(crate) fn to_range(&self) -> RangeQueryValue {
        match self {
            SimpleQueryValue::U8(v) => RangeQueryValue::U8((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::U16(v) => RangeQueryValue::U16((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::U32(v) => RangeQueryValue::U32((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::U64(v) => RangeQueryValue::U64((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::U128(v) => {
                RangeQueryValue::U128((Bound::Included(v.clone()), Bound::Included(v.clone())))
            }
            SimpleQueryValue::I8(v) => RangeQueryValue::I8((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::I16(v) => RangeQueryValue::I16((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::I32(v) => RangeQueryValue::I32((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::I64(v) => RangeQueryValue::I64((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::I128(v) => {
                RangeQueryValue::I128((Bound::Included(v.clone()), Bound::Included(v.clone())))
            }
            SimpleQueryValue::F32(v) => RangeQueryValue::F32((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::F64(v) => RangeQueryValue::F64((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::Bool(v) => {
                RangeQueryValue::Bool((Bound::Included(v.clone()), Bound::Included(v.clone())))
            }
            SimpleQueryValue::String(v) => {
                RangeQueryValue::String((Bound::Included(v.clone()), Bound::Included(v.clone())))
            }
            SimpleQueryValue::Ref(v) => RangeQueryValue::Ref((Bound::Included(v.clone()), Bound::Included(v.clone()))),
            SimpleQueryValue::Embedded(v) => {
                RangeQueryValue::Embedded((Bound::Included(v.clone()), Bound::Included(v.clone())))
            }
        }
    }

    pub(crate) fn to_range_option(&self) -> OptionRangeQueryValue {
        match self {
            SimpleQueryValue::U8(v) => {
                OptionRangeQueryValue::U8((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::U16(v) => {
                OptionRangeQueryValue::U16((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::U32(v) => {
                OptionRangeQueryValue::U32((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::U64(v) => {
                OptionRangeQueryValue::U64((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::U128(v) => {
                OptionRangeQueryValue::U128((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::I8(v) => {
                OptionRangeQueryValue::I8((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::I16(v) => {
                OptionRangeQueryValue::I16((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::I32(v) => {
                OptionRangeQueryValue::I32((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::I64(v) => {
                OptionRangeQueryValue::I64((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::I128(v) => {
                OptionRangeQueryValue::I128((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::F32(v) => {
                OptionRangeQueryValue::F32((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::F64(v) => {
                OptionRangeQueryValue::F64((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::Bool(v) => {
                OptionRangeQueryValue::Bool((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::String(v) => {
                OptionRangeQueryValue::String((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::Ref(v) => {
                OptionRangeQueryValue::Ref((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
            SimpleQueryValue::Embedded(v) => {
                OptionRangeQueryValue::Embedded((Bound::Included(Some(v.clone())), Bound::Included(Some(v.clone()))))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RangeQueryValue {
    U8((Bound<u8>, Bound<u8>)),
    U16((Bound<u16>, Bound<u16>)),
    U32((Bound<u32>, Bound<u32>)),
    U64((Bound<u64>, Bound<u64>)),
    U128((Bound<u128>, Bound<u128>)),
    I8((Bound<i8>, Bound<i8>)),
    I16((Bound<i16>, Bound<i16>)),
    I32((Bound<i32>, Bound<i32>)),
    I64((Bound<i64>, Bound<i64>)),
    I128((Bound<i128>, Bound<i128>)),
    F32((Bound<f32>, Bound<f32>)),
    F64((Bound<f64>, Bound<f64>)),
    Bool((Bound<bool>, Bound<bool>)),
    String((Bound<String>, Bound<String>)),
    Ref((Bound<RawRef>, Bound<RawRef>)),
    Embedded((Bound<EmbValue<'static>>, Bound<EmbValue<'static>>)),
    Option(OptionRangeQueryValue),
    OptionVec(OptionVecRangeQueryValue),
    Vec(VecRangeQueryValue),
}

#[derive(Debug, Clone, PartialEq)]
pub enum VecRangeQueryValue {
    U8((Bound<Vec<u8>>, Bound<Vec<u8>>)),
    U16((Bound<Vec<u16>>, Bound<Vec<u16>>)),
    U32((Bound<Vec<u32>>, Bound<Vec<u32>>)),
    U64((Bound<Vec<u64>>, Bound<Vec<u64>>)),
    U128((Bound<Vec<u128>>, Bound<Vec<u128>>)),
    I8((Bound<Vec<i8>>, Bound<Vec<i8>>)),
    I16((Bound<Vec<i16>>, Bound<Vec<i16>>)),
    I32((Bound<Vec<i32>>, Bound<Vec<i32>>)),
    I64((Bound<Vec<i64>>, Bound<Vec<i64>>)),
    I128((Bound<Vec<i128>>, Bound<Vec<i128>>)),
    F32((Bound<Vec<f32>>, Bound<Vec<f32>>)),
    F64((Bound<Vec<f64>>, Bound<Vec<f64>>)),
    Bool((Bound<Vec<bool>>, Bound<Vec<bool>>)),
    String((Bound<Vec<String>>, Bound<Vec<String>>)),
    Ref((Bound<Vec<RawRef>>, Bound<Vec<RawRef>>)),
    Embedded((Bound<Vec<EmbValue<'static>>>, Bound<Vec<EmbValue<'static>>>)),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptionRangeQueryValue {
    U8((Bound<Option<u8>>, Bound<Option<u8>>)),
    U16((Bound<Option<u16>>, Bound<Option<u16>>)),
    U32((Bound<Option<u32>>, Bound<Option<u32>>)),
    U64((Bound<Option<u64>>, Bound<Option<u64>>)),
    U128((Bound<Option<u128>>, Bound<Option<u128>>)),
    I8((Bound<Option<i8>>, Bound<Option<i8>>)),
    I16((Bound<Option<i16>>, Bound<Option<i16>>)),
    I32((Bound<Option<i32>>, Bound<Option<i32>>)),
    I64((Bound<Option<i64>>, Bound<Option<i64>>)),
    I128((Bound<Option<i128>>, Bound<Option<i128>>)),
    F32((Bound<Option<f32>>, Bound<Option<f32>>)),
    F64((Bound<Option<f64>>, Bound<Option<f64>>)),
    Bool((Bound<Option<bool>>, Bound<Option<bool>>)),
    String((Bound<Option<String>>, Bound<Option<String>>)),
    Ref((Bound<Option<RawRef>>, Bound<Option<RawRef>>)),
    Embedded((Bound<Option<EmbValue<'static>>>, Bound<Option<EmbValue<'static>>>)),
}
#[derive(Debug, Clone, PartialEq)]
pub enum OptionVecRangeQueryValue {
    U8((Bound<Option<Vec<u8>>>, Bound<Option<Vec<u8>>>)),
    U16((Bound<Option<Vec<u16>>>, Bound<Option<Vec<u16>>>)),
    U32((Bound<Option<Vec<u32>>>, Bound<Option<Vec<u32>>>)),
    U64((Bound<Option<Vec<u64>>>, Bound<Option<Vec<u64>>>)),
    U128((Bound<Option<Vec<u128>>>, Bound<Option<Vec<u128>>>)),
    I8((Bound<Option<Vec<i8>>>, Bound<Option<Vec<i8>>>)),
    I16((Bound<Option<Vec<i16>>>, Bound<Option<Vec<i16>>>)),
    I32((Bound<Option<Vec<i32>>>, Bound<Option<Vec<i32>>>)),
    I64((Bound<Option<Vec<i64>>>, Bound<Option<Vec<i64>>>)),
    I128((Bound<Option<Vec<i128>>>, Bound<Option<Vec<i128>>>)),
    F32((Bound<Option<Vec<f32>>>, Bound<Option<Vec<f32>>>)),
    F64((Bound<Option<Vec<f64>>>, Bound<Option<Vec<f64>>>)),
    Bool((Bound<Option<Vec<bool>>>, Bound<Option<Vec<bool>>>)),
    String((Bound<Option<Vec<String>>>, Bound<Option<Vec<String>>>)),
    Ref((Bound<Option<Vec<RawRef>>>, Bound<Option<Vec<RawRef>>>)),
    Embedded(
        (
            Bound<Option<Vec<EmbValue<'static>>>>,
            Bound<Option<Vec<EmbValue<'static>>>>,
        ),
    ),
}

pub trait SolveSimpleQueryValue {
    fn new(self) -> SRes<SimpleQueryValue>;
}

pub trait SolveRangeQueryValue {
    fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue;
    fn range_vec(val: (&Bound<&Vec<Self>>, &Bound<&Vec<Self>>)) -> VecRangeQueryValue
    where
        Self: Sized;
    fn range_option(val: (&Bound<&Option<Self>>, &Bound<&Option<Self>>)) -> OptionRangeQueryValue
    where
        Self: Sized;
    fn range_option_vec(val: (&Bound<&Option<Vec<Self>>>, &Bound<&Option<Vec<Self>>>)) -> OptionVecRangeQueryValue
    where
        Self: Sized;
}

macro_rules! impl_query_type {
    ($t:ident,$v1:ident) => {
        impl SolveSimpleQueryValue for $t {
            fn new(self) -> SRes<SimpleQueryValue> {
                Ok(SimpleQueryValue::$v1(self))
            }
        }
        impl SolveRangeQueryValue for $t {
            fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue {
                RangeQueryValue::$v1((val.0.cloned(), val.1.cloned()))
            }
            fn range_vec(val: (&Bound<&Vec<Self>>, &Bound<&Vec<Self>>)) -> VecRangeQueryValue
            where
                Self: Sized,
            {
                VecRangeQueryValue::$v1((val.0.cloned(), val.1.cloned()))
            }
            fn range_option(val: (&Bound<&Option<Self>>, &Bound<&Option<Self>>)) -> OptionRangeQueryValue
            where
                Self: Sized,
            {
                OptionRangeQueryValue::$v1((val.0.cloned(), val.1.cloned()))
            }
            fn range_option_vec(
                val: (&Bound<&Option<Vec<Self>>>, &Bound<&Option<Vec<Self>>>),
            ) -> OptionVecRangeQueryValue
            where
                Self: Sized,
            {
                OptionVecRangeQueryValue::$v1((val.0.cloned(), val.1.cloned()))
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
        Ok(SimpleQueryValue::Ref(RawRef::from(&self)))
    }
}
impl<T: Persistent> SolveRangeQueryValue for Ref<T> {
    fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue {
        let first = match val.0 {
            Bound::Included(v) => Bound::Included(RawRef::from(*v)),
            Bound::Excluded(v) => Bound::Excluded(RawRef::from(*v)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let second = match val.1 {
            Bound::Included(v) => Bound::Included(RawRef::from(*v)),
            Bound::Excluded(v) => Bound::Excluded(RawRef::from(*v)),
            Bound::Unbounded => Bound::Unbounded,
        };
        RangeQueryValue::Ref((first, second))
    }
    fn range_vec(val: (&Bound<&Vec<Self>>, &Bound<&Vec<Self>>)) -> VecRangeQueryValue
    where
        Self: Sized,
    {
        let first = match val.0 {
            Bound::Included(v) => Bound::Included(v.iter().map(RawRef::from).collect()),
            Bound::Excluded(v) => Bound::Excluded(v.iter().map(RawRef::from).collect()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let second = match val.1 {
            Bound::Included(v) => Bound::Included(v.iter().map(RawRef::from).collect()),
            Bound::Excluded(v) => Bound::Excluded(v.iter().map(RawRef::from).collect()),
            Bound::Unbounded => Bound::Unbounded,
        };
        VecRangeQueryValue::Ref((first, second))
    }
    fn range_option(val: (&Bound<&Option<Self>>, &Bound<&Option<Self>>)) -> OptionRangeQueryValue
    where
        Self: Sized,
    {
        let first = match val.0 {
            Bound::Included(v) => Bound::Included(v.as_ref().map(RawRef::from)),
            Bound::Excluded(v) => Bound::Excluded(v.as_ref().map(RawRef::from)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let second = match val.1 {
            Bound::Included(v) => Bound::Included(v.as_ref().map(RawRef::from)),
            Bound::Excluded(v) => Bound::Excluded(v.as_ref().map(RawRef::from)),
            Bound::Unbounded => Bound::Unbounded,
        };
        OptionRangeQueryValue::Ref((first, second))
    }
    fn range_option_vec(val: (&Bound<&Option<Vec<Self>>>, &Bound<&Option<Vec<Self>>>)) -> OptionVecRangeQueryValue
    where
        Self: Sized,
    {
        let first = match val.0 {
            Bound::Included(v) => Bound::Included(v.as_ref().map(|x| x.iter().map(RawRef::from).collect())),
            Bound::Excluded(v) => Bound::Excluded(v.as_ref().map(|x| x.iter().map(RawRef::from).collect())),
            Bound::Unbounded => Bound::Unbounded,
        };
        let second = match val.1 {
            Bound::Included(v) => Bound::Included(v.as_ref().map(|x| x.iter().map(RawRef::from).collect())),
            Bound::Excluded(v) => Bound::Excluded(v.as_ref().map(|x| x.iter().map(RawRef::from).collect())),
            Bound::Unbounded => Bound::Unbounded,
        };
        OptionVecRangeQueryValue::Ref((first, second))
    }
}

impl<T: PartialEq + EmbeddedDescription + Clone + 'static> SolveSimpleQueryValue for T {
    fn new(self) -> SRes<SimpleQueryValue> {
        Ok(SimpleQueryValue::Embedded(EmbValue::new_eq(self)))
    }
}
impl<T: EmbeddedDescription + PartialOrd + Clone + 'static> SolveRangeQueryValue for T {
    fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue {
        let first = match val.0 {
            Bound::Included(v) => Bound::Included(EmbValue::new_ord((*v).clone())),
            Bound::Excluded(v) => Bound::Excluded(EmbValue::new_ord((*v).clone())),
            Bound::Unbounded => Bound::Unbounded,
        };
        let second = match val.1 {
            Bound::Included(v) => Bound::Included(EmbValue::new_ord((*v).clone())),
            Bound::Excluded(v) => Bound::Excluded(EmbValue::new_ord((*v).clone())),
            Bound::Unbounded => Bound::Unbounded,
        };
        RangeQueryValue::Embedded((first, second))
    }
    fn range_vec(val: (&Bound<&Vec<Self>>, &Bound<&Vec<Self>>)) -> VecRangeQueryValue
    where
        Self: Sized,
    {
        let first = match val.0 {
            Bound::Included(v) => Bound::Included(v.iter().map(|v| EmbValue::new_ord(v.clone())).collect()),
            Bound::Excluded(v) => Bound::Excluded(v.iter().map(|v| EmbValue::new_ord(v.clone())).collect()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let second = match val.1 {
            Bound::Included(v) => Bound::Included(v.iter().map(|v| EmbValue::new_ord(v.clone())).collect()),
            Bound::Excluded(v) => Bound::Excluded(v.iter().map(|v| EmbValue::new_ord(v.clone())).collect()),
            Bound::Unbounded => Bound::Unbounded,
        };
        VecRangeQueryValue::Embedded((first, second))
    }
    fn range_option(val: (&Bound<&Option<Self>>, &Bound<&Option<Self>>)) -> OptionRangeQueryValue
    where
        Self: Sized,
    {
        let first = match val.0 {
            Bound::Included(v) => Bound::Included(v.as_ref().map(|v| EmbValue::new_ord(v.clone()))),
            Bound::Excluded(v) => Bound::Excluded(v.as_ref().map(|v| EmbValue::new_ord(v.clone()))),
            Bound::Unbounded => Bound::Unbounded,
        };
        let second = match val.1 {
            Bound::Included(v) => Bound::Included(v.as_ref().map(|v| EmbValue::new_ord(v.clone()))),
            Bound::Excluded(v) => Bound::Excluded(v.as_ref().map(|v| EmbValue::new_ord(v.clone()))),
            Bound::Unbounded => Bound::Unbounded,
        };
        OptionRangeQueryValue::Embedded((first, second))
    }
    fn range_option_vec(val: (&Bound<&Option<Vec<Self>>>, &Bound<&Option<Vec<Self>>>)) -> OptionVecRangeQueryValue
    where
        Self: Sized,
    {
        let first = match val.0 {
            Bound::Included(v) => Bound::Included(
                v.as_ref()
                    .map(|x| x.iter().map(|v| EmbValue::new_ord(v.clone())).collect()),
            ),
            Bound::Excluded(v) => Bound::Excluded(
                v.as_ref()
                    .map(|x| x.iter().map(|v| EmbValue::new_ord(v.clone())).collect()),
            ),
            Bound::Unbounded => Bound::Unbounded,
        };
        let second = match val.1 {
            Bound::Included(v) => Bound::Included(
                v.as_ref()
                    .map(|x| x.iter().map(|v| EmbValue::new_ord(v.clone())).collect()),
            ),
            Bound::Excluded(v) => Bound::Excluded(
                v.as_ref()
                    .map(|x| x.iter().map(|v| EmbValue::new_ord(v.clone())).collect()),
            ),
            Bound::Unbounded => Bound::Unbounded,
        };
        OptionVecRangeQueryValue::Embedded((first, second))
    }
}

pub trait SolveQueryValue {
    fn new(self) -> SRes<QueryValue>;
}
pub trait SolveQueryRange {
    fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue;
}

impl<T: SolveSimpleQueryValue> SolveQueryValue for T {
    fn new(self) -> SRes<QueryValue> {
        Ok(QueryValue::Single(self.new()?))
    }
}
impl<T: SolveRangeQueryValue> SolveQueryRange for T {
    fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue {
        SolveRangeQueryValue::range(val)
    }
}

impl<T: SolveSimpleQueryValue> SolveQueryValue for Option<T> {
    fn new(self) -> SRes<QueryValue> {
        Ok(QueryValue::Option(self.map(|v| v.new()).transpose()?))
    }
}
impl<T: SolveRangeQueryValue> SolveQueryRange for Option<T> {
    fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue {
        RangeQueryValue::Option(SolveRangeQueryValue::range_option(val))
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
impl<T: SolveRangeQueryValue> SolveQueryRange for Option<Vec<T>> {
    fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue {
        RangeQueryValue::OptionVec(SolveRangeQueryValue::range_option_vec(val))
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
impl<T: SolveRangeQueryValue> SolveQueryRange for Vec<T> {
    fn range(val: (&Bound<&Self>, &Bound<&Self>)) -> RangeQueryValue {
        RangeQueryValue::Vec(SolveRangeQueryValue::range_vec(val))
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

    pub(crate) fn add_field_range<T: SolveQueryRange + Clone>(
        &mut self,
        field: Rc<dyn FieldInfo>,
        range: (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::Range(SolveQueryRange::range(range)),
        }))
    }

    pub(crate) fn add_field_range_is<T: SolveQueryRange + Clone>(
        &mut self,
        field: Rc<dyn FieldInfo>,
        range: (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::RangeIs(SolveQueryRange::range(range)),
        }))
    }

    pub(crate) fn add_field_range_contains<T: SolveQueryRange + Clone>(
        &mut self,
        field: Rc<dyn FieldInfo>,
        range: (&Bound<&T>, &Bound<&T>),
    ) {
        self.filters.push(FilterItem::Field(FilterFieldItem {
            field,
            filter_type: FilterType::RangeContains(SolveQueryRange::range(range)),
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
    Range(RangeQueryValue),
    RangeContains(RangeQueryValue),
    RangeIs(RangeQueryValue),
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
    pub(crate) fn new(filter: FilterHolder, orders: Vec<Orders>) -> Self {
        OrdersFilters { orders, filter }
    }
}

#[derive(Debug)]
pub(crate) struct Query {
    pub(crate) type_name: String,
    pub(crate) projections: Vec<Projection>,
    pub(crate) orders_filter: OrdersFilters,
}

impl Query {
    pub fn new(type_name: &str, filter: FilterHolder, orders: Vec<Orders>, projections: Vec<Projection>) -> Self {
        Self {
            type_name: type_name.to_owned(),
            orders_filter: OrdersFilters::new(filter, orders),
            projections,
        }
    }
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
