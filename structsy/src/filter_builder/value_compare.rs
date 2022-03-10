use crate::{
    filter_builder::{
        plan_model::QueryValuePlan,
        query_model::{EmbValue, RawRef, SimpleQueryValue},
    },
    internal::EmbeddedDescription,
    PersistentEmbedded, Ref,
};
use std::ops::{Bound, RangeBounds};

use super::query_model::MyOrd;

pub(crate) trait ValueCompare {
    fn equals(&self, value: QueryValuePlan) -> bool;
    fn contains_value(&self, value: QueryValuePlan) -> bool;
    fn is(&self, value: QueryValuePlan) -> bool;
    fn range(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_contains(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_is(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
}

macro_rules! impl_field_type {
    ($t:ident,$v:ident) => {
        impl ValueCompare for $t {
            fn equals(&self, value: QueryValuePlan) -> bool {
                match value {
                    QueryValuePlan::Single(SimpleQueryValue::$v(v)) => self.eq(&v),
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        false
                    }
                }
            }
            fn contains_value(&self, _value: QueryValuePlan) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
            fn is(&self, _value: QueryValuePlan) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
            fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                let rv = match value {
                    Bound::Included(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Included(v),
                    Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Excluded(v),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                let lv = match value1 {
                    Bound::Included(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Included(v),
                    Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Excluded(v),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                (rv, lv).contains(self)
            }
            fn range_contains(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
            fn range_is(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
        }
        impl ValueCompare for Vec<$t> {
            fn equals(&self, value: QueryValuePlan) -> bool {
                match value {
                    QueryValuePlan::Array(v) => self
                        .iter()
                        .map(|x| SimpleQueryValue::$v(x.clone()))
                        .collect::<Vec<_>>()
                        .eq(&v),
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        false
                    }
                }
            }
            fn contains_value(&self, value: QueryValuePlan) -> bool {
                match value {
                    QueryValuePlan::Single(SimpleQueryValue::$v(v)) => self.contains(&v),
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        false
                    }
                }
            }
            fn is(&self, _value: QueryValuePlan) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
            fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                let rv = match value {
                    Bound::Included(QueryValuePlan::Array(v)) => Bound::Included(v),
                    Bound::Excluded(QueryValuePlan::Array(v)) => Bound::Excluded(v),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                let lv = match value1 {
                    Bound::Included(QueryValuePlan::Array(v)) => Bound::Included(v),
                    Bound::Excluded(QueryValuePlan::Array(v)) => Bound::Excluded(v),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                (rv, lv).contains(
                    &self
                        .iter()
                        .map(|x| SimpleQueryValue::$v(x.clone()))
                        .collect::<Vec<_>>(),
                )
            }
            fn range_contains(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                let rv = match value {
                    Bound::Included(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Included(v),
                    Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Excluded(v),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                let lv = match value1 {
                    Bound::Included(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Included(v),
                    Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Excluded(v),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                for el in self {
                    if (rv.clone(), lv.clone()).contains(el) {
                        return true;
                    }
                }
                false
            }
            fn range_is(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
        }

        impl ValueCompare for Option<$t> {
            fn equals(&self, value: QueryValuePlan) -> bool {
                match value {
                    QueryValuePlan::Option(v) => self.clone().map(|x| SimpleQueryValue::$v(x)).eq(&v),
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        false
                    }
                }
            }
            fn contains_value(&self, _value: QueryValuePlan) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
            fn is(&self, value: QueryValuePlan) -> bool {
                match value {
                    QueryValuePlan::Single(v) => self.clone().map(|x| SimpleQueryValue::$v(x)).eq(&Some(v)),
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        false
                    }
                }
            }
            fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                let rv = match value {
                    Bound::Included(QueryValuePlan::Option(Some(SimpleQueryValue::$v(v)))) => Bound::Included(Some(v)),
                    Bound::Included(QueryValuePlan::Option(None)) => Bound::Included(None),
                    Bound::Excluded(QueryValuePlan::Option(Some(SimpleQueryValue::$v(v)))) => Bound::Excluded(Some(v)),
                    Bound::Excluded(QueryValuePlan::Option(None)) => Bound::Excluded(None),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                let lv = match value1 {
                    Bound::Included(QueryValuePlan::Option(Some(SimpleQueryValue::$v(v)))) => Bound::Included(Some(v)),
                    Bound::Included(QueryValuePlan::Option(None)) => Bound::Included(None),
                    Bound::Excluded(QueryValuePlan::Option(Some(SimpleQueryValue::$v(v)))) => Bound::Excluded(Some(v)),
                    Bound::Excluded(QueryValuePlan::Option(None)) => Bound::Excluded(None),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                (rv, lv).contains(self)
            }

            fn range_contains(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
            fn range_is(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
                let rv = match value {
                    Bound::Included(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Included(Some(v)),
                    Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Excluded(Some(v)),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                let lv = match value1 {
                    Bound::Included(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Included(Some(v)),
                    Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::$v(v))) => Bound::Excluded(Some(v)),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return false;
                    }
                };
                (rv, lv).contains(self)
            }
        }
    };
}

impl_field_type!(u8, U8);
impl_field_type!(u16, U16);
impl_field_type!(u32, U32);
impl_field_type!(u64, U64);
impl_field_type!(u128, U128);
impl_field_type!(i8, I8);
impl_field_type!(i16, I16);
impl_field_type!(i32, I32);
impl_field_type!(i64, I64);
impl_field_type!(i128, I128);
impl_field_type!(f32, F32);
impl_field_type!(f64, F64);
impl_field_type!(bool, Bool);
impl_field_type!(String, String);

impl<T> ValueCompare for Ref<T> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Ref(v)) => self.raw_id.eq(&v.id),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn contains_value(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn is(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(&RawRef::from(self))
    }
    fn range_contains(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
}
impl<T> ValueCompare for Vec<Ref<T>> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Array(v) => self
                .iter()
                .map(|x| SimpleQueryValue::Ref(RawRef::from(x)))
                .collect::<Vec<_>>()
                .eq(&v),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn contains_value(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Ref(v)) => {
                self.iter().map(|x| RawRef::from(x)).collect::<Vec<_>>().contains(&v)
            }
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn is(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Array(v)) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Array(v)) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Array(v)) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Array(v)) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(
            &self
                .iter()
                .map(|x| SimpleQueryValue::Ref(RawRef::from(x)))
                .collect::<Vec<_>>(),
        )
    }
    fn range_contains(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        for el in self {
            if (rv.clone(), lv.clone()).contains(&RawRef::from(el)) {
                return true;
            }
        }
        false
    }
    fn range_is(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
}

impl<T> ValueCompare for Option<Ref<T>> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Option(v) => self.clone().map(|x| SimpleQueryValue::Ref(RawRef::from(&x))).eq(&v),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn contains_value(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn is(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Single(v) => self
                .clone()
                .map(|x| SimpleQueryValue::Ref(RawRef::from(&x)))
                .eq(&Some(v)),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Option(Some(SimpleQueryValue::Ref(v)))) => Bound::Included(Some(v)),
            Bound::Included(QueryValuePlan::Option(None)) => Bound::Included(None),
            Bound::Excluded(QueryValuePlan::Option(Some(SimpleQueryValue::Ref(v)))) => Bound::Excluded(Some(v)),
            Bound::Excluded(QueryValuePlan::Option(None)) => Bound::Excluded(None),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Option(Some(SimpleQueryValue::Ref(v)))) => Bound::Included(Some(v)),
            Bound::Included(QueryValuePlan::Option(None)) => Bound::Included(None),
            Bound::Excluded(QueryValuePlan::Option(Some(SimpleQueryValue::Ref(v)))) => Bound::Excluded(Some(v)),
            Bound::Excluded(QueryValuePlan::Option(None)) => Bound::Excluded(None),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(&self.clone().map(|x| RawRef::from(&x)))
    }

    fn range_contains(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Included(Some(v)),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Excluded(Some(v)),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Included(Some(v)),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Ref(v))) => Bound::Excluded(Some(v)),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(&self.clone().map(|x| RawRef::from(&x)))
    }
}

impl<T: EmbeddedDescription + PartialOrd + 'static> ValueCompare for T {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Embedded(v)) => v.eq(self as &dyn MyOrd),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn contains_value(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn is(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(self as &dyn MyOrd)
    }
    fn range_contains(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
}

impl<T: EmbeddedDescription + Ord + 'static> ValueCompare for Vec<T> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Array(v) => {
                let mut fi = v.iter().peekable();
                let mut si = self.iter().peekable();
                while let (Some(first), Some(second)) = (fi.peek(), si.peek()) {
                    match first {
                        SimpleQueryValue::Embedded(v) => {
                            if !v.eq(second as &T as &dyn MyOrd) {
                                return false;
                            }
                        }
                        _ => {
                            debug_assert!(false, "should never match a wrong type");
                            return false;
                        }
                    }
                    fi.next();
                    si.next();
                }
                fi.peek().is_none() && si.peek().is_none()
            }
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn contains_value(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Embedded(v)) => {
                for x in self {
                    if v.eq(x as &dyn MyOrd) {
                        return true;
                    }
                }
                false
            }
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn is(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Array(v)) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Array(v)) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Array(v)) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Array(v)) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        todo!()
        //(rv, lv).contains(self)
    }
    fn range_contains(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Included(v),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        for el in self {
            if (rv.clone(), lv.clone()).contains(el as &dyn MyOrd) {
                return true;
            }
        }
        false
    }
    fn range_is(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
}
/*

impl<T:EmbeddedDescription+ Ord> ValueCompare for Option<T> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Option(v) => self.clone().map(|x|SimpleQueryValue::Embedded(x)).eq(&v),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn contains_value(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn is(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Single(v) => self.clone().map(|x|SimpleQueryValue::Embedded(x)).eq(&Some(v)),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v)))) =>Bound::Included(Some(v)),
            Bound::Included(QueryValuePlan::Option(None)) =>Bound::Included(None),
            Bound::Excluded(QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v)))) =>Bound::Excluded(Some(v)),
            Bound::Excluded(QueryValuePlan::Option(None)) =>Bound::Excluded(None),
            Bound::Unbounded =>Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v)))) =>Bound::Included(Some(v)),
            Bound::Included(QueryValuePlan::Option(None)) =>Bound::Included(None),
            Bound::Excluded(QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v)))) =>Bound::Excluded(Some(v)),
            Bound::Excluded(QueryValuePlan::Option(None)) =>Bound::Excluded(None),
            Bound::Unbounded =>Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(self)
    }

    fn range_contains(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) =>Bound::Included(Some(v)),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) =>Bound::Excluded(Some(v)),
            Bound::Unbounded =>Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) =>Bound::Included(Some(v)),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) =>Bound::Excluded(Some(v)),
            Bound::Unbounded =>Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(self)
    }
}
*/
