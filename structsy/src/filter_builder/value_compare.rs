use crate::{
    filter_builder::{
        plan_model::QueryValuePlan,
        query_model::{EmbValue, RawRef, SimpleQueryValue},
    },
    internal::EmbeddedDescription,
    Ref,
};
use std::cmp::{min, Ordering};
use std::ops::{Bound, RangeBounds};

use super::query_model::{MyEq, MyOrd};

pub trait ValueCompare {
    fn equals(&self, value: QueryValuePlan) -> bool;
    fn contains_value(&self, value: QueryValuePlan) -> bool;
    fn is(&self, value: QueryValuePlan) -> bool;
}
pub(crate) trait ValueRange: ValueCompare {
    fn range(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_contains(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_is(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
}

macro_rules! impl_value_compare {
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
        }
        impl ValueRange for $t {
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
        }
        impl ValueRange for Vec<$t> {
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
        }
        impl ValueRange for Option<$t> {
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

impl_value_compare!(u8, U8);
impl_value_compare!(u16, U16);
impl_value_compare!(u32, U32);
impl_value_compare!(u64, U64);
impl_value_compare!(u128, U128);
impl_value_compare!(i8, I8);
impl_value_compare!(i16, I16);
impl_value_compare!(i32, I32);
impl_value_compare!(i64, I64);
impl_value_compare!(i128, I128);
impl_value_compare!(f32, F32);
impl_value_compare!(f64, F64);
impl_value_compare!(bool, Bool);
impl_value_compare!(String, String);

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
}
impl<T> ValueRange for Ref<T> {
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
}
impl<T> ValueRange for Vec<Ref<T>> {
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
}
impl<T> ValueRange for Option<Ref<T>> {
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

impl<T: EmbeddedDescription + PartialEq + 'static> ValueCompare for T {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Embedded(v)) => v.eq(self as &dyn MyEq),
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
}
impl<T: EmbeddedDescription + PartialOrd + 'static> ValueRange for T {
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

impl<T: EmbeddedDescription + PartialEq + 'static> ValueCompare for Vec<T> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Array(v) => {
                let mut fi = v.iter().peekable();
                let mut si = self.iter().peekable();
                while let (Some(first), Some(second)) = (fi.peek(), si.peek()) {
                    match first {
                        SimpleQueryValue::Embedded(v) => {
                            if !v.eq(second as &T as &dyn MyEq) {
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
                    if v.eq(x as &dyn MyEq) {
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
}
impl<T: EmbeddedDescription + PartialOrd + 'static> ValueRange for Vec<T> {
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

        fn inline_cmp<X: EmbeddedDescription + PartialOrd + 'static>(
            left: &Vec<X>,
            right: &Vec<SimpleQueryValue>,
        ) -> Option<Ordering> {
            let l = min(right.len(), left.len());

            // Slice to the loop iteration range to enable bound check
            // elimination in the compiler
            let lhs = &left[..l];
            let rhs = &right[..l];

            for i in 0..l {
                if let SimpleQueryValue::Embedded(v) = &rhs[i] {
                    match (&lhs[i] as &X as &dyn MyOrd).partial_cmp(v) {
                        Some(Ordering::Equal) => (),
                        non_eq => return non_eq,
                    }
                }
            }

            left.len().partial_cmp(&right.len())
        }

        (match lv {
            // TODO: double check this logic
            Bound::Included(start) => inline_cmp(self, &start) == Some(Ordering::Less),
            Bound::Excluded(start) => inline_cmp(self, &start) != Some(Ordering::Greater),
            Bound::Unbounded => true,
        }) && (match rv {
            Bound::Included(end) => inline_cmp(self, &end) != Some(Ordering::Greater),
            Bound::Excluded(end) => inline_cmp(self, &end) == Some(Ordering::Less),
            Bound::Unbounded => true,
        })
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

impl<T: EmbeddedDescription + PartialEq + 'static> ValueCompare for Option<T> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v))) => {
                if let Some(sv) = self {
                    v.eq(sv as &T as &dyn MyEq)
                } else {
                    false
                }
            }
            QueryValuePlan::Option(None) => self.is_none(),
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
            QueryValuePlan::Single(SimpleQueryValue::Embedded(v)) => {
                if let Some(sv) = self {
                    v.eq(sv as &T as &dyn MyEq)
                } else {
                    false
                }
            }
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
}

impl<T: EmbeddedDescription + PartialOrd + 'static> ValueRange for Option<T> {
    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v)))) => Bound::Included(Some(v)),
            Bound::Included(QueryValuePlan::Option(None)) => Bound::Included(None),
            Bound::Excluded(QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v)))) => Bound::Excluded(Some(v)),
            Bound::Excluded(QueryValuePlan::Option(None)) => Bound::Excluded(None),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v)))) => Bound::Included(Some(v)),
            Bound::Included(QueryValuePlan::Option(None)) => Bound::Included(None),
            Bound::Excluded(QueryValuePlan::Option(Some(SimpleQueryValue::Embedded(v)))) => Bound::Excluded(Some(v)),
            Bound::Excluded(QueryValuePlan::Option(None)) => Bound::Excluded(None),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(&self.as_ref().map(|sv| EmbValue::new_ref(sv)))
    }

    fn range_contains(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        let rv = match value {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Included(Some(v)),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Excluded(Some(v)),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        let lv = match value1 {
            Bound::Included(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Included(Some(v)),
            Bound::Excluded(QueryValuePlan::Single(SimpleQueryValue::Embedded(v))) => Bound::Excluded(Some(v)),
            Bound::Unbounded => Bound::Unbounded,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return false;
            }
        };
        (rv, lv).contains(&self.as_ref().map(|sv| EmbValue::new_ref(sv)))
    }
}
