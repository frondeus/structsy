use crate::{
    filter_builder::{
        plan_model::QueryValuePlan,
        query_model::{RawRef, SimpleQueryValue},
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
pub trait ValueRange: ValueCompare {
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering>;
    fn range(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_contains(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn range_is(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool;
    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering;
}

impl<T: ValueCompare> ValueCompare for Option<T> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match (value, self) {
            (QueryValuePlan::Option(Some(v)), Some(ov)) => ov.equals(QueryValuePlan::Single(v)),
            (QueryValuePlan::Option(None), None) => true,
            (QueryValuePlan::Option(Some(_)), None) => false,
            (QueryValuePlan::Option(None), Some(_)) => false,
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
        if let Some(v) = self {
            v.equals(value)
        } else {
            false
        }
    }
}

impl<T: ValueCompare> ValueCompare for Vec<T> {
    fn equals(&self, value: QueryValuePlan) -> bool {
        match value {
            QueryValuePlan::Array(v) => {
                let mut fi = v.iter().peekable();
                let mut si = self.iter().peekable();
                while let (Some(first), Some(second)) = (fi.peek(), si.peek()) {
                    if !second.equals(QueryValuePlan::Single((*first).clone())) {
                        return false;
                    }
                }
                fi.next();
                si.next();
                fi.peek().is_none() && si.peek().is_none()
            }
            _ => {
                debug_assert!(false, "should never match a wrong type");
                false
            }
        }
    }
    fn contains_value(&self, value: QueryValuePlan) -> bool {
        for x in self {
            if x.equals(value.clone()) {
                return true;
            }
        }
        false
    }
    fn is(&self, _value: QueryValuePlan) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
}

impl<T: ValueRange> ValueRange for Option<T> {
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
        match (self, value) {
            (Some(v), QueryValuePlan::Option(Some(v1))) => v.compare(QueryValuePlan::Single(v1)),
            (Some(_v), QueryValuePlan::Option(None)) => Some(Ordering::Greater),
            (None, QueryValuePlan::Option(Some(_v1))) => Some(Ordering::Less),
            (None, QueryValuePlan::Option(None)) => Some(Ordering::Equal),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return None;
            }
        }
    }
    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        if let Some(s) = self {
            let rv = match value {
                Bound::Included(QueryValuePlan::Option(Some(v))) => Bound::Included(QueryValuePlan::Single(v)),
                Bound::Included(QueryValuePlan::Option(None)) => Bound::Unbounded,
                Bound::Excluded(QueryValuePlan::Option(Some(v))) => Bound::Excluded(QueryValuePlan::Single(v)),
                Bound::Excluded(QueryValuePlan::Option(None)) => Bound::Unbounded,
                Bound::Unbounded => Bound::Unbounded,
                _ => {
                    debug_assert!(false, "should never match a wrong type");
                    return false;
                }
            };
            let lv = match value1 {
                Bound::Included(QueryValuePlan::Option(Some(v))) => Bound::Included(QueryValuePlan::Single(v)),
                Bound::Included(QueryValuePlan::Option(None)) => Bound::Unbounded,
                Bound::Excluded(QueryValuePlan::Option(Some(v))) => Bound::Excluded(QueryValuePlan::Single(v)),
                Bound::Excluded(QueryValuePlan::Option(None)) => Bound::Unbounded,
                Bound::Unbounded => Bound::Unbounded,
                _ => {
                    debug_assert!(false, "should never match a wrong type");
                    return false;
                }
            };
            s.range_contains((rv, lv))
        } else {
            (match value {
                Bound::Included(QueryValuePlan::Option(Some(_))) => false,
                Bound::Included(QueryValuePlan::Option(None)) => true,
                Bound::Excluded(QueryValuePlan::Option(Some(_))) => false,
                Bound::Excluded(QueryValuePlan::Option(None)) => false,
                Bound::Unbounded => true,
                _ => {
                    debug_assert!(false, "should never match a wrong type");
                    return false;
                }
            }) && (match value1 {
                Bound::Included(QueryValuePlan::Option(Some(_))) => true,
                Bound::Included(QueryValuePlan::Option(None)) => true,
                Bound::Excluded(QueryValuePlan::Option(Some(_))) => true,
                Bound::Excluded(QueryValuePlan::Option(None)) => false,
                Bound::Unbounded => true,
                _ => {
                    debug_assert!(false, "should never match a wrong type");
                    return false;
                }
            })
        }
    }

    fn range_contains(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, r: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        if let Some(v) = self {
            v.range_is(r)
        } else {
            false
        }
    }
    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Some(v), Some(v1)) => v.sort_compare(v1),
            (None, None) => std::cmp::Ordering::Equal,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
        }
    }
}

impl<T: ValueRange> ValueRange for Vec<T> {
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
        let left = self;
        let right = match value {
            QueryValuePlan::Array(a) => a,
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return None;
            }
        };
        let l = min(right.len(), left.len());

        // Slice to the loop iteration range to enable bound check
        // elimination in the compiler
        let lhs = &left[..l];
        let rhs = &right[..l];

        for i in 0..l {
            match lhs[i].compare(QueryValuePlan::Single(rhs[i].clone())) {
                Some(Ordering::Equal) => (),
                non_eq => return non_eq,
            }
        }

        left.len().partial_cmp(&right.len())
    }

    fn range(&self, (value, value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        (match value {
            // TODO: double check this logic
            Bound::Included(start) => self.compare(start) == Some(Ordering::Less),
            Bound::Excluded(start) => self.compare(start) != Some(Ordering::Greater),
            Bound::Unbounded => true,
        }) && (match value1 {
            Bound::Included(end) => self.compare(end) != Some(Ordering::Greater),
            Bound::Excluded(end) => self.compare(end) == Some(Ordering::Less),
            Bound::Unbounded => true,
        })
    }
    fn range_contains(&self, value: (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        for el in self {
            if el.range(value.clone()) {
                return true;
            }
        }
        false
    }
    fn range_is(&self, (_value, _value1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>)) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
        let s = self.len().cmp(&other.len());
        if s == std::cmp::Ordering::Equal {
            for c in 0..self.len() {
                let vc = self[c].sort_compare(&other[c]);
                if vc != std::cmp::Ordering::Equal {
                    return vc;
                }
            }
            std::cmp::Ordering::Equal
        } else {
            s
        }
    }
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
            fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
                match value {
                    QueryValuePlan::Single(SimpleQueryValue::$v(v)) => v.partial_cmp(self),
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return None;
                    }
                }
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

            fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
                use std::cmp::PartialOrd;
                self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Less)
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
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Ref(v)) => v.partial_cmp(&RawRef::from(self)),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return None;
            }
        }
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

    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Less)
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
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Embedded(v)) => v.partial_cmp(self as &dyn MyOrd),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return None;
            }
        }
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
    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Less)
    }
}
