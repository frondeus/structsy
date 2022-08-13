use crate::{
    filter_builder::{
        plan_model::QueryValuePlan,
        query_model::{
            EmbValue, OptionRangeQueryValue, OptionVecRangeQueryValue, RangeQueryValue, RawRef, SimpleQueryValue,
            VecRangeQueryValue,
        },
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
    type RangeType;
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering>;
    fn range(&self, value: RangeQueryValue) -> bool;
    fn range_vec(&self, _value: VecRangeQueryValue) -> bool {
        false
    }
    fn range_contains(&self, value: RangeQueryValue) -> bool;
    fn range_is(&self, value: RangeQueryValue) -> bool;
    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering;
    fn extract_range_vec(_value: VecRangeQueryValue) -> (Bound<Vec<Self::RangeType>>, Bound<Vec<Self::RangeType>>)
    where
        Self: Sized,
    {
        unreachable!()
    }
    fn extract_range_option(
        _value: OptionRangeQueryValue,
    ) -> (Bound<Option<Self::RangeType>>, Bound<Option<Self::RangeType>>)
    where
        Self: Sized,
    {
        unreachable!()
    }
    fn extract_range_option_vec(_value: OptionVecRangeQueryValue) -> (VecRangeQueryValue, bool)
    where
        Self: Sized,
    {
        unreachable!()
    }
    fn map_type(&self) -> Self::RangeType;
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
                if self.len() == v.len() {
                    let mut fi = v.iter();
                    let mut si = self.iter();
                    while let (Some(first), Some(second)) = (fi.next(), si.next()) {
                        if !second.equals(QueryValuePlan::Single((*first).clone())) {
                            return false;
                        }
                    }
                    true
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

impl<T: ValueRange + PartialOrd> ValueRange for Option<T>
where
    <T as ValueRange>::RangeType: PartialOrd,
{
    type RangeType = Option<<T as ValueRange>::RangeType>;
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
        match (self, value) {
            (Some(v), QueryValuePlan::Option(Some(v1))) => v.compare(QueryValuePlan::Single(v1)),
            (Some(_v), QueryValuePlan::Option(None)) => Some(Ordering::Less),
            (None, QueryValuePlan::Option(Some(_v1))) => Some(Ordering::Greater),
            (None, QueryValuePlan::Option(None)) => Some(Ordering::Equal),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return None;
            }
        }
    }
    fn range(&self, value: RangeQueryValue) -> bool {
        match value {
            RangeQueryValue::Option(vr) => {
                let r = T::extract_range_option(vr);
                if self.is_some() {
                    let nb0 = match r.0 {
                        Bound::Included(Some(v)) => Bound::Included(Some(v)),
                        Bound::Included(None) => Bound::Unbounded,
                        Bound::Excluded(Some(v)) => Bound::Excluded(Some(v)),
                        Bound::Excluded(None) => Bound::Unbounded,
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    let nb1 = match r.1 {
                        Bound::Included(Some(v)) => Bound::Included(Some(v)),
                        Bound::Included(None) => Bound::Unbounded,
                        Bound::Excluded(Some(v)) => Bound::Excluded(Some(v)),
                        Bound::Excluded(None) => Bound::Unbounded,
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    (nb0, nb1).contains(&self.map_type())
                } else {
                    match r {
                        (Bound::Included(Some(_)), Bound::Included(Some(_))) => false,
                        (Bound::Excluded(Some(_)), Bound::Excluded(Some(_))) => false,
                        _ => true,
                    }
                }
            }
            RangeQueryValue::OptionVec(vr) => {
                let (r, is_none) = T::extract_range_option_vec(vr);
                if let Some(v) = self {
                    T::range_vec(&v, r)
                } else {
                    is_none
                }
            }
            _ => false,
        }
    }

    fn range_contains(&self, _value: RangeQueryValue) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, r: RangeQueryValue) -> bool {
        if let Some(v) = self {
            v.range(r)
        } else {
            true
        }
    }
    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Some(v), Some(v1)) => v.sort_compare(v1),
            (None, None) => std::cmp::Ordering::Equal,
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
        }
    }

    fn map_type(&self) -> Self::RangeType {
        self.as_ref().map(|x| x.map_type())
    }
}

impl<T: ValueRange> ValueRange for Vec<T>
where
    <T as ValueRange>::RangeType: PartialOrd,
{
    type RangeType = Vec<<T as ValueRange>::RangeType>;
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

    fn range(&self, value: RangeQueryValue) -> bool {
        match value {
            RangeQueryValue::Vec(vr) => T::extract_range_vec(vr).contains(&self.map_type()),
            _ => false,
        }
    }
    fn range_vec(&self, value: VecRangeQueryValue) -> bool {
        T::extract_range_vec(value).contains(&self.map_type())
    }
    fn range_contains(&self, value: RangeQueryValue) -> bool {
        for el in self {
            if el.range(value.clone()) {
                return true;
            }
        }
        false
    }
    fn range_is(&self, _value: RangeQueryValue) -> bool {
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

    fn map_type(&self) -> Self::RangeType {
        self.iter().map(|x| x.map_type()).collect()
    }
    fn extract_range_option_vec(value: OptionVecRangeQueryValue) -> (VecRangeQueryValue, bool)
    where
        Self: Sized,
    {
        T::extract_range_option_vec(value)
    }
}

fn map_option_vec<T>(
    (b0, b1): (Bound<Option<Vec<T>>>, Bound<Option<Vec<T>>>),
) -> ((Bound<Vec<T>>, Bound<Vec<T>>), bool) {
    let is_none = match (&b0, &b0) {
        (Bound::Included(Some(_)), Bound::Included(Some(_))) => false,
        (Bound::Excluded(Some(_)), Bound::Excluded(Some(_))) => false,
        _ => true,
    };

    let nb0 = match b0 {
        Bound::Included(Some(v)) => Bound::Included(v),
        Bound::Included(None) => Bound::Unbounded,
        Bound::Excluded(Some(v)) => Bound::Excluded(v),
        Bound::Excluded(None) => Bound::Unbounded,
        Bound::Unbounded => Bound::Unbounded,
    };
    let nb1 = match b1 {
        Bound::Included(Some(v)) => Bound::Included(v),
        Bound::Included(None) => Bound::Unbounded,
        Bound::Excluded(Some(v)) => Bound::Excluded(v),
        Bound::Excluded(None) => Bound::Unbounded,
        Bound::Unbounded => Bound::Unbounded,
    };
    ((nb0, nb1), is_none)
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
            type RangeType = $t;
            fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
                match value {
                    QueryValuePlan::Single(SimpleQueryValue::$v(v)) => self.partial_cmp(&v),
                    _ => {
                        debug_assert!(false, "should never match a wrong type");
                        return None;
                    }
                }
            }
            fn range(&self, value: RangeQueryValue) -> bool {
                match value {
                    RangeQueryValue::$v(r) => r.contains(self),
                    _ => false,
                }
            }
            fn range_contains(&self, _value: RangeQueryValue) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }
            fn range_is(&self, _value: RangeQueryValue) -> bool {
                debug_assert!(false, "should never call wrong action");
                false
            }

            fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
                use std::cmp::PartialOrd;
                self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Less)
            }

            fn extract_range_vec(
                value: VecRangeQueryValue,
            ) -> (Bound<Vec<Self::RangeType>>, Bound<Vec<Self::RangeType>>)
            where
                Self: Sized,
            {
                match value {
                    VecRangeQueryValue::$v(r) => r,
                    _ => unreachable!(),
                }
            }
            fn extract_range_option(
                value: OptionRangeQueryValue,
            ) -> (Bound<Option<Self::RangeType>>, Bound<Option<Self::RangeType>>)
            where
                Self: Sized,
            {
                match value {
                    OptionRangeQueryValue::$v(r) => r,
                    _ => unreachable!(),
                }
            }
            fn extract_range_option_vec(value: OptionVecRangeQueryValue) -> (VecRangeQueryValue, bool)
            where
                Self: Sized,
            {
                match value {
                    OptionVecRangeQueryValue::$v(r) => {
                        let (nb, is_none) = map_option_vec(r);
                        (VecRangeQueryValue::$v(nb), is_none)
                    }
                    _ => unreachable!(),
                }
            }
            fn map_type(&self) -> Self::RangeType {
                self.clone()
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
    type RangeType = RawRef;
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Ref(v)) => RawRef::from(self).partial_cmp(&v),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return None;
            }
        }
    }
    fn range(&self, value: RangeQueryValue) -> bool {
        match value {
            RangeQueryValue::Ref(r) => r.contains(&RawRef::from(self)),
            _ => false,
        }
    }
    fn range_contains(&self, _value: RangeQueryValue) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, _value: RangeQueryValue) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }

    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Less)
    }
    fn extract_range_vec(value: VecRangeQueryValue) -> (Bound<Vec<Self::RangeType>>, Bound<Vec<Self::RangeType>>)
    where
        Self: Sized,
    {
        match value {
            VecRangeQueryValue::Ref(v) => v,
            _ => unreachable!(),
        }
    }
    fn extract_range_option(
        value: OptionRangeQueryValue,
    ) -> (Bound<Option<Self::RangeType>>, Bound<Option<Self::RangeType>>)
    where
        Self: Sized,
    {
        match value {
            OptionRangeQueryValue::Ref(v) => v,
            _ => unreachable!(),
        }
    }
    fn extract_range_option_vec(value: OptionVecRangeQueryValue) -> (VecRangeQueryValue, bool)
    where
        Self: Sized,
    {
        match value {
            OptionVecRangeQueryValue::Ref(r) => {
                let (b, is_none) = map_option_vec(r);
                (VecRangeQueryValue::Ref(b), is_none)
            }
            _ => unreachable!(),
        }
    }
    fn map_type(&self) -> Self::RangeType {
        RawRef::from(self)
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
impl<T: EmbeddedDescription + PartialOrd + Clone + 'static> ValueRange for T {
    type RangeType = EmbValue<'static>;
    fn compare(&self, value: QueryValuePlan) -> Option<Ordering> {
        match value {
            QueryValuePlan::Single(SimpleQueryValue::Embedded(v)) => (self as &dyn MyOrd).partial_cmp(&v),
            _ => {
                debug_assert!(false, "should never match a wrong type");
                return None;
            }
        }
    }
    fn range(&self, value: RangeQueryValue) -> bool {
        match value {
            RangeQueryValue::Embedded(r) => r.contains(self as &dyn MyOrd),
            _ => false,
        }
    }
    fn range_contains(&self, _value: RangeQueryValue) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn range_is(&self, _value: RangeQueryValue) -> bool {
        debug_assert!(false, "should never call wrong action");
        false
    }
    fn sort_compare(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Less)
    }

    fn extract_range_vec(value: VecRangeQueryValue) -> (Bound<Vec<Self::RangeType>>, Bound<Vec<Self::RangeType>>)
    where
        Self: Sized,
    {
        match value {
            VecRangeQueryValue::Embedded(v) => v,
            _ => unreachable!(),
        }
    }
    fn extract_range_option(
        value: OptionRangeQueryValue,
    ) -> (Bound<Option<Self::RangeType>>, Bound<Option<Self::RangeType>>)
    where
        Self: Sized,
    {
        match value {
            OptionRangeQueryValue::Embedded(v) => v,
            _ => unreachable!(),
        }
    }
    fn extract_range_option_vec(value: OptionVecRangeQueryValue) -> (VecRangeQueryValue, bool)
    where
        Self: Sized,
    {
        match value {
            OptionVecRangeQueryValue::Embedded(r) => {
                let (b, is_none) = map_option_vec(r);
                (VecRangeQueryValue::Embedded(b), is_none)
            }
            _ => unreachable!(),
        }
    }
    fn map_type(&self) -> Self::RangeType {
        EmbValue::new_ord(self.clone())
    }
}
