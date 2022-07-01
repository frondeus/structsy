use super::{
    plan_model::{FieldPathPlan, IndexInfo, InfoFinder, QueryValuePlan},
    query_model::SimpleQueryValue,
    reader::{Reader, ReaderIterator},
};
use crate::{
    desc::{index_name, Description},
    format::PersistentEmbedded,
    index::RangeInstanceIter,
    Order, Persistent, Ref, SRes, Structsy,
};
use std::ops::Bound;

fn index_score(
    reader: Reader,
    index_name: &str,
    (bound_0, bound_1): (Bound<SimpleQueryValue>, Bound<SimpleQueryValue>),
) -> SRes<usize> {
    match bound_0 {
        Bound::Included(SimpleQueryValue::U8(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U8(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::U16(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U16(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },

        Bound::Included(SimpleQueryValue::U32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::U128(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U128(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::U64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I8(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I8(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I16(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I16(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I128(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I128(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::F32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => f32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::F32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => f32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::F64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => f64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::F64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => f64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::String(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::String(r1)) => {
                String::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::String(r1)) => {
                String::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => {
                String::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded)))
            }
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::String(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::String(r1)) => {
                String::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::String(r1)) => {
                String::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => {
                String::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded)))
            }
            _ => Ok(usize::MAX),
        },

        Bound::Unbounded => match bound_1 {
            Bound::Included(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::String(r1)) => {
                String::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Included(r1))))
            }
            Bound::Included(SimpleQueryValue::Bool(_)) => Ok(usize::MAX),
            Bound::Included(SimpleQueryValue::Ref(_)) => Ok(usize::MAX),
            Bound::Included(SimpleQueryValue::Embedded(_)) => Ok(usize::MAX),
            Bound::Excluded(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::String(r1)) => {
                String::finder().score(reader, index_name, Some((Bound::Unbounded, Bound::Excluded(r1))))
            }
            Bound::Excluded(SimpleQueryValue::Bool(_)) => Ok(usize::MAX),
            Bound::Excluded(SimpleQueryValue::Ref(_)) => Ok(usize::MAX),
            Bound::Excluded(SimpleQueryValue::Embedded(_)) => Ok(usize::MAX),

            Bound::Unbounded => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::Bool(_)) => Ok(usize::MAX),
        Bound::Included(SimpleQueryValue::Ref(_)) => Ok(usize::MAX),
        Bound::Included(SimpleQueryValue::Embedded(_)) => Ok(usize::MAX),
        Bound::Excluded(SimpleQueryValue::Bool(_)) => Ok(usize::MAX),
        Bound::Excluded(SimpleQueryValue::Ref(_)) => Ok(usize::MAX),
        Bound::Excluded(SimpleQueryValue::Embedded(_)) => Ok(usize::MAX),
    }
}

pub(crate) fn index_find_range<'a, P: Persistent + 'static>(
    reader: Reader<'a>,
    index_name: &str,
    (b_0, b_1): (Bound<QueryValuePlan>, Bound<QueryValuePlan>),
    order: Order,
) -> SRes<Box<dyn ReaderIterator<Item = (Ref<P>, P)> + 'a>> {
    let bound_0 = match b_0 {
        Bound::Excluded(QueryValuePlan::Single(v)) => Bound::Excluded(v),
        Bound::Included(QueryValuePlan::Single(v)) => Bound::Included(v),
        Bound::Unbounded => Bound::Unbounded,
        _ => todo!(),
    };
    let bound_1 = match b_1 {
        Bound::Excluded(QueryValuePlan::Single(v)) => Bound::Excluded(v),
        Bound::Included(QueryValuePlan::Single(v)) => Bound::Included(v),
        Bound::Unbounded => Bound::Unbounded,
        _ => todo!(),
    };
    match bound_0 {
        Bound::Included(SimpleQueryValue::U8(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U8(r1)) => {
                map_finder::<P, u8>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U8(r1)) => {
                map_finder::<P, u8>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => map_finder::<P, u8>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded)),
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::U8(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U8(r1)) => {
                map_finder::<P, u8>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U8(r1)) => {
                map_finder::<P, u8>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => map_finder::<P, u8>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded)),
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::U16(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U16(r1)) => {
                map_finder::<P, u16>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U16(r1)) => {
                map_finder::<P, u16>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, u16>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::U16(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U16(r1)) => {
                map_finder::<P, u16>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U16(r1)) => {
                map_finder::<P, u16>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, u16>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },

        Bound::Included(SimpleQueryValue::U32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U32(r1)) => {
                map_finder::<P, u32>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U32(r1)) => {
                map_finder::<P, u32>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, u32>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::U32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U32(r1)) => {
                map_finder::<P, u32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U32(r1)) => {
                map_finder::<P, u32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, u32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::U128(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U128(r1)) => {
                map_finder::<P, u128>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U128(r1)) => {
                map_finder::<P, u128>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, u128>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::U128(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U128(r1)) => {
                map_finder::<P, u128>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U128(r1)) => {
                map_finder::<P, u128>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, u128>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::U64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U64(r1)) => {
                map_finder::<P, u64>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U64(r1)) => {
                map_finder::<P, u64>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, u64>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::U64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::U64(r1)) => {
                map_finder::<P, u64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U64(r1)) => {
                map_finder::<P, u64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, u64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::I8(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I8(r1)) => {
                map_finder::<P, i8>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I8(r1)) => {
                map_finder::<P, i8>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => map_finder::<P, i8>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded)),
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::I8(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I8(r1)) => {
                map_finder::<P, i8>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I8(r1)) => {
                map_finder::<P, i8>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => map_finder::<P, i8>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded)),
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::I16(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I16(r1)) => {
                map_finder::<P, i16>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I16(r1)) => {
                map_finder::<P, i16>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, i16>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::I16(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I16(r1)) => {
                map_finder::<P, i16>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I16(r1)) => {
                map_finder::<P, i16>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, i16>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::I32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I32(r1)) => {
                map_finder::<P, i32>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I32(r1)) => {
                map_finder::<P, i32>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, i32>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::I32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I32(r1)) => {
                map_finder::<P, i32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I32(r1)) => {
                map_finder::<P, i32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, i32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::I64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I64(r1)) => {
                map_finder::<P, i64>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I64(r1)) => {
                map_finder::<P, i64>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, i64>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::I64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I64(r1)) => {
                map_finder::<P, i64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I64(r1)) => {
                map_finder::<P, i64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, i64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::I128(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I128(r1)) => {
                map_finder::<P, i128>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I128(r1)) => {
                map_finder::<P, i128>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, i128>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::I128(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::I128(r1)) => {
                map_finder::<P, i128>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I128(r1)) => {
                map_finder::<P, i128>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, i128>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::F32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::F32(r1)) => {
                map_finder::<P, f32>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::F32(r1)) => {
                map_finder::<P, f32>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, f32>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::F32(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::F32(r1)) => {
                map_finder::<P, f32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::F32(r1)) => {
                map_finder::<P, f32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, f32>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::F64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::F64(r1)) => {
                map_finder::<P, f64>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::F64(r1)) => {
                map_finder::<P, f64>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, f64>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::F64(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::F64(r1)) => {
                map_finder::<P, f64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::F64(r1)) => {
                map_finder::<P, f64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, f64>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Included(SimpleQueryValue::String(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::String(r1)) => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Included(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::String(r1)) => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Included(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Included(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },
        Bound::Excluded(SimpleQueryValue::String(r0)) => match bound_1 {
            Bound::Included(SimpleQueryValue::String(r1)) => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Excluded(r0), Bound::Included(r1)))
            }
            Bound::Excluded(SimpleQueryValue::String(r1)) => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Excluded(r0), Bound::Excluded(r1)))
            }
            Bound::Unbounded => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Excluded(r0), Bound::Unbounded))
            }
            _ => unreachable!("wrong value in the range"),
        },

        Bound::Unbounded => match bound_1 {
            Bound::Included(SimpleQueryValue::U8(r1)) => {
                map_finder::<P, u8>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::U16(r1)) => {
                map_finder::<P, u16>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::U32(r1)) => {
                map_finder::<P, u32>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::U64(r1)) => {
                map_finder::<P, u64>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::U128(r1)) => {
                map_finder::<P, u128>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::I8(r1)) => {
                map_finder::<P, i8>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::I16(r1)) => {
                map_finder::<P, i16>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::I32(r1)) => {
                map_finder::<P, i32>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::I64(r1)) => {
                map_finder::<P, i64>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::I128(r1)) => {
                map_finder::<P, i128>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::F32(r1)) => {
                map_finder::<P, f32>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::F64(r1)) => {
                map_finder::<P, f64>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::String(r1)) => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Unbounded, Bound::Included(r1)))
            }
            Bound::Included(SimpleQueryValue::Bool(_)) => unreachable!("wrong value in the range"),
            Bound::Included(SimpleQueryValue::Ref(_)) => unreachable!("wrong value in the range"),
            Bound::Included(SimpleQueryValue::Embedded(_)) => unreachable!("wrong value in the range"),
            Bound::Excluded(SimpleQueryValue::U8(r1)) => {
                map_finder::<P, u8>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U16(r1)) => {
                map_finder::<P, u16>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U32(r1)) => {
                map_finder::<P, u32>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U64(r1)) => {
                map_finder::<P, u64>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::U128(r1)) => {
                map_finder::<P, u128>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I8(r1)) => {
                map_finder::<P, i8>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I16(r1)) => {
                map_finder::<P, i16>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I32(r1)) => {
                map_finder::<P, i32>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I64(r1)) => {
                map_finder::<P, i64>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::I128(r1)) => {
                map_finder::<P, i128>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::F32(r1)) => {
                map_finder::<P, f32>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::F64(r1)) => {
                map_finder::<P, f64>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::String(r1)) => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Unbounded, Bound::Excluded(r1)))
            }
            Bound::Excluded(SimpleQueryValue::Bool(_)) => unreachable!("wrong value in the range"),
            Bound::Excluded(SimpleQueryValue::Ref(_)) => unreachable!("wrong value in the range"),
            Bound::Excluded(SimpleQueryValue::Embedded(_)) => unreachable!("wrong value in the range"),
            Bound::Unbounded => {
                map_finder::<P, String>(order, reader, index_name, (Bound::Unbounded, Bound::Unbounded))
            }
        },
        Bound::Included(SimpleQueryValue::Bool(_)) => unreachable!("wrong value in the range"),
        Bound::Included(SimpleQueryValue::Ref(_)) => unreachable!("wrong value in the range"),
        Bound::Included(SimpleQueryValue::Embedded(_)) => unreachable!("wrong value in the range"),
        Bound::Excluded(SimpleQueryValue::Bool(_)) => unreachable!("wrong value in the range"),
        Bound::Excluded(SimpleQueryValue::Ref(_)) => unreachable!("wrong value in the range"),
        Bound::Excluded(SimpleQueryValue::Embedded(_)) => unreachable!("wrong value in the range"),
    }
}
fn map_finder<'a, P: Persistent + 'static, K: PersistentEmbedded + 'static>(
    order: Order,
    reader: Reader<'a>,
    name: &str,
    range: (Bound<K>, Bound<K>),
) -> SRes<Box<dyn ReaderIterator<Item = (Ref<P>, P)> + 'a>> {
    let found = RangeInstanceIter::new(K::finder().find_range(reader, name, range)?);
    if Order::Desc == order {
        Ok(Box::new(found.reader_rev()))
    } else {
        Ok(Box::new(found))
    }
}
impl InfoFinder for Structsy {
    fn find_index(
        &self,
        type_name: &str,
        field_path: &FieldPathPlan,
        range: Option<(Bound<QueryValuePlan>, Bound<QueryValuePlan>)>,
        mode: Order,
    ) -> Option<IndexInfo> {
        if let Ok(definition) = self.structsy_impl.full_definition_by_name(type_name) {
            let mut desc = Some(&definition.desc);
            let mut last_field = None;
            for field in &field_path.path {
                if let Some(Description::Struct(s)) = desc {
                    if let Some(field) = s.get_field(&field.name()) {
                        if let Some(val) = field.get_field_type_description() {
                            desc = Some(val);
                        }
                        last_field = Some(field);
                    } else {
                        last_field = None;
                    }
                } else {
                    last_field = None;
                }
            }
            if let Some(field) = last_field {
                if let Some(_) = field.indexed() {
                    let index_name = index_name(type_name, &field_path.field_path_names_str());
                    Some(IndexInfo::new(
                        field_path.to_owned(),
                        index_name,
                        range,
                        mode,
                        field.field_type.clone(),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
    fn score_index(&self, index: &IndexInfo) -> SRes<usize> {
        let reader = Reader::Structsy(self.clone());
        if let Some(bounds) = index.index_range.clone() {
            if let Some(bb) = QueryValuePlan::extract_bounds(bounds) {
                index_score(reader, &index.index_name, bb)
            } else {
                index.value_type.index_score(reader, &index.index_name)
            }
        } else {
            index.value_type.index_score(reader, &index.index_name)
        }
    }
}
