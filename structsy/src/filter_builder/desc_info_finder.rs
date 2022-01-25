use super::{
    plan_model::{IndexInfo, InfoFinder, QueryValuePlan},
    query_model::SimpleQueryValue,
    reader::Reader,
};
use crate::{
    desc::{index_name, Description},
    format::PersistentEmbedded,
    internal::FieldInfo,
    Order, SRes, Structsy,
};
use std::{ops::Bound, rc::Rc};

fn index_score(
    reader: Reader,
    index_name: &str,
    (range_0, range_1): (Bound<SimpleQueryValue>, Bound<SimpleQueryValue>),
) -> SRes<usize> {
    match range_0 {
        Bound::Included(SimpleQueryValue::U8(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U8(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U8(r1)) => {
                u8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::U16(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U16(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U16(r1)) => {
                u16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },

        Bound::Included(SimpleQueryValue::U32(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U32(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U32(r1)) => {
                u32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::U128(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U128(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U128(r1)) => {
                u128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::U64(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::U64(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::U64(r1)) => {
                u64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => u64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I8(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i8::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I8(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I8(r1)) => {
                i8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i8::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I16(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i16::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I16(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I16(r1)) => {
                i16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i16::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I32(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I32(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I32(r1)) => {
                i32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I64(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I64(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I64(r1)) => {
                i64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::I128(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i128::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::I128(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::I128(r1)) => {
                i128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => i128::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::F32(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => f32::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::F32(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F32(r1)) => {
                f32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => f32::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::F64(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => f64::finder().score(reader, index_name, Some((Bound::Included(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Excluded(SimpleQueryValue::F64(r0)) => match range_1 {
            Bound::Included(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Included(r1))))
            }
            Bound::Excluded(SimpleQueryValue::F64(r1)) => {
                f64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Excluded(r1))))
            }
            Bound::Unbounded => f64::finder().score(reader, index_name, Some((Bound::Excluded(r0), Bound::Unbounded))),
            _ => Ok(usize::MAX),
        },
        Bound::Included(SimpleQueryValue::String(r0)) => match range_1 {
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
        Bound::Excluded(SimpleQueryValue::String(r0)) => match range_1 {
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

        Bound::Unbounded => match range_1 {
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

impl InfoFinder for Structsy {
    fn find_index(
        &self,
        type_name: &str,
        field_path: &[Rc<dyn FieldInfo>],
        range: Option<(Bound<QueryValuePlan>, Bound<QueryValuePlan>)>,
        mode: Order,
    ) -> Option<IndexInfo> {
        if let Ok(definition) = self.structsy_impl.full_definition_by_name(type_name) {
            let mut desc = Some(&definition.desc);
            let mut last_field = None;
            for field in field_path {
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
                    let index_name = index_name(type_name, &field_path.iter().map(|x| x.name()).collect::<Vec<_>>());
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
