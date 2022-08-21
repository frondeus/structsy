use super::{
    plan_model::{FieldPathPlan, IndexInfo, InfoFinder},
    query_model::RangeQueryValue,
    reader::{Reader, ReaderIterator},
};
use crate::{
    desc::{index_name, Description},
    format::PersistentEmbedded,
    index::{RangeInstanceIter, RangeIter},
    Order, Persistent, Ref, SRes,
};

fn index_score(reader: &mut Reader, index_name: &str, bound: RangeQueryValue) -> SRes<usize> {
    match bound {
        RangeQueryValue::U8(b) => u8::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::U16(b) => u16::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::U32(b) => u32::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::U64(b) => u64::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::U128(b) => u128::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::I8(b) => i8::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::I16(b) => i16::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::I32(b) => i32::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::I64(b) => i64::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::I128(b) => i128::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::F32(b) => f32::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::F64(b) => f64::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::Bool(b) => bool::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::String(b) => String::finder().score(reader, index_name, Some(b)),
        RangeQueryValue::Vec(_) => Ok(usize::MAX),
        RangeQueryValue::OptionVec(_) => Ok(usize::MAX),
        RangeQueryValue::Option(_) => Ok(usize::MAX),
        RangeQueryValue::Ref(_) => Ok(usize::MAX),
        RangeQueryValue::Embedded(_) => Ok(usize::MAX),
    }
}

pub(crate) fn index_find_range<'a, P: Persistent + 'static>(
    reader: Reader<'a>,
    index_name: &str,
    range: RangeQueryValue,
    order: Order,
) -> SRes<Box<dyn ReaderIterator<Item = (Ref<P>, P)> + 'a>> {
    match range {
        RangeQueryValue::U8(b) => map_finder(order, u8::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::U16(b) => map_finder(order, u16::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::U32(b) => map_finder(order, u32::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::U64(b) => map_finder(order, u64::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::U128(b) => map_finder(order, u128::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::I8(b) => map_finder(order, i8::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::I16(b) => map_finder(order, i16::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::I32(b) => map_finder(order, i32::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::I64(b) => map_finder(order, i64::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::I128(b) => map_finder(order, i128::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::F32(b) => map_finder(order, f32::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::F64(b) => map_finder(order, f64::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::Bool(b) => map_finder(order, bool::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::String(b) => map_finder(order, String::finder().find_range(reader, index_name, b)?),
        RangeQueryValue::Vec(_) => unreachable!("wrong value in the range"),
        RangeQueryValue::Option(_) => unreachable!("wrong value in the range"),
        RangeQueryValue::OptionVec(_) => unreachable!("wrong value in the range"),
        RangeQueryValue::Ref(_) => unreachable!("wrong value in the range"),
        RangeQueryValue::Embedded(_) => unreachable!("wrong value in the range"),
    }
}
fn map_finder<'a, P: Persistent + 'static, K: PersistentEmbedded + 'static>(
    order: Order,
    iter: RangeIter<'a, K>,
) -> SRes<Box<dyn ReaderIterator<Item = (Ref<P>, P)> + 'a>> {
    let found = RangeInstanceIter::new(iter);
    if Order::Desc == order {
        Ok(Box::new(found.reader_rev()))
    } else {
        Ok(Box::new(found))
    }
}

fn support_index(val: &Option<RangeQueryValue>) -> bool {
    match val {
        Some(RangeQueryValue::OptionVec(_)) => false,
        Some(RangeQueryValue::Vec(_)) => false,
        Some(RangeQueryValue::Option(_)) => false,
        _ => true,
    }
}

impl<'a> InfoFinder for Reader<'a> {
    fn find_index(
        &self,
        type_name: &str,
        field_path: &FieldPathPlan,
        range: Option<RangeQueryValue>,
        mode: Order,
    ) -> Option<IndexInfo> {
        if !support_index(&range) {
            return None;
        }
        if let Ok(definition) = self.structsy().structsy_impl.full_definition_by_name(type_name) {
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
    fn score_index(&mut self, index: &IndexInfo) -> SRes<usize> {
        if let Some(bounds) = index.index_range.clone() {
            index_score(self, &index.index_name, bounds)
        } else {
            index.value_type.index_score(self, &index.index_name)
        }
    }
}
