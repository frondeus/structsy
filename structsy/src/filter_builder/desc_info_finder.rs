use super::plan_model::{IndexInfo, InfoFinder, QueryValuePlan};
use crate::{
    desc::{index_name, Description},
    structsy::StructsyImpl,
    Order,
};
use std::ops::Bound;

impl InfoFinder for StructsyImpl {
    fn find_index(
        &self,
        type_name: &str,
        field_path: &[String],
        range: Option<(Bound<QueryValuePlan>, Bound<QueryValuePlan>)>,
        mode: Order,
    ) -> Option<IndexInfo> {
        if let Ok(definition) = self.full_definition_by_name(type_name) {
            let mut desc = Some(&definition.desc);
            let mut last_field = None;
            for field in field_path {
                if let Some(Description::Struct(s)) = desc {
                    if let Some(field) = s.get_field(&field) {
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
                    let index_name = index_name(type_name, &field_path.iter().map(|x| x.as_str()).collect::<Vec<_>>());
                    Some(IndexInfo::new(field_path.to_owned(), index_name, range, mode))
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
    fn score_index(&self, index: &IndexInfo) -> usize {
        todo!()
    }
}
