use crate::{
    desc::{
        Description, EnumDescription, FieldDescription, FieldType, FieldValueType, StructDescription,
        VariantDescription,
    },
    error::SRes,
    internal::PersistentEmbedded,
};
use std::io::Read;

#[derive(PartialEq, Clone, Debug)]
pub enum Record {
    Struct(StructRecord),
    Enum(EnumRecord),
}
impl Record {
    pub fn read(read: &mut dyn Read, desc: &Description) -> SRes<Record> {
        Ok(match desc {
            Description::Struct(s) => Record::Struct(StructRecord::read(read, s)?),
            Description::Enum(e) => Record::Enum(EnumRecord::read(read, e)?),
        })
    }
}

/// Struct metadata for internal use
#[derive(PartialEq, Clone, Debug)]
pub struct StructRecord {
    pub(crate) struct_name: String,
    pub(crate) fields: Vec<FieldValue>,
}
impl StructRecord {
    fn read(read: &mut dyn Read, desc: &StructDescription) -> SRes<StructRecord> {
        let mut fields = Vec::new();
        for field in desc.fields() {
            fields.push(FieldValue::read(read, field)?);
        }
        Ok(StructRecord {
            struct_name: desc.get_name().clone(),
            fields,
        })
    }
    pub fn type_name(&self) -> &str {
        &self.struct_name
    }
    pub fn fileds(&self) -> impl Iterator<Item = &FieldValue> {
        self.fields.iter()
    }
    pub fn filed(&self, name: &str) -> Option<&FieldValue> {
        for field in &self.fields {
            if field.name == name {
                return Some(&field);
            }
        }
        None
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct EnumRecord {
    pub(crate) name: String,
    pub(crate) variants: Box<VariantValue>,
}
impl EnumRecord {
    fn read(read: &mut dyn Read, desc: &EnumDescription) -> SRes<EnumRecord> {
        let pos = u32::read(read)?;
        let value = VariantValue::read(read, desc.variant(pos as usize))?;
        Ok(EnumRecord {
            name: desc.get_name().clone(),
            variants: Box::new(value),
        })
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct VariantValue {
    pub(crate) position: u32,
    pub(crate) name: String,
    pub(crate) value: Option<Value>,
}
impl VariantValue {
    fn read(read: &mut dyn Read, desc: &VariantDescription) -> SRes<VariantValue> {
        let value = if let Some(value_type) = desc.value_type() {
            Some(Value::read(read, value_type)?)
        } else {
            None
        };
        Ok(VariantValue {
            position: desc.position(),
            name: desc.name().to_owned(),
            value,
        })
    }
}

/// Field metadata for internal use
#[derive(PartialEq, Clone, Debug)]
pub struct FieldValue {
    pub(crate) position: u32,
    pub(crate) name: String,
    pub(crate) field_type: Value,
}
impl FieldValue {
    fn read(read: &mut dyn Read, field: &FieldDescription) -> SRes<FieldValue> {
        Ok(FieldValue {
            position: field.position(),
            name: field.name().to_owned(),
            field_type: Value::read(read, field.field_type())?,
        })
    }
    pub fn position(&self) -> u32 {
        self.position
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn value(&self) -> &Value {
        &self.field_type
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum Value {
    Value(SimpleValue),
    Option(Option<SimpleValue>),
    Array(Vec<SimpleValue>),
    OptionArray(Option<Vec<SimpleValue>>),
}
impl Value {
    fn read(read: &mut dyn Read, field_type: &FieldType) -> SRes<Value> {
        Ok(match field_type {
            FieldType::Value(t) => Value::Value(SimpleValue::read(read, t)?),
            FieldType::Option(t) => {
                if u8::read(read)? == 1 {
                    Value::Option(Some(SimpleValue::read(read, t)?))
                } else {
                    Value::Option(None)
                }
            }
            FieldType::Array(t) => {
                let len = u32::read(read)?;
                let mut v = Vec::new();
                for _ in 0..len {
                    v.push(SimpleValue::read(read, t)?);
                }
                Value::Array(v)
            }
            FieldType::OptionArray(t) => {
                if u8::read(read)? == 1 {
                    let len = u32::read(read)?;
                    let mut v = Vec::new();
                    for _ in 0..len {
                        v.push(SimpleValue::read(read, t)?);
                    }
                    Value::OptionArray(Some(v))
                } else {
                    Value::OptionArray(None)
                }
            }
        })
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum SimpleValue {
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
    Ref(String),
    Embedded(Record),
}
impl SimpleValue {
    fn read(read: &mut dyn Read, value_type: &FieldValueType) -> SRes<SimpleValue> {
        use crate::desc::FieldValueType::*;
        Ok(match value_type {
            U8 => SimpleValue::U8(u8::read(read)?),
            U16 => SimpleValue::U16(u16::read(read)?),
            U32 => SimpleValue::U32(u32::read(read)?),
            U64 => SimpleValue::U64(u64::read(read)?),
            U128 => SimpleValue::U128(u128::read(read)?),
            I8 => SimpleValue::I8(i8::read(read)?),
            I16 => SimpleValue::I16(i16::read(read)?),
            I32 => SimpleValue::I32(i32::read(read)?),
            I64 => SimpleValue::I64(i64::read(read)?),
            I128 => SimpleValue::I128(i128::read(read)?),
            F32 => SimpleValue::F32(f32::read(read)?),
            F64 => SimpleValue::F64(f64::read(read)?),
            Bool => SimpleValue::Bool(bool::read(read)?),
            String => SimpleValue::String(std::string::String::read(read)?),
            Ref(t) => SimpleValue::Ref(format!("{}@{}", t, std::string::String::read(read)?)),
            Embedded(desc) => SimpleValue::Embedded(Record::read(read, &desc)?),
        })
    }
}
