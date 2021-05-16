use crate::{
    desc::{
        Description, EnumDescription, FieldDescription, FieldType, FieldValueType, StructDescription,
        VariantDescription,
    },
    error::SRes,
    internal::PersistentEmbedded,
};
use persy::{IndexType, PersyId, Transaction, ValueMode};
use std::io::{Read, Write};

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

    pub fn write(&self, write: &mut dyn Write, desc: &Description) -> SRes<()> {
        Ok(match self {
            Record::Struct(s) => {
                let sd = match desc {
                    Description::Struct(ed) => ed,
                    _ => panic!("description does not match the value"),
                };
                s.write(write, sd)?;
            }
            Record::Enum(e) => {
                let ed = match desc {
                    Description::Enum(ed) => ed,
                    _ => panic!("description does not match the value"),
                };
                e.write(write, ed)?;
            }
        })
    }

    pub(crate) fn put_indexes(&self, tx: &mut persy::Transaction, id: &PersyId) -> SRes<()> {
        Ok(match self {
            Record::Struct(s) => {
                s.put_indexes(tx, id)?;
            }
            Record::Enum(e) => {
                e.put_indexes(tx, id)?;
            }
        })
    }

    pub(crate) fn remove_indexes(&self, tx: &mut persy::Transaction, id: &PersyId) -> SRes<()> {
        Ok(match self {
            Record::Struct(s) => {
                s.remove_indexes(tx, id)?;
            }
            Record::Enum(e) => {
                e.remove_indexes(tx, id)?;
            }
        })
    }

    pub fn type_name(&self) -> &str {
        match self {
            Record::Struct(s) => s.type_name(),
            Record::Enum(e) => e.type_name(),
        }
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

    fn write(&self, write: &mut dyn Write, desc: &StructDescription) -> SRes<()> {
        for field in &self.fields {
            let fd = if let Some(fd) = desc.get_field(field.name()) {
                fd
            } else {
                panic!("value do not match the definition");
            };

            field.write(write, fd)?;
        }
        Ok(())
    }

    pub fn type_name(&self) -> &str {
        &self.struct_name
    }
    pub fn fields(&self) -> impl Iterator<Item = &FieldValue> {
        self.fields.iter()
    }
    pub fn field(&self, name: &str) -> Option<&FieldValue> {
        for field in &self.fields {
            if field.name == name {
                return Some(&field);
            }
        }
        None
    }

    pub(crate) fn put_indexes(&self, tx: &mut persy::Transaction, id: &PersyId) -> SRes<()> {
        for field in &self.fields {
            field.put_indexes(tx, self.type_name(), id)?;
        }
        Ok(())
    }
    pub(crate) fn remove_indexes(&self, tx: &mut persy::Transaction, id: &PersyId) -> SRes<()> {
        for field in &self.fields {
            field.remove_indexes(tx, self.type_name(), id)?;
        }
        Ok(())
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct EnumRecord {
    pub(crate) name: String,
    pub(crate) variant: Box<VariantValue>,
}
impl EnumRecord {
    fn read(read: &mut dyn Read, desc: &EnumDescription) -> SRes<EnumRecord> {
        let pos = u32::read(read)?;
        let value = VariantValue::read(read, desc.variant(pos as usize))?;
        Ok(EnumRecord {
            name: desc.get_name().clone(),
            variant: Box::new(value),
        })
    }

    fn write(&self, write: &mut dyn Write, desc: &EnumDescription) -> SRes<()> {
        u32::write(&self.variant.position, write)?;
        self.variant
            .write(write, desc.variant(self.variant.position as usize))?;
        Ok(())
    }

    pub fn variant(&self) -> &VariantValue {
        &self.variant
    }

    pub fn type_name(&self) -> &str {
        &self.name
    }
    pub(crate) fn put_indexes(&self, tx: &mut persy::Transaction, id: &PersyId) -> SRes<()> {
        self.variant.put_indexes(tx, id)
    }
    pub(crate) fn remove_indexes(&self, tx: &mut persy::Transaction, id: &PersyId) -> SRes<()> {
        self.variant.remove_indexes(tx, id)
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
    fn write(&self, write: &mut dyn Write, desc: &VariantDescription) -> SRes<()> {
        if let Some(val) = &self.value {
            val.write(write, desc.value_type().as_ref().expect("value and desc match"))?;
        }
        Ok(())
    }
    pub fn value(&self) -> &Option<Value> {
        &self.value
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub(crate) fn put_indexes(&self, _tx: &mut persy::Transaction, _id: &PersyId) -> SRes<()> {
        Ok(())
    }
    pub(crate) fn remove_indexes(&self, _tx: &mut persy::Transaction, _id: &PersyId) -> SRes<()> {
        Ok(())
    }
}

/// Field metadata for internal use
#[derive(PartialEq, Clone, Debug)]
pub struct FieldValue {
    pub(crate) position: u32,
    pub(crate) name: String,
    pub(crate) field_type: Value,
    pub(crate) indexed: Option<ValueMode>,
}
impl FieldValue {
    fn read(read: &mut dyn Read, field: &FieldDescription) -> SRes<FieldValue> {
        Ok(FieldValue {
            position: field.position(),
            name: field.name().to_owned(),
            field_type: Value::read(read, field.field_type())?,
            indexed: field.indexed.clone(),
        })
    }
    fn write(&self, write: &mut dyn Write, field: &FieldDescription) -> SRes<()> {
        self.field_type.write(write, &field.field_type)
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
    pub(crate) fn put_indexes(&self, tx: &mut persy::Transaction, type_name: &str, id: &PersyId) -> SRes<()> {
        if let Some(_) = self.indexed {
            self.field_type.put_index(tx, type_name, &self.name, id)?;
        }
        Ok(())
    }
    pub(crate) fn remove_indexes(&self, tx: &mut persy::Transaction, type_name: &str, id: &PersyId) -> SRes<()> {
        if let Some(_) = self.indexed {
            self.field_type.remove_index(tx, type_name, &self.name, id)?;
        }
        Ok(())
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

    fn write(&self, write: &mut dyn Write, field_type: &FieldType) -> SRes<()> {
        Ok(match self {
            Value::Value(v) => {
                let vt = match field_type {
                    FieldType::Value(vt) => vt,
                    _ => panic!("desc do not match field type"),
                };
                v.write(write, vt)?;
            }
            Value::Option(v) => {
                let vt = match field_type {
                    FieldType::Option(vt) => vt,
                    _ => panic!("desc do not match field type"),
                };
                if let Some(sv) = v {
                    u8::write(&1, write)?;
                    sv.write(write, vt)?;
                } else {
                    u8::write(&0, write)?;
                }
            }
            Value::Array(v) => {
                let vt = match field_type {
                    FieldType::Array(vt) => vt,
                    _ => panic!("desc do not match field type"),
                };
                u32::write(&(v.len() as u32), write)?;
                for sv in v {
                    sv.write(write, vt)?;
                }
            }
            Value::OptionArray(v) => {
                let vt = match field_type {
                    FieldType::OptionArray(vt) => vt,
                    _ => panic!("desc do not match field type"),
                };
                if let Some(sv) = v {
                    u8::write(&1, write)?;
                    u32::write(&(sv.len() as u32), write)?;
                    for ssv in sv {
                        ssv.write(write, vt)?;
                    }
                } else {
                    u8::write(&0, write)?;
                }
            }
        })
    }

    pub(crate) fn put_index(&self, tx: &mut persy::Transaction, type_name: &str, name: &str, id: &PersyId) -> SRes<()> {
        Ok(match self {
            Value::Value(v) => {
                v.put_index(tx, type_name, name, id)?;
            }
            Value::Option(v) => {
                if let Some(sv) = v {
                    sv.put_index(tx, type_name, name, id)?;
                }
            }
            Value::Array(v) => {
                for sv in v {
                    sv.put_index(tx, type_name, name, id)?;
                }
            }
            Value::OptionArray(v) => {
                if let Some(sv) = v {
                    for ssv in sv {
                        ssv.put_index(tx, type_name, name, id)?;
                    }
                }
            }
        })
    }
    pub(crate) fn remove_index(
        &self,
        tx: &mut persy::Transaction,
        type_name: &str,
        name: &str,
        id: &PersyId,
    ) -> SRes<()> {
        Ok(match self {
            Value::Value(v) => {
                v.remove_index(tx, type_name, name, id)?;
            }
            Value::Option(v) => {
                if let Some(sv) = v {
                    sv.remove_index(tx, type_name, name, id)?;
                }
            }
            Value::Array(v) => {
                for sv in v {
                    sv.remove_index(tx, type_name, name, id)?;
                }
            }
            Value::OptionArray(v) => {
                if let Some(sv) = v {
                    for ssv in sv {
                        ssv.remove_index(tx, type_name, name, id)?;
                    }
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
    fn write(&self, write: &mut dyn Write, value_type: &FieldValueType) -> SRes<()> {
        Ok(match self {
            SimpleValue::U8(v) => u8::write(v, write)?,
            SimpleValue::U16(v) => u16::write(v, write)?,
            SimpleValue::U32(v) => u32::write(v, write)?,
            SimpleValue::U64(v) => u64::write(v, write)?,
            SimpleValue::U128(v) => u128::write(v, write)?,
            SimpleValue::I8(v) => i8::write(v, write)?,
            SimpleValue::I16(v) => i16::write(v, write)?,
            SimpleValue::I32(v) => i32::write(v, write)?,
            SimpleValue::I64(v) => i64::write(v, write)?,
            SimpleValue::I128(v) => i128::write(v, write)?,
            SimpleValue::F32(v) => f32::write(v, write)?,
            SimpleValue::F64(v) => f64::write(v, write)?,
            SimpleValue::Bool(v) => bool::write(v, write)?,
            SimpleValue::String(v) => std::string::String::write(v, write)?,
            SimpleValue::Ref(v) => {
                let values = v.split('@').collect::<Vec<_>>();
                if values.len() < 2 {
                    panic!("wrong value");
                }
                std::string::String::write(&values[1].to_owned(), write)?;
            }
            SimpleValue::Embedded(v) => {
                let desc = match value_type {
                    FieldValueType::Embedded(desc) => desc,
                    _ => panic!("type do not mach desc"),
                };
                v.write(write, &desc)?;
            }
        })
    }
    pub(crate) fn put_index(&self, tx: &mut persy::Transaction, type_name: &str, name: &str, id: &PersyId) -> SRes<()> {
        Ok(match self {
            SimpleValue::U8(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::U16(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::U32(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::U64(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::U128(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::I8(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::I16(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::I32(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::I64(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::I128(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::F32(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::F64(v) => put_index(tx, type_name, name, v, id)?,
            SimpleValue::Bool(_v) => (),
            SimpleValue::String(v) => put_index(tx, type_name, name, &v.clone(), id)?,
            SimpleValue::Ref(v) => {
                let values = v.split('@').collect::<Vec<_>>();
                if values.len() < 2 {
                    panic!("wrong value");
                }
                put_index(tx, type_name, name, &values[1].parse::<PersyId>()?, id)?;
            }
            SimpleValue::Embedded(_v) => (),
        })
    }

    pub(crate) fn remove_index(
        &self,
        tx: &mut persy::Transaction,
        type_name: &str,
        name: &str,
        id: &PersyId,
    ) -> SRes<()> {
        Ok(match self {
            SimpleValue::U8(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::U16(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::U32(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::U64(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::U128(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::I8(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::I16(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::I32(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::I64(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::I128(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::F32(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::F64(v) => remove_index(tx, type_name, name, v, id)?,
            SimpleValue::Bool(_v) => (),
            SimpleValue::String(v) => remove_index(tx, type_name, name, &v.clone(), id)?,
            SimpleValue::Ref(v) => {
                let values = v.split('@').collect::<Vec<_>>();
                if values.len() < 2 {
                    panic!("wrong value");
                }
                remove_index(tx, type_name, name, &values[1].parse::<PersyId>()?, id)?;
            }
            SimpleValue::Embedded(_v) => (),
        })
    }
}

fn put_index<T: IndexType>(tx: &mut Transaction, type_name: &str, name: &str, k: &T, id: &PersyId) -> SRes<()> {
    tx.put::<T, PersyId>(&format!("{}.{}", type_name, name), k.clone(), id.clone())?;
    Ok(())
}

fn remove_index<T: IndexType>(tx: &mut Transaction, type_name: &str, name: &str, k: &T, id: &PersyId) -> SRes<()> {
    tx.remove::<T, PersyId>(&format!("{}.{}", type_name, name), k.clone(), Some(id.clone()))?;
    Ok(())
}
