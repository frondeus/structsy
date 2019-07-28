use super::{EmbeddedDescription, Persistent, Ref, SRes};
use crate::format::PersistentEmbedded;
use persy::ValueMode;
use std::io::{Read, Write};

pub enum FieldValueType {
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    F32,
    F64,
    Bool,
    String,
    Ref(String),
    Embedded(StructDescription),
}
pub enum FieldType {
    Value(FieldValueType),
    Option(FieldValueType),
    Array(FieldValueType),
    OptionArray(FieldValueType),
}

impl FieldValueType {
    fn read(read: &mut Read) -> SRes<FieldValueType> {
        let sv = u8::read(read)?;
        Ok(match sv {
            1 => FieldValueType::U8,
            2 => FieldValueType::U16,
            3 => FieldValueType::U32,
            4 => FieldValueType::U64,
            5 => FieldValueType::U128,
            6 => FieldValueType::I8,
            7 => FieldValueType::I16,
            8 => FieldValueType::I32,
            9 => FieldValueType::I64,
            10 => FieldValueType::I128,
            11 => FieldValueType::F32,
            12 => FieldValueType::F64,
            13 => FieldValueType::Bool,
            14 => FieldValueType::String,
            15 => {
                let s = String::read(read)?;
                FieldValueType::Ref(s)
            }
            26 => {
                let s = StructDescription::read(read)?;
                FieldValueType::Embedded(s)
            }
            _ => panic!("error on de-serialization"),
        })
    }
    fn write(&self, write: &mut Write) -> SRes<()> {
        match self {
            FieldValueType::U8 => u8::write(&1, write)?,
            FieldValueType::U16 => u8::write(&2, write)?,
            FieldValueType::U32 => u8::write(&3, write)?,
            FieldValueType::U64 => u8::write(&4, write)?,
            FieldValueType::U128 => u8::write(&5, write)?,
            FieldValueType::I8 => u8::write(&6, write)?,
            FieldValueType::I16 => u8::write(&7, write)?,
            FieldValueType::I32 => u8::write(&8, write)?,
            FieldValueType::I64 => u8::write(&9, write)?,
            FieldValueType::I128 => u8::write(&10, write)?,
            FieldValueType::F32 => u8::write(&11, write)?,
            FieldValueType::F64 => u8::write(&12, write)?,
            FieldValueType::Bool => u8::write(&13, write)?,
            FieldValueType::String => u8::write(&14, write)?,
            FieldValueType::Ref(t) => {
                u8::write(&15, write)?;
                String::write(&t, write)?;
            }
            FieldValueType::Embedded(t) => {
                u8::write(&16, write)?;
                t.write(write)?;
            }
        }
        Ok(())
    }
}
pub trait SupportedType {
    fn resolve() -> FieldType;
}
pub trait SimpleType {
    fn resolve() -> FieldValueType;
}
macro_rules! impl_field_type {
    ($t:ident,$v1:expr) => {
        impl SimpleType for $t {
            fn resolve() -> FieldValueType {
                $v1
            }
        }
    };
}

impl_field_type!(u8, (FieldValueType::U8));
impl_field_type!(u16, (FieldValueType::U16));
impl_field_type!(u32, (FieldValueType::U32));
impl_field_type!(u64, (FieldValueType::U64));
impl_field_type!(u128, (FieldValueType::U128));
impl_field_type!(i8, (FieldValueType::I8));
impl_field_type!(i16, (FieldValueType::I16));
impl_field_type!(i32, (FieldValueType::I32));
impl_field_type!(i64, (FieldValueType::I64));
impl_field_type!(i128, (FieldValueType::I128));
impl_field_type!(f32, (FieldValueType::F32));
impl_field_type!(f64, (FieldValueType::F64));
impl_field_type!(bool, (FieldValueType::Bool));
impl_field_type!(String, (FieldValueType::String));

impl<T: Persistent> SimpleType for Ref<T> {
    fn resolve() -> FieldValueType {
        FieldValueType::Ref(T::get_description().name)
    }
}

impl<T: EmbeddedDescription> SimpleType for T {
    fn resolve() -> FieldValueType {
        FieldValueType::Embedded(T::get_description())
    }
}

impl<T: SimpleType> SupportedType for T {
    fn resolve() -> FieldType {
        FieldType::Value(T::resolve())
    }
}

impl<T: SimpleType> SupportedType for Option<T> {
    fn resolve() -> FieldType {
        FieldType::Option(T::resolve())
    }
}

impl<T: SimpleType> SupportedType for Option<Vec<T>> {
    fn resolve() -> FieldType {
        FieldType::OptionArray(T::resolve())
    }
}

impl<T: SimpleType> SupportedType for Vec<T> {
    fn resolve() -> FieldType {
        FieldType::Array(T::resolve())
    }
}

impl FieldType {
    pub fn resolve<T: SupportedType>() -> FieldType {
        T::resolve()
    }

    fn read(read: &mut Read) -> SRes<FieldType> {
        let t = u8::read(read)?;
        Ok(match t {
            1 => FieldType::Value(FieldValueType::read(read)?),
            2 => FieldType::Option(FieldValueType::read(read)?),
            3 => FieldType::Array(FieldValueType::read(read)?),
            4 => FieldType::OptionArray(FieldValueType::read(read)?),
            _ => panic!("invalid value"),
        })
    }
    fn write(&self, write: &mut Write) -> SRes<()> {
        match self {
            FieldType::Value(t) => {
                u8::write(&1, write)?;
                t.write(write)?;
            }
            FieldType::Option(t) => {
                u8::write(&2, write)?;
                t.write(write)?;
            }
            FieldType::Array(t) => {
                u8::write(&3, write)?;
                t.write(write)?;
            }
            FieldType::OptionArray(t) => {
                u8::write(&4, write)?;
                t.write(write)?;
            }
        }
        Ok(())
    }
}

pub struct FieldDescription {
    pub(crate) name: String,
    pub(crate) field_type: FieldType,
    pub(crate) indexed: Option<ValueMode>,
}

impl FieldDescription {
    pub fn new(name: &str, field_type: FieldType, indexed: Option<ValueMode>) -> FieldDescription {
        FieldDescription {
            name: name.to_string(),
            field_type,
            indexed,
        }
    }
    fn read(read: &mut Read) -> SRes<FieldDescription> {
        let name = String::read(read)?;
        let field_type = FieldType::read(read)?;
        let indexed_value = u8::read(read)?;
        let indexed = match indexed_value {
            0 => None,
            1 => Some(ValueMode::CLUSTER),
            2 => Some(ValueMode::EXCLUSIVE),
            3 => Some(ValueMode::REPLACE),
            _ => panic!("index type reading failure"),
        };
        Ok(FieldDescription {
            name,
            field_type,
            indexed,
        })
    }
    fn write(&self, write: &mut Write) -> SRes<()> {
        self.name.write(write)?;
        self.field_type.write(write)?;
        match self.indexed {
            None => u8::write(&0, write)?,
            Some(ValueMode::CLUSTER) => u8::write(&1, write)?,
            Some(ValueMode::EXCLUSIVE) => u8::write(&2, write)?,
            Some(ValueMode::REPLACE) => u8::write(&3, write)?,
        }
        Ok(())
    }
}

pub struct StructDescription {
    pub(crate) name: String,
    pub(crate) hash_id: u64,
    pub(crate) fields: Vec<FieldDescription>,
}

impl StructDescription {
    pub fn new(name: &str, hash_id: u64, fields: Vec<FieldDescription>) -> StructDescription {
        StructDescription {
            name: name.to_string(),
            hash_id,
            fields,
        }
    }
    pub(crate) fn read(read: &mut Read) -> SRes<StructDescription> {
        let name = String::read(read)?;
        let hash_id = u64::read(read)?;
        let n_fields = u32::read(read)?;
        let mut fields = Vec::new();
        for _ in 0..n_fields {
            fields.push(FieldDescription::read(read)?);
        }
        Ok(StructDescription { name, hash_id, fields })
    }
    pub(crate) fn write(&self, write: &mut Write) -> SRes<()> {
        self.name.write(write)?;
        self.hash_id.write(write)?;
        (self.fields.len() as u32).write(write)?;
        for f in &self.fields {
            f.write(write)?;
        }
        Ok(())
    }
}
