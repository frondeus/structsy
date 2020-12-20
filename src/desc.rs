use crate::{
    format::PersistentEmbedded,
    internal::{EmbeddedDescription, Persistent},
    structsy::INTERNAL_SEGMENT_NAME,
    Ref, SRes, Structsy, StructsyTx,
};
use data_encoding::BASE32_DNSSEC;
use persy::{PersyId, ValueMode};
use std::io::{Read, Write};

#[derive(PartialEq, Eq, Clone, Debug)]
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
    Embedded(Description),
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum FieldType {
    Value(FieldValueType),
    Option(FieldValueType),
    Array(FieldValueType),
    OptionArray(FieldValueType),
}

impl FieldValueType {
    fn read(read: &mut dyn Read) -> SRes<FieldValueType> {
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
            16 => {
                let s = Description::read(read)?;
                FieldValueType::Embedded(s)
            }
            _ => panic!("error on de-serialization"),
        })
    }
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
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
    fn has_refer_to(&self, name: &str) -> bool {
        match self {
            FieldValueType::Ref(t) => t == name,
            FieldValueType::Embedded(t) => t.has_refer_to(name),
            _ => false,
        }
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

impl_field_type!(u8, FieldValueType::U8);
impl_field_type!(u16, FieldValueType::U16);
impl_field_type!(u32, FieldValueType::U32);
impl_field_type!(u64, FieldValueType::U64);
impl_field_type!(u128, FieldValueType::U128);
impl_field_type!(i8, FieldValueType::I8);
impl_field_type!(i16, FieldValueType::I16);
impl_field_type!(i32, FieldValueType::I32);
impl_field_type!(i64, FieldValueType::I64);
impl_field_type!(i128, FieldValueType::I128);
impl_field_type!(f32, FieldValueType::F32);
impl_field_type!(f64, FieldValueType::F64);
impl_field_type!(bool, FieldValueType::Bool);
impl_field_type!(String, FieldValueType::String);

impl<T: Persistent> SimpleType for Ref<T> {
    fn resolve() -> FieldValueType {
        FieldValueType::Ref(T::get_description().get_name().to_string())
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

    fn read(read: &mut dyn Read) -> SRes<FieldType> {
        let t = u8::read(read)?;
        Ok(match t {
            1 => FieldType::Value(FieldValueType::read(read)?),
            2 => FieldType::Option(FieldValueType::read(read)?),
            3 => FieldType::Array(FieldValueType::read(read)?),
            4 => FieldType::OptionArray(FieldValueType::read(read)?),
            _ => panic!("invalid value"),
        })
    }
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
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

/// Field metadata for internal use
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct FieldDescription {
    pub(crate) position: u32,
    pub(crate) name: String,
    pub(crate) field_type: FieldType,
    pub(crate) indexed: Option<ValueMode>,
}

impl FieldDescription {
    pub fn new<T: SupportedType>(position: u32, name: &str, indexed: Option<ValueMode>) -> FieldDescription {
        FieldDescription {
            position,
            name: name.to_string(),
            field_type: FieldType::resolve::<T>(),
            indexed,
        }
    }
    fn read(read: &mut dyn Read) -> SRes<FieldDescription> {
        let position = u32::read(read)?;
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
            position,
            name,
            field_type,
            indexed,
        })
    }
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        self.position.write(write)?;
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

    fn has_refer_to(&self, name: &str) -> bool {
        match &self.field_type {
            FieldType::Array(t) => t.has_refer_to(name),
            FieldType::Option(t) => t.has_refer_to(name),
            FieldType::OptionArray(t) => t.has_refer_to(name),
            FieldType::Value(t) => t.has_refer_to(name),
        }
    }
}

pub struct InternalDescription {
    pub desc: Description,
    pub checked: bool,
    pub id: PersyId,
    segment_name: String,
}

#[derive(Clone)]
pub(crate) struct DefinitionInfo {
    segment_name: String,
}

impl DefinitionInfo {
    pub(crate) fn segment_name(&self) -> &str {
        &self.segment_name
    }
}

impl InternalDescription {
    pub fn has_refer_to(&self, name: &str) -> bool {
        self.desc.has_refer_to(name)
    }

    pub(crate) fn info(&self) -> DefinitionInfo {
        DefinitionInfo {
            segment_name: self.segment_name.clone(),
        }
    }

    pub fn read(id: PersyId, read: &mut dyn Read) -> SRes<Self> {
        let desc = Description::read(read)?;
        let segment_name = String::read(read)?;
        Ok(InternalDescription {
            desc,
            checked: false,
            id,
            segment_name,
        })
    }

    pub fn create<T: Persistent>(desc: Description, structsy: &Structsy) -> SRes<InternalDescription> {
        let rnd = rand::random::<u32>();
        let segment_name = format!("{}_{}", BASE32_DNSSEC.encode(&rnd.to_be_bytes()), desc.get_name());
        let mut buff = Vec::new();
        desc.write(&mut buff)?;
        segment_name.write(&mut buff)?;
        let mut tx = structsy.begin()?;
        let id = tx.trans.insert(INTERNAL_SEGMENT_NAME, &buff)?;
        tx.trans.create_segment(&segment_name)?;
        T::declare(&mut tx)?;
        tx.commit()?;
        Ok(InternalDescription {
            desc,
            checked: true,
            id,
            segment_name,
        })
    }
}

/// Struct metadata for internal use
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct StructDescription {
    pub(crate) name: String,
    pub(crate) fields: Vec<FieldDescription>,
}

impl StructDescription {
    pub fn new(name: &str, fields: &[FieldDescription]) -> StructDescription {
        StructDescription {
            name: name.to_string(),
            fields: Vec::from(fields),
        }
    }
    pub fn read(read: &mut dyn Read) -> SRes<StructDescription> {
        let name = String::read(read)?;
        let n_fields = u32::read(read)?;
        let mut fields = Vec::new();
        for _ in 0..n_fields {
            fields.push(FieldDescription::read(read)?);
        }
        Ok(StructDescription { name, fields })
    }
    pub fn write(&self, write: &mut dyn Write) -> SRes<()> {
        self.name.write(write)?;
        (self.fields.len() as u32).write(write)?;
        for f in &self.fields {
            f.write(write)?;
        }
        Ok(())
    }

    pub(crate) fn has_refer_to(&self, name: &str) -> bool {
        for f in &self.fields {
            if f.has_refer_to(name) {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_field(&self, name: &str) -> Option<&FieldDescription> {
        self.fields.iter().filter(|f| f.name == name).next()
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct VariantDescription {
    pub(crate) position: u32,
    pub(crate) name: String,
    pub(crate) ty: Option<FieldType>,
}

impl VariantDescription {
    pub fn new(name: &str, position: u32) -> Self {
        Self {
            name: name.to_string(),
            position,
            ty: None,
        }
    }
    pub fn new_value<T: SupportedType>(name: &str, position: u32) -> Self {
        Self {
            name: name.to_string(),
            position,
            ty: Some(FieldType::resolve::<T>()),
        }
    }
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        self.name.write(write)?;
        self.position.write(write)?;
        if let Some(ty) = &self.ty {
            true.write(write)?;
            ty.write(write)?;
        } else {
            false.write(write)?;
        }
        Ok(())
    }
    fn read(read: &mut dyn Read) -> SRes<VariantDescription> {
        let name = String::read(read)?;
        let position = u32::read(read)?;
        let ty = if bool::read(read)? {
            Some(FieldType::read(read)?)
        } else {
            None
        };
        Ok(Self { name, position, ty })
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct EnumDescription {
    pub(crate) name: String,
    pub(crate) variants: Vec<VariantDescription>,
}

impl EnumDescription {
    pub fn new(name: &str, variants: &[VariantDescription]) -> Self {
        EnumDescription {
            name: name.to_string(),
            variants: Vec::from(variants),
        }
    }

    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        self.name.write(write)?;
        (self.variants.len() as u32).write(write)?;
        for variant in &self.variants {
            variant.write(write)?;
        }
        Ok(())
    }
    fn read(read: &mut dyn Read) -> SRes<EnumDescription> {
        let name = String::read(read)?;
        let len = u32::read(read)?;
        let mut variants = Vec::new();
        for _ in 0..len {
            variants.push(VariantDescription::read(read)?);
        }
        Ok(Self::new(&name, &variants))
    }
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Description {
    Struct(StructDescription),
    Enum(EnumDescription),
}

impl Description {
    pub fn get_name(&self) -> String {
        match self {
            Description::Struct(s) => s.get_name(),
            Description::Enum(e) => e.get_name(),
        }
    }

    pub fn has_refer_to(&self, name: &str) -> bool {
        match self {
            Description::Struct(s) => s.has_refer_to(name),
            Description::Enum(_e) => unimplemented!(),
        }
    }

    pub fn write(&self, write: &mut dyn Write) -> SRes<()> {
        match self {
            Description::Struct(s) => {
                1u8.write(write)?;
                s.write(write)?;
            }
            Description::Enum(e) => {
                2u8.write(write)?;
                e.write(write)?;
            }
        }
        Ok(())
    }

    pub fn read(read: &mut dyn Read) -> SRes<Description> {
        Ok(match u8::read(read)? {
            1u8 => Description::Struct(StructDescription::read(read)?),
            2u8 => Description::Enum(EnumDescription::read(read)?),
            _ => panic!("wrong description serialization"),
        })
    }
}
