use crate::{
    format::PersistentEmbedded,
    internal::{EmbeddedDescription, Persistent},
    record::{Record, SimpleValue, Value},
    structsy::{StructsyImpl, INTERNAL_SEGMENT_NAME},
    Ref, SRes, StructsyTx, Sytx,
};
use data_encoding::BASE32_DNSSEC;
use persy::{PersyId, ValueMode};
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum SimpleValueType {
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
pub enum ValueType {
    Value(SimpleValueType),
    Option(SimpleValueType),
    Array(SimpleValueType),
    OptionArray(SimpleValueType),
}

impl SimpleValueType {
    fn read(read: &mut dyn Read) -> SRes<SimpleValueType> {
        let sv = u8::read(read)?;
        Ok(match sv {
            1 => SimpleValueType::U8,
            2 => SimpleValueType::U16,
            3 => SimpleValueType::U32,
            4 => SimpleValueType::U64,
            5 => SimpleValueType::U128,
            6 => SimpleValueType::I8,
            7 => SimpleValueType::I16,
            8 => SimpleValueType::I32,
            9 => SimpleValueType::I64,
            10 => SimpleValueType::I128,
            11 => SimpleValueType::F32,
            12 => SimpleValueType::F64,
            13 => SimpleValueType::Bool,
            14 => SimpleValueType::String,
            15 => {
                let s = String::read(read)?;
                SimpleValueType::Ref(s)
            }
            16 => {
                let s = Description::read(read)?;
                SimpleValueType::Embedded(s)
            }
            _ => panic!("error on de-serialization"),
        })
    }
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        match self {
            SimpleValueType::U8 => u8::write(&1, write)?,
            SimpleValueType::U16 => u8::write(&2, write)?,
            SimpleValueType::U32 => u8::write(&3, write)?,
            SimpleValueType::U64 => u8::write(&4, write)?,
            SimpleValueType::U128 => u8::write(&5, write)?,
            SimpleValueType::I8 => u8::write(&6, write)?,
            SimpleValueType::I16 => u8::write(&7, write)?,
            SimpleValueType::I32 => u8::write(&8, write)?,
            SimpleValueType::I64 => u8::write(&9, write)?,
            SimpleValueType::I128 => u8::write(&10, write)?,
            SimpleValueType::F32 => u8::write(&11, write)?,
            SimpleValueType::F64 => u8::write(&12, write)?,
            SimpleValueType::Bool => u8::write(&13, write)?,
            SimpleValueType::String => u8::write(&14, write)?,
            SimpleValueType::Ref(t) => {
                u8::write(&15, write)?;
                String::write(&t, write)?;
            }
            SimpleValueType::Embedded(t) => {
                u8::write(&16, write)?;
                t.write(write)?;
            }
        }
        Ok(())
    }

    fn remap_refer(&mut self, old: &str, new: &str) -> bool {
        match self {
            SimpleValueType::Ref(ref mut t) => {
                if t == old {
                    *t = new.to_string();
                    true
                } else {
                    false
                }
            }
            SimpleValueType::Embedded(t) => t.remap_refer(old, new),
            _ => false,
        }
    }
}
pub trait SupportedType {
    fn resolve() -> ValueType;
    fn new(self) -> SRes<Value>;
}
pub trait SimpleType {
    fn resolve() -> SimpleValueType;
    fn new(self) -> SRes<SimpleValue>;
}
macro_rules! impl_field_type {
    ($t:ident,$v1:ident) => {
        impl SimpleType for $t {
            fn resolve() -> SimpleValueType {
                SimpleValueType::$v1
            }
            fn new(self) -> SRes<SimpleValue> {
                Ok(SimpleValue::$v1(self))
            }
        }
    };
}

impl_field_type!(u8, U8);
impl_field_type!(u16, U16);
impl_field_type!(u32, U32);
impl_field_type!(u64, U64);
impl_field_type!(u128, U128);
impl_field_type!(i8, I8);
impl_field_type!(i16, I16);
impl_field_type!(i32, I32);
impl_field_type!(i64, I64);
impl_field_type!(i128, I128);
impl_field_type!(f32, F32);
impl_field_type!(f64, F64);
impl_field_type!(bool, Bool);
impl_field_type!(String, String);

impl<T: Persistent> SimpleType for Ref<T> {
    fn resolve() -> SimpleValueType {
        SimpleValueType::Ref(T::get_description().get_name())
    }
    fn new(self) -> SRes<SimpleValue> {
        Ok(SimpleValue::Ref(format!("{}", self.raw_id)))
    }
}

impl<T: EmbeddedDescription> SimpleType for T {
    fn resolve() -> SimpleValueType {
        SimpleValueType::Embedded(T::get_description())
    }
    fn new(self) -> SRes<SimpleValue> {
        let desk = T::get_description();
        let mut buff = Vec::new();
        self.write(&mut buff)?;
        let record = Record::read(&mut Cursor::new(buff), &desk)?;
        Ok(SimpleValue::Embedded(record))
    }
}

impl<T: SimpleType> SupportedType for T {
    fn resolve() -> ValueType {
        ValueType::Value(T::resolve())
    }
    fn new(self) -> SRes<Value> {
        Ok(Value::Value(self.new()?))
    }
}

impl<T: SimpleType> SupportedType for Option<T> {
    fn resolve() -> ValueType {
        ValueType::Option(T::resolve())
    }
    fn new(self) -> SRes<Value> {
        Ok(Value::Option(self.map(|v| v.new()).transpose()?))
    }
}

impl<T: SimpleType> SupportedType for Option<Vec<T>> {
    fn resolve() -> ValueType {
        ValueType::OptionArray(T::resolve())
    }
    fn new(self) -> SRes<Value> {
        Ok(Value::OptionArray(
            self.map(|vec| vec.into_iter().map(|v| v.new()).collect::<SRes<Vec<SimpleValue>>>())
                .transpose()?,
        ))
    }
}

impl<T: SimpleType> SupportedType for Vec<T> {
    fn resolve() -> ValueType {
        ValueType::Array(T::resolve())
    }
    fn new(self) -> SRes<Value> {
        Ok(Value::Array(
            self.into_iter().map(|v| v.new()).collect::<SRes<Vec<SimpleValue>>>()?,
        ))
    }
}

impl ValueType {
    pub fn resolve<T: SupportedType>() -> ValueType {
        T::resolve()
    }

    fn read(read: &mut dyn Read) -> SRes<ValueType> {
        let t = u8::read(read)?;
        Ok(match t {
            1 => ValueType::Value(SimpleValueType::read(read)?),
            2 => ValueType::Option(SimpleValueType::read(read)?),
            3 => ValueType::Array(SimpleValueType::read(read)?),
            4 => ValueType::OptionArray(SimpleValueType::read(read)?),
            _ => panic!("invalid value"),
        })
    }
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        match self {
            ValueType::Value(t) => {
                u8::write(&1, write)?;
                t.write(write)?;
            }
            ValueType::Option(t) => {
                u8::write(&2, write)?;
                t.write(write)?;
            }
            ValueType::Array(t) => {
                u8::write(&3, write)?;
                t.write(write)?;
            }
            ValueType::OptionArray(t) => {
                u8::write(&4, write)?;
                t.write(write)?;
            }
        }
        Ok(())
    }
    fn remap_refer(&mut self, new: &str, old: &str) -> bool {
        match self {
            ValueType::Array(ref mut t) => t.remap_refer(new, old),
            ValueType::Option(ref mut t) => t.remap_refer(new, old),
            ValueType::OptionArray(ref mut t) => t.remap_refer(new, old),
            ValueType::Value(ref mut t) => t.remap_refer(new, old),
        }
    }
}

/// Field metadata for internal use
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct FieldDescription {
    pub(crate) position: u32,
    pub(crate) name: String,
    pub(crate) field_type: ValueType,
    pub(crate) indexed: Option<ValueMode>,
}

impl FieldDescription {
    pub fn new<T: SupportedType>(position: u32, name: &str, indexed: Option<ValueMode>) -> FieldDescription {
        FieldDescription {
            position,
            name: name.to_string(),
            field_type: ValueType::resolve::<T>(),
            indexed,
        }
    }
    fn read(read: &mut dyn Read) -> SRes<FieldDescription> {
        let position = u32::read(read)?;
        let name = String::read(read)?;
        let field_type = ValueType::read(read)?;
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

    fn remap_refer(&mut self, new: &str, old: &str) -> bool {
        self.field_type.remap_refer(new, old)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn position(&self) -> u32 {
        self.position
    }

    pub fn field_type(&self) -> &ValueType {
        &self.field_type
    }

    pub fn indexed(&self) -> &Option<ValueMode> {
        &self.indexed
    }
}

#[derive(Clone)]
pub struct InternalDescription {
    pub desc: Description,
    pub checked: bool,
    pub id: PersyId,
    segment_name: String,
    migration_started: bool,
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
    pub(crate) fn info(&self) -> DefinitionInfo {
        DefinitionInfo {
            segment_name: self.segment_name.clone(),
        }
    }

    pub fn read(id: PersyId, read: &mut dyn Read) -> SRes<Self> {
        let desc = Description::read(read)?;
        let segment_name = String::read(read)?;
        let migration_started = bool::read(read)?;
        Ok(InternalDescription {
            desc,
            checked: false,
            id,
            segment_name,
            migration_started,
        })
    }

    pub(crate) fn create<T: Persistent>(desc: Description, structsy: &Arc<StructsyImpl>) -> SRes<InternalDescription> {
        let rnd = rand::random::<u32>();
        let segment_name = format!("{}_{}", BASE32_DNSSEC.encode(&rnd.to_be_bytes()), desc.get_name());
        let mut buff = Vec::new();
        desc.write(&mut buff)?;
        segment_name.write(&mut buff)?;
        false.write(&mut buff)?;
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
            migration_started: false,
        })
    }

    pub(crate) fn start_migration(&mut self) {
        self.migration_started = true;
    }

    pub(crate) fn is_migration_started(&self) -> bool {
        self.migration_started
    }

    pub(crate) fn update(&self, st: &Arc<StructsyImpl>) -> SRes<()> {
        let mut tx = st.begin()?;
        self.update_tx(&mut tx)?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn update_tx(&self, tx: &mut dyn Sytx) -> SRes<()> {
        let mut buff = Vec::new();
        self.desc.write(&mut buff)?;
        self.segment_name.write(&mut buff)?;
        self.migration_started.write(&mut buff)?;
        tx.tx().trans.update(INTERNAL_SEGMENT_NAME, &self.id, &buff)?;
        Ok(())
    }

    pub(crate) fn migrate<T: Persistent>(&mut self, tx: &mut dyn Sytx) -> SRes<()> {
        self.migration_started = false;
        self.desc = T::get_description();
        self.update_tx(tx)?;
        Ok(())
    }

    pub(crate) fn remap_refer(&mut self, old: &str, new: &str) -> bool {
        self.desc.remap_refer(old, new)
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

    pub(crate) fn remap_refer(&mut self, old: &str, new: &str) -> bool {
        let mut changed = false;
        for f in &mut self.fields {
            if f.remap_refer(old, new) {
                changed = true;
            }
        }
        changed
    }

    pub(crate) fn get_field(&self, name: &str) -> Option<&FieldDescription> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn fields(&self) -> impl std::iter::Iterator<Item = &FieldDescription> {
        self.fields.iter()
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct VariantDescription {
    pub(crate) position: u32,
    pub(crate) name: String,
    pub(crate) ty: Option<ValueType>,
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
            ty: Some(ValueType::resolve::<T>()),
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
            Some(ValueType::read(read)?)
        } else {
            None
        };
        Ok(Self { name, position, ty })
    }
    fn remap_refer(&mut self, new: &str, old: &str) -> bool {
        if let Some(ref mut r) = self.ty {
            r.remap_refer(new, old)
        } else {
            false
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn position(&self) -> u32 {
        self.position
    }

    pub fn value_type(&self) -> &Option<ValueType> {
        &self.ty
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
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    fn remap_refer(&mut self, new: &str, old: &str) -> bool {
        let mut changed = false;
        for v in &mut self.variants {
            if v.remap_refer(old, new) {
                changed = true;
            }
        }
        changed
    }

    pub fn variants(&self) -> impl std::iter::Iterator<Item = &VariantDescription> {
        self.variants.iter()
    }
    pub fn variant(&self, pos: usize) -> &VariantDescription {
        &self.variants[pos]
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

    pub fn remap_refer(&mut self, old: &str, new: &str) -> bool {
        match self {
            Description::Struct(s) => s.remap_refer(old, new),
            Description::Enum(e) => e.remap_refer(old, new),
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
