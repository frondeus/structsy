use crate::{
    filter_builder::Reader,
    format::PersistentEmbedded,
    internal::{EmbeddedDescription, Persistent},
    record::{Record, SimpleValue, Value},
    structsy::{StructsyImpl, INTERNAL_SEGMENT_NAME},
    OwnedSytx, Ref, SRes, StructsyTx, Sytx,
};
use data_encoding::BASE32_DNSSEC;
use persy::{IndexType, PersyId, Transaction, ValueMode};
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

pub struct StructDescriptionBuilder {
    desc: StructDescription,
}
impl StructDescriptionBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            desc: StructDescription {
                name: name.to_string(),
                fields: Vec::new(),
            },
        }
    }
    pub fn add_field(mut self, position: u32, name: String, field_type: ValueType, indexed: Option<ValueMode>) -> Self {
        let field = FieldDescription {
            position,
            name,
            field_type,
            indexed,
        };
        self.desc.fields.push(field);
        self
    }

    pub fn build(self) -> Description {
        Description::Struct(self.desc)
    }
}

pub struct EnumDescriptionBuilder {
    en: EnumDescription,
}
impl EnumDescriptionBuilder {
    pub fn new(name: &str) -> Self {
        EnumDescriptionBuilder {
            en: EnumDescription {
                name: name.to_owned(),
                variants: Vec::new(),
            },
        }
    }
    pub fn add_variant(mut self, position: u32, name: &str, value_type: Option<ValueType>) -> Self {
        let var = VariantDescription {
            position,
            name: name.to_owned(),
            ty: value_type,
        };
        self.en.variants.push(var);
        self
    }

    pub fn build(self) -> Description {
        Description::Enum(self.en)
    }
}

pub struct ValueTypeBuilder {
    t: Option<ValueType>,
}

impl ValueTypeBuilder {
    pub fn simple(st: SimpleValueType) -> Self {
        Self {
            t: Some(ValueType::Value(st)),
        }
    }
    pub fn option(st: SimpleValueType) -> Self {
        Self {
            t: Some(ValueType::Option(st)),
        }
    }
    pub fn array(st: SimpleValueType) -> Self {
        Self {
            t: Some(ValueType::Array(st)),
        }
    }
    pub fn option_array(st: SimpleValueType) -> Self {
        Self {
            t: Some(ValueType::OptionArray(st)),
        }
    }
    pub fn build(self) -> ValueType {
        self.t.expect("expect a type")
    }
}

pub struct SimpleValueTypeBuilder {
    t: Option<SimpleValueType>,
}

impl SimpleValueTypeBuilder {
    pub fn from_name(name: &str) -> Self {
        Self {
            t: Some(match name {
                "U8" => SimpleValueType::U8,
                "U16" => SimpleValueType::U16,
                "U32" => SimpleValueType::U32,
                "U64" => SimpleValueType::U64,
                "U128" => SimpleValueType::U128,
                "I8" => SimpleValueType::I8,
                "I16" => SimpleValueType::I16,
                "I32" => SimpleValueType::I32,
                "I63" => SimpleValueType::I64,
                "I128" => SimpleValueType::I128,
                "F32" => SimpleValueType::F32,
                "F64" => SimpleValueType::F64,
                "Bool" => SimpleValueType::Bool,
                "String" => SimpleValueType::String,
                _ => {
                    if name.starts_with("Ref") {
                        panic!("use ref method for reference case");
                    }
                    if name.starts_with("Embedded") {
                        panic!("use ref method for ref case");
                    }
                    panic!("no type found with name {}", name);
                }
            }),
        }
    }
    pub fn embedded(desc: Description) -> Self {
        Self {
            t: Some(SimpleValueType::Embedded(desc)),
        }
    }
    pub fn reference(name: String) -> Self {
        Self {
            t: Some(SimpleValueType::Ref(name)),
        }
    }
    pub fn build(self) -> SimpleValueType {
        self.t.expect("there is a type")
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ValueType {
    Value(SimpleValueType),
    Option(SimpleValueType),
    Array(SimpleValueType),
    OptionArray(SimpleValueType),
}

impl ValueType {
    pub(crate) fn index_score(&self, reader: Reader, index_name: &str) -> SRes<usize> {
        match self {
            ValueType::Value(v) => v.index_score(reader, index_name),
            ValueType::Array(v) => v.index_score(reader, index_name),
            ValueType::Option(v) => v.index_score(reader, index_name),
            ValueType::OptionArray(v) => v.index_score(reader, index_name),
        }
    }
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
                String::write(t, write)?;
            }
            SimpleValueType::Embedded(t) => {
                u8::write(&16, write)?;
                t.write(write)?;
            }
        }
        Ok(())
    }

    pub(crate) fn create_index(
        &self,
        tx: &mut persy::Transaction,
        type_name: &str,
        name: &str,
        value_mode: ValueMode,
    ) -> SRes<()> {
        match self {
            SimpleValueType::U8 => create_index::<u8>(tx, type_name, name, value_mode)?,
            SimpleValueType::U16 => create_index::<u16>(tx, type_name, name, value_mode)?,
            SimpleValueType::U32 => create_index::<u32>(tx, type_name, name, value_mode)?,
            SimpleValueType::U64 => create_index::<u64>(tx, type_name, name, value_mode)?,
            SimpleValueType::U128 => create_index::<u128>(tx, type_name, name, value_mode)?,
            SimpleValueType::I8 => create_index::<i8>(tx, type_name, name, value_mode)?,
            SimpleValueType::I16 => create_index::<i16>(tx, type_name, name, value_mode)?,
            SimpleValueType::I32 => create_index::<i32>(tx, type_name, name, value_mode)?,
            SimpleValueType::I64 => create_index::<i64>(tx, type_name, name, value_mode)?,
            SimpleValueType::I128 => create_index::<i128>(tx, type_name, name, value_mode)?,
            SimpleValueType::F32 => create_index::<f32>(tx, type_name, name, value_mode)?,
            SimpleValueType::F64 => create_index::<f64>(tx, type_name, name, value_mode)?,
            SimpleValueType::Bool => (),
            SimpleValueType::String => create_index::<String>(tx, type_name, name, value_mode)?,
            SimpleValueType::Ref(_) => {
                create_index::<PersyId>(tx, type_name, name, value_mode)?;
            }
            SimpleValueType::Embedded(_v) => (),
        }
        Ok(())
    }

    pub(crate) fn index_score(&self, reader: Reader, index_name: &str) -> SRes<usize> {
        match self {
            SimpleValueType::U8 => u8::finder().score(reader, index_name, None),
            SimpleValueType::U16 => u16::finder().score(reader, index_name, None),
            SimpleValueType::U32 => u32::finder().score(reader, index_name, None),
            SimpleValueType::U64 => u64::finder().score(reader, index_name, None),
            SimpleValueType::U128 => u128::finder().score(reader, index_name, None),
            SimpleValueType::I8 => i8::finder().score(reader, index_name, None),
            SimpleValueType::I16 => i16::finder().score(reader, index_name, None),
            SimpleValueType::I32 => i32::finder().score(reader, index_name, None),
            SimpleValueType::I64 => i64::finder().score(reader, index_name, None),
            SimpleValueType::I128 => i128::finder().score(reader, index_name, None),
            SimpleValueType::F32 => f32::finder().score(reader, index_name, None),
            SimpleValueType::F64 => f64::finder().score(reader, index_name, None),
            SimpleValueType::Bool => Ok(usize::MAX),
            SimpleValueType::String => String::finder().score(reader, index_name, None),
            SimpleValueType::Ref(_) => Ok(usize::MAX),
            SimpleValueType::Embedded(_v) => Ok(usize::MAX),
        }
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

pub(crate) fn index_name(type_name: &str, field_path: &[&str]) -> String {
    format!("{}.{}", type_name, field_path.join("."))
}

fn create_index<T: IndexType>(tx: &mut Transaction, type_name: &str, name: &str, value_mode: ValueMode) -> SRes<()> {
    tx.create_index::<T, PersyId>(&index_name(type_name, &[name]), value_mode)?;
    Ok(())
}
impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Value(s) => write!(f, "Value<{}>", s),
            ValueType::Option(v) => write!(f, "Option<{}>", v),
            ValueType::Array(v) => write!(f, "Array<{}>", v),
            ValueType::OptionArray(v) => write!(f, "OptionArray<{}>", v),
        }
    }
}
impl std::fmt::Display for SimpleValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimpleValueType::U8 => write!(f, "U8"),
            SimpleValueType::U16 => write!(f, "U16"),
            SimpleValueType::U32 => write!(f, "U32"),
            SimpleValueType::U64 => write!(f, "U64"),
            SimpleValueType::U128 => write!(f, "U128"),
            SimpleValueType::I8 => write!(f, "I8"),
            SimpleValueType::I16 => write!(f, "I16"),
            SimpleValueType::I32 => write!(f, "I32"),
            SimpleValueType::I64 => write!(f, "I63"),
            SimpleValueType::I128 => write!(f, "I128"),
            SimpleValueType::F32 => write!(f, "F32"),
            SimpleValueType::F64 => write!(f, "F64"),
            SimpleValueType::Bool => write!(f, "Bool"),
            SimpleValueType::String => write!(f, "String"),
            SimpleValueType::Ref(t) => write!(f, "Ref#{}", t),
            SimpleValueType::Embedded(t) => write!(f, "Embedded#{}", t.get_name()),
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

    pub(crate) fn create_index(
        &self,
        tx: &mut persy::Transaction,
        type_name: &str,
        name: &str,
        value_mode: ValueMode,
    ) -> SRes<()> {
        match self {
            ValueType::Value(t) => t.create_index(tx, type_name, name, value_mode),
            ValueType::Option(t) => t.create_index(tx, type_name, name, value_mode),
            ValueType::Array(t) => t.create_index(tx, type_name, name, value_mode),
            ValueType::OptionArray(t) => t.create_index(tx, type_name, name, value_mode),
        }
    }
}
#[cfg(feature = "serde")]
pub(crate) fn value_mode_serialize<S: serde::Serializer>(
    value: &Option<ValueMode>,
    serilizer: S,
) -> Result<S::Ok, S::Error> {
    if let Some(val) = value {
        let mode = match val {
            ValueMode::Replace => "replace",
            ValueMode::Cluster => "cluster",
            ValueMode::Exclusive => "exclusive",
        };
        serilizer.serialize_some(mode)
    } else {
        serilizer.serialize_none()
    }
}

#[cfg(feature = "serde")]
pub(crate) fn value_mode_deserialize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ValueMode>, D::Error> {
    use serde::Deserialize;
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(v) = value {
        let mode = match v.as_str() {
            "replace" => ValueMode::Replace,
            "cluster" => ValueMode::Cluster,
            "exclusive" => ValueMode::Exclusive,
            _ => return Err(format!("Value Mode '{}' does not exists", v)).map_err(serde::de::Error::custom),
        };
        Ok(Some(mode))
    } else {
        Ok(None)
    }
}

/// Field metadata for internal use
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FieldDescription {
    pub(crate) position: u32,
    pub(crate) name: String,
    pub(crate) field_type: ValueType,
    #[cfg_attr(
        feature = "serde",
        serde(serialize_with = "value_mode_serialize", deserialize_with = "value_mode_deserialize")
    )]
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
            1 => Some(ValueMode::Cluster),
            2 => Some(ValueMode::Exclusive),
            3 => Some(ValueMode::Replace),
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
            Some(ValueMode::Cluster) => u8::write(&1, write)?,
            Some(ValueMode::Exclusive) => u8::write(&2, write)?,
            Some(ValueMode::Replace) => u8::write(&3, write)?,
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

    pub fn get_field_type_description(&self) -> Option<&Description> {
        if let SimpleValueType::Embedded(d) = match &self.field_type {
            ValueType::Value(v) => v,
            ValueType::Array(v) => v,
            ValueType::Option(v) => v,
            ValueType::OptionArray(v) => v,
        } {
            Some(d)
        } else {
            None
        }
    }

    pub fn indexed(&self) -> &Option<ValueMode> {
        &self.indexed
    }

    pub(crate) fn create_index(&self, tx: &mut Transaction, type_name: &str) -> SRes<()> {
        if let Some(t) = &self.indexed {
            self.field_type.create_index(tx, type_name, &self.name, t.clone())?;
        }
        Ok(())
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

    pub(crate) fn int_create(
        desc: Description,
        structsy: &Arc<StructsyImpl>,
        define: impl Fn(&mut OwnedSytx) -> SRes<()>,
    ) -> SRes<InternalDescription> {
        let rnd = rand::random::<u32>();
        let segment_name = format!("{}_{}", BASE32_DNSSEC.encode(&rnd.to_be_bytes()), desc.get_name());
        let mut buff = Vec::new();
        desc.write(&mut buff)?;
        segment_name.write(&mut buff)?;
        false.write(&mut buff)?;
        let mut tx = structsy.begin()?;
        let id = tx.trans.insert(INTERNAL_SEGMENT_NAME, &buff)?;
        tx.trans.create_segment(&segment_name)?;
        define(&mut tx)?;
        tx.commit()?;
        Ok(InternalDescription {
            desc,
            checked: true,
            id,
            segment_name,
            migration_started: false,
        })
    }

    pub(crate) fn create_raw(desc: Description, structsy: &Arc<StructsyImpl>) -> SRes<InternalDescription> {
        let dc = desc.clone();
        Self::int_create(desc, structsy, move |tx| dc.raw_define(&mut tx.trans))
    }

    pub(crate) fn create<T: Persistent>(desc: Description, structsy: &Arc<StructsyImpl>) -> SRes<InternalDescription> {
        Self::int_create(desc, structsy, move |tx| T::declare(tx))
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

    pub(crate) fn raw_define(&self, tx: &mut Transaction) -> SRes<()> {
        for field in &self.fields {
            field.create_index(tx, &self.name)?;
        }
        Ok(())
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

    pub(crate) fn raw_define(&self, _tx: &mut Transaction) -> SRes<()> {
        Ok(())
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

    pub(crate) fn raw_define(&self, tx: &mut Transaction) -> SRes<()> {
        match self {
            Description::Struct(s) => s.raw_define(tx),
            Description::Enum(e) => e.raw_define(tx),
        }
    }
}
