use std::str::FromStr;

use structsy::{
    internal::{EnumDescriptionBuilder, SimpleValueTypeBuilder, StructDescriptionBuilder, ValueTypeBuilder},
    record::{Record, SimpleValue, Value},
    RawAccess, SRes, Structsy, StructsyTx,
};
use structsy_derive::{Persistent, PersistentEmbedded};
use tempfile::tempdir;

fn structsy_inst(name: &str, test: fn(db: &Structsy) -> SRes<()>) {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join(format!("{}.stry", name));

    let db = Structsy::open(&file).expect("can open just create");
    test(&db).expect("test is fine");
}

#[derive(Persistent)]
struct Basic {
    name: String,
}

impl Basic {
    fn new(name: &str) -> Basic {
        Basic { name: name.to_string() }
    }
}

#[test]
fn test_scan_raw() {
    structsy_inst("scan_raw", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.insert(&Basic::new("bbb"))?;
        tx.insert(&Basic::new("ccc"))?;
        tx.commit()?;

        let iter = db.raw_scan("Basic")?;
        let mut value = Vec::new();
        for (_id, rec) in iter {
            match rec {
                Record::Struct(st) => {
                    let field = st.field("name").unwrap();
                    match field.value() {
                        Value::Value(SimpleValue::String(s)) => value.push(s.clone()),
                        _ => panic!("wrong value"),
                    }
                }
                _ => panic!("wrong record"),
            }
        }
        assert_eq!(value, vec!["aaa", "bbb", "ccc"]);
        Ok(())
    });
}

#[derive(Persistent)]
enum BasicEnum {
    Basic(String),
    None,
}

impl BasicEnum {
    fn new(name: &str) -> Self {
        Self::Basic(name.to_string())
    }
    fn none() -> Self {
        Self::None
    }
}

#[test]
fn test_scan_raw_enum() {
    structsy_inst("scan_raw_enum", |db| {
        db.define::<BasicEnum>()?;
        let mut tx = db.begin()?;
        tx.insert(&BasicEnum::new("aaa"))?;
        tx.insert(&BasicEnum::new("bbb"))?;
        tx.insert(&BasicEnum::none())?;
        tx.commit()?;

        let iter = db.raw_scan("BasicEnum")?;
        let mut value = Vec::new();
        for (_id, rec) in iter {
            match rec {
                Record::Enum(en) => {
                    let variant = en.variant();
                    match variant.value() {
                        Some(Value::Value(SimpleValue::String(s))) => value.push(s.clone()),
                        None => value.push("__none".to_owned()),
                        _ => panic!("wrong value"),
                    }
                }
                _ => panic!("wrong record"),
            }
        }
        assert_eq!(value, vec!["aaa", "bbb", "__none"]);
        Ok(())
    });
}

#[test]
fn test_raw_read_insert() {
    structsy_inst("raw_read_insert", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.commit()?;

        let iter = db.raw_scan("Basic")?;
        let mut raw_tx = db.raw_begin()?;
        for (_id, rec) in iter {
            raw_tx.raw_insert(&rec)?;
        }
        raw_tx.prepare()?.commit()?;
        let iter = db.raw_scan("Basic")?;
        for (_id, rec) in iter {
            match rec {
                Record::Struct(st) => {
                    let field = st.field("name").unwrap();
                    match field.value() {
                        Value::Value(SimpleValue::String(s)) => assert_eq!("aaa", s),
                        _ => panic!("wrong value"),
                    }
                }
                _ => panic!("wrong record"),
            }
        }
        assert_eq!(db.raw_scan("Basic")?.count(), 2);
        Ok(())
    });
}

#[test]
fn test_raw_read_update() {
    structsy_inst("scan_raw_read_update", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.insert(&Basic::new("bbb"))?;
        tx.commit()?;

        let mut iter = db.raw_scan("Basic")?;
        let (_, record) = iter.next().unwrap();
        let (id, _) = iter.next().unwrap();
        // Set the value of the first record on the second record
        let mut tx = db.raw_begin()?;
        tx.raw_update(&id, &record)?;
        tx.prepare()?.commit()?;

        let iter = db.raw_scan("Basic")?;
        let mut value = Vec::new();
        for (_id, rec) in iter {
            match rec {
                Record::Struct(st) => {
                    let field = st.field("name").unwrap();
                    match field.value() {
                        Value::Value(SimpleValue::String(s)) => value.push(s.clone()),
                        _ => panic!("wrong value"),
                    }
                }
                _ => panic!("wrong record"),
            }
        }
        assert_eq!(value, vec!["aaa", "aaa"]);
        Ok(())
    });
}

#[test]
fn test_raw_read_delete() {
    structsy_inst("raw_read_delete", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.commit()?;

        let iter = db.raw_scan("Basic")?;
        let mut raw_tx = db.raw_begin()?;
        for (id, _rec) in iter {
            raw_tx.raw_delete(&id)?;
        }
        raw_tx.prepare()?.commit()?;
        assert_eq!(db.raw_scan("Basic")?.count(), 0);
        Ok(())
    });
}

#[test]
fn test_raw_read_update_field() {
    structsy_inst("scan_raw_read_update", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.commit()?;

        let mut iter = db.raw_scan("Basic")?;
        let (id, mut record) = iter.next().unwrap();
        match &mut record {
            Record::Struct(rec) => rec.set_field("name", String::from_str("ccc").unwrap())?,
            _ => panic!("wrong record"),
        }
        // Set the value of the first record on the second record
        let mut tx = db.raw_begin()?;
        tx.raw_update(&id, &record)?;
        tx.prepare()?.commit()?;

        let iter = db.raw_scan("Basic")?;
        let mut value = Vec::new();
        for (_id, rec) in iter {
            match rec {
                Record::Struct(st) => {
                    let field = st.field("name").unwrap();
                    match field.value() {
                        Value::Value(SimpleValue::String(s)) => value.push(s.clone()),
                        _ => panic!("wrong value"),
                    }
                }
                _ => panic!("wrong record"),
            }
        }
        assert_eq!(value, vec!["ccc"]);
        Ok(())
    });
}

#[test]
fn test_description_mapping() {
    structsy_inst("description_mapping", |db| {
        db.define::<Basic>()?;

        let od = db.list_defined()?.filter(|d| d.get_name() == "Basic").next().unwrap();

        let mut desc_builder = StructDescriptionBuilder::new("Basic");
        let field_type = ValueTypeBuilder::simple(SimpleValueTypeBuilder::from_name("String").build()).build();
        let indexed = None;
        desc_builder = desc_builder.add_field(0, "name".to_owned(), field_type, indexed);

        assert_eq!(od, desc_builder.build());

        Ok(())
    });
}

#[derive(Persistent)]
enum ToMap {
    Variant,
    SecondVariant(String),
    ThirdVariant(Emb),
}

#[derive(PersistentEmbedded)]
struct Emb {
    name: String,
}

#[test]
fn test_description_mapping_enum() {
    structsy_inst("enum_description", |db| {
        db.define::<ToMap>()?;

        let od = db.list_defined()?.filter(|d| d.get_name() == "ToMap").next().unwrap();

        let mut desc_builder = EnumDescriptionBuilder::new("ToMap");
        let field_type = ValueTypeBuilder::simple(SimpleValueTypeBuilder::from_name("String").build()).build();
        desc_builder = desc_builder.add_variant(0, "Variant", None);
        desc_builder = desc_builder.add_variant(1, "SecondVariant", Some(field_type));
        let emb = StructDescriptionBuilder::new("Emb")
            .add_field(
                0,
                "name".to_owned(),
                ValueTypeBuilder::simple(SimpleValueTypeBuilder::from_name("String").build()).build(),
                None,
            )
            .build();
        let field_type = ValueTypeBuilder::simple(SimpleValueTypeBuilder::embedded(emb).build()).build();
        desc_builder = desc_builder.add_variant(2, "ThirdVariant", Some(field_type));

        assert_eq!(od, desc_builder.build());

        Ok(())
    });
}
