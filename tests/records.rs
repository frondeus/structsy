use structsy::{
    record::{Record, SimpleValue, Value},
    RawAccess, SRes, Structsy, StructsyTx,
};
use structsy_derive::Persistent;
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
    structsy_inst("scan_raw", |db| {
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
