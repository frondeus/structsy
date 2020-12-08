use structsy::{Structsy, StructsyTx};
use structsy_derive::Persistent;
use tempfile::tempdir;

#[derive(Persistent)]
struct DataV0 {
    name: String,
}

#[derive(Persistent)]
struct DataV1 {
    name: String,
    size: u32,
}

impl From<DataV0> for DataV1 {
    fn from(data: DataV0) -> DataV1 {
        DataV1 {
            name: data.name,
            size: 0,
        }
    }
}

#[test]
fn test_migration() {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join("test_migration.stry");
    {
        let db = Structsy::open(file.clone()).unwrap();
        db.define::<DataV0>().unwrap();
        let mut tx = db.begin().unwrap();
        tx.insert(&DataV0 {
            name: "aaa".to_string(),
        })
        .unwrap();
        tx.commit().unwrap();
    }
    {
        let db = Structsy::open(file).unwrap();
        db.define::<DataV1>().unwrap();
        db.migrate::<DataV0, DataV1>().unwrap();
        let found = db.scan::<DataV1>().unwrap().next().unwrap();
        assert_eq!(&found.1.name, "aaa");
        assert_eq!(found.1.size, 0);
    }
}

#[test]
fn test_double_migration() {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join("test_migration.stry");
    {
        let db = Structsy::open(file.clone()).unwrap();
        db.define::<DataV0>().unwrap();
        let mut tx = db.begin().unwrap();
        tx.insert(&DataV0 {
            name: "aaa".to_string(),
        })
        .unwrap();
        tx.commit().unwrap();
    }
    {
        let db = Structsy::open(file.clone()).unwrap();
        db.define::<DataV1>().unwrap();
        db.migrate::<DataV0, DataV1>().unwrap();
        let found = db.scan::<DataV1>().unwrap().next().unwrap();
        assert_eq!(&found.1.name, "aaa");
        assert_eq!(found.1.size, 0);
    }
    {
        let db = Structsy::open(file).unwrap();
        db.define::<DataV1>().unwrap();
        db.migrate::<DataV0, DataV1>().unwrap();
        let found = db.scan::<DataV1>().unwrap().next().unwrap();
        assert_eq!(&found.1.name, "aaa");
        assert_eq!(found.1.size, 0);
    }
}
