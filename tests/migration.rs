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
        let prep = Structsy::prepare_open(file).unwrap();
        prep.migrate::<DataV0, DataV1>().unwrap();
        let db = prep.open().unwrap();
        db.define::<DataV1>().unwrap();
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
        let prep = Structsy::prepare_open(file.clone()).unwrap();
        prep.migrate::<DataV0, DataV1>().unwrap();
        let db = prep.open().unwrap();
        db.define::<DataV1>().unwrap();
        let found = db.scan::<DataV1>().unwrap().next().unwrap();
        assert_eq!(&found.1.name, "aaa");
        assert_eq!(found.1.size, 0);
    }
    {
        let prep = Structsy::prepare_open(file).unwrap();
        prep.migrate::<DataV0, DataV1>().unwrap();
        let db = prep.open().unwrap();
        db.define::<DataV1>().unwrap();
        let found = db.scan::<DataV1>().unwrap().next().unwrap();
        assert_eq!(&found.1.name, "aaa");
        assert_eq!(found.1.size, 0);
    }
}

mod first {

    use std::path::PathBuf;
    use structsy::{Ref, SRes, Structsy, StructsyTx};
    use structsy_derive::Persistent;

    #[derive(Persistent)]
    struct DataV0 {
        name: String,
    }
    type Data = DataV0;

    #[derive(Persistent)]
    struct DataRef {
        data: Ref<Data>,
    }

    pub fn first_operation(file: PathBuf) -> SRes<()> {
        let db = Structsy::open(file.clone())?;
        db.define::<Data>()?;
        db.define::<DataRef>()?;
        let mut tx = db.begin()?;
        let id = tx.insert(&Data {
            name: "aaa".to_string(),
        })?;

        tx.insert(&DataRef { data: id })?;
        tx.commit()?;
        Ok(())
    }
}

mod second {

    use std::path::PathBuf;

    use structsy::{Ref, SRes, Structsy};
    use structsy_derive::Persistent;

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
        fn from(dt: DataV0) -> Self {
            DataV1 { name: dt.name, size: 0 }
        }
    }

    type Data = DataV1;

    #[derive(Persistent)]
    struct DataRef {
        data: Ref<Data>,
    }

    pub fn second_operation(file: PathBuf) -> SRes<()> {
        let prep = Structsy::prepare_open(file)?;
        prep.migrate::<DataV0, DataV1>()?;
        let db = prep.open()?;
        db.define::<Data>()?;
        db.define::<DataRef>()?;
        let found = db.scan::<Data>()?.next().unwrap();
        assert_eq!(&found.1.name, "aaa");
        assert_eq!(found.1.size, 0);
        let ref_found = db.scan::<DataRef>()?.next().unwrap();
        assert_eq!(ref_found.1.data, found.0);
        Ok(())
    }
}

#[test]
fn test_ref_migration() {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join("test_migration.stry");
    first::first_operation(file.clone()).unwrap();
    second::second_operation(file).unwrap();
}
