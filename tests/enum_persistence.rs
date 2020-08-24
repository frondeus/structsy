use structsy::{Structsy, StructsyTx};
use structsy_derive::{Persistent, PersistentEmbedded};
use tempfile::tempdir;

#[derive(PersistentEmbedded, Eq, PartialEq, Debug)]
struct Value {
    pos: u32,
    val: u32,
}

#[derive(Persistent, Eq, PartialEq, Debug)]
enum SimpleEnum {
    First(Value),
    Second,
}

#[test]
fn save_enum_read() {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join("save_enum_read.db");
    let id;
    let id_second;
    {
        let db = Structsy::open(&file).expect("can open just create");
        db.define::<SimpleEnum>().expect("can define the struct");
        let data = SimpleEnum::First(Value { pos: 10, val: 20 });
        let second_data = SimpleEnum::Second;
        let mut tx = db.begin().expect("transaction started");
        id = tx.insert(&data).expect("data saved correctly");
        id_second = tx.insert(&second_data).expect("data saved correctly");
        tx.commit().expect("trasaction is committed");
    }
    {
        let config = Structsy::config(file).create(false);
        let db = Structsy::open(config).expect("can open just create");
        db.define::<SimpleEnum>().expect("can define the struct");
        let data = db
            .read(&id)
            .expect("can read just saved record")
            .expect("the record is there");
        assert_eq!(data, SimpleEnum::First(Value { pos: 10, val: 20 }));
        let data_second = db
            .read(&id_second)
            .expect("can read just saved record")
            .expect("the record is there");
        assert_eq!(data_second, SimpleEnum::Second);
    }
}
