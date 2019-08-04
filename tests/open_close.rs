use structsy::{Structsy, StructsyTx};
use structsy_derive::Persistent;
use tempfile::tempdir;

#[derive(Persistent, Debug)]
struct Simple {
    name: String,
}

#[test]
fn save_close_open_read() {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join("save_close_open_read.db");
    let id;
    {
        let db = Structsy::open(&file).expect("can open just create");
        db.define::<Simple>().expect("can define the struct");
        let data = Simple {
            name: "one".to_string(),
        };
        let mut tx = db.begin().expect("transaction started");
        id = tx.insert(&data).expect("data saved correctly");
        db.commit(tx).expect("trasaction is committed");
    }
    {
        let config = Structsy::config(file).create(false);
        let db = Structsy::open(config).expect("can open just create");
        db.define::<Simple>().expect("can define the struct");
        let data = db
            .read(&id)
            .expect("can read just saved record")
            .expect("the record is there");
        assert_eq!(data.name, "one");
    }
}

