use structsy::{Ref, Structsy, StructsyTx};
use structsy_derive::{Persistent, PersistentEmbedded};
use tempfile::tempdir;

#[derive(Persistent, PartialEq, Debug)]
struct AllTypes {
    val_st: String,
    val_u8: u8,
    val_u16: u16,
    val_u32: u32,
    val_u64: u64,
    val_u128: u64,
    val_i8: i8,
    val_i16: i16,
    val_i32: i32,
    val_i64: i64,
    val_i128: i64,
    val_f32: f32,
    val_f64: f64,
    val_ref: Ref<Other>,
    val_emb: Embed,
}
#[derive(Persistent, PartialEq, Debug)]
struct Other {
    val_u8: u8,
}
#[derive(PersistentEmbedded, PartialEq, Debug)]
struct Embed {
    val_u8: u8,
}

#[test]
fn test_persist_all_values() {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join("save_close_open_read.db");
    let db = Structsy::open(&file).expect("can open just create");
    db.define::<Other>().expect("other defined correctly");
    db.define::<AllTypes>().expect("All Types defined correctly");
    let other = Other { val_u8: 5 };
    let mut tx = db.begin().expect("tx started");
    let ref_id = tx.insert(&other).expect("inserted correctly");
    let all = AllTypes {
        val_st: "aa".to_string(),
        val_u8: 10,
        val_u16: 11,
        val_u32: 12,
        val_u64: 13,
        val_u128: 14,
        val_i8: 15,
        val_i16: 16,
        val_i32: 17,
        val_i64: 18,
        val_i128: 19,
        val_f32: 20.0,
        val_f64: 21.0,
        val_ref: ref_id,
        val_emb: Embed { val_u8: 50 },
    };
    let id = tx.insert(&all).expect("inserted correctly");
    db.commit(tx).expect("committed correctly");
    let read_all = db.read(&id).expect("read correctly").expect("is a record");
    assert_eq!(all, read_all);
    let read_ref = db
        .read(&read_all.val_ref)
        .expect("read correctly")
        .expect("is a record");
    assert_eq!(other, read_ref);
}
