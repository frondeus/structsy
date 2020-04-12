use structsy::{Structsy, StructsyError, StructsyTx};
use structsy_derive::Persistent;

#[derive(Persistent, Debug, PartialEq)]
struct MyData {
    #[index(mode = "cluster")]
    name: String,
    address: String,
}

fn main() -> Result<(), StructsyError> {
    let db = Structsy::open("my_data.db")?;
    db.define::<MyData>()?;

    let my_data = MyData {
        name: "Structsy".to_string(),
        address: "https://gitlab.com/tglman/structsy".to_string(),
    };
    let mut tx = db.begin()?;
    let id = tx.insert(&my_data)?;
    tx.commit()?;

    let to_find = "Structsy".to_string();
    let iter = MyData::find_by_name(&db, &to_find)?;
    let (_id, data) = iter.iter().next().unwrap();
    assert_eq!(data.name, to_find);

    let mut tx = db.begin()?;
    tx.delete(&id)?;
    tx.commit()?;

    let iter = MyData::find_by_name(&db, &to_find)?;
    assert_eq!(None, iter.iter().next());

    Ok(())
}
