use structsy::{Structsy, StructsyError, StructsyTx};
use structsy_derive::Persistent;

#[derive(Persistent, Debug, PartialEq)]
struct MyData {
    #[index(mode = "cluster")]
    name: String,
    address: String,
}
impl MyData {
    fn new(name: &str, address: &str) -> MyData {
        MyData {
            name: name.to_string(),
            address: address.to_string(),
        }
    }
}

fn main() -> Result<(), StructsyError> {
    let db = Structsy::open("example_basic.db")?;
    db.define::<MyData>()?;

    let my_data = MyData::new("Structsy", "https://gitlab.com/tglman/structsy");
    let mut tx = db.begin()?;
    let id = tx.insert(&my_data)?;
    tx.commit()?;

    let mut iter = db.scan::<MyData>()?;
    let (_id, data) = iter.next().unwrap();
    assert_eq!(data.name, "Structsy".to_string());

    let mut tx = db.begin()?;
    tx.delete(&id)?;
    tx.commit()?;

    let mut iter = db.scan::<MyData>()?;
    assert_eq!(None, iter.next());

    Ok(())
}
