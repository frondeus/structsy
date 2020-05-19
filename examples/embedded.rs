use structsy::{Structsy, StructsyError, StructsyTx};
use structsy_derive::{Persistent, PersistentEmbedded};

#[derive(Persistent, Debug, PartialEq)]
struct MyData {
    #[index(mode = "cluster")]
    name: String,
    address: Address,
}
impl MyData {
    fn new(name: &str, address: Address) -> MyData {
        MyData {
            name: name.to_string(),
            address,
        }
    }
}

#[derive(PersistentEmbedded, Debug, PartialEq)]
struct Address {
    host: String,
    port: u16,
}
impl Address {
    fn new(host: &str, port: u16) -> Address {
        Address {
            host: host.to_string(),
            port,
        }
    }
}

fn main() -> Result<(), StructsyError> {
    let db = Structsy::open("my_data.db")?;
    db.define::<MyData>()?;

    let my_data = MyData::new("My host", Address::new("127.0.0.1", 2424));
    let mut tx = db.begin()?;
    let id = tx.insert(&my_data)?;
    tx.commit()?;

    let mut iter = db.scan::<MyData>()?;
    let (_id, data) = iter.next().unwrap();
    assert_eq!(data.name, "My Host");

    let mut tx = db.begin()?;
    tx.delete(&id)?;
    tx.commit()?;

    let mut iter = db.scan::<MyData>()?;
    assert_eq!(None, iter.next());

    Ok(())
}
