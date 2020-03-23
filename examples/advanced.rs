use structsy::{IterResult, Ref, Structsy, StructsyError, StructsyIntoIter, StructsyIter, StructsyTx};
use structsy_derive::{queries, Persistent};

#[derive(Persistent, Debug, PartialEq)]
struct MyData {
    #[index(mode = "cluster")]
    name: String,
    address: String,
}

#[queries(MyData)]
trait MyDataQuery {
    fn search(&self, address: String) -> IterResult<MyData>;
}

fn main() -> Result<(), StructsyError> {
    let db = Structsy::open("my_data.db")?;
    db.define::<MyData>()?;

    let my_data = MyData {
        name: "Structsy".to_string(),
        address: "https://gitlab.com/tglman/structsy".to_string(),
    };
    let mut tx = db.begin()?;
    let _id = tx.insert(&my_data)?;
    db.commit(tx)?;

    let to_find = "https://gitlab.com/tglman/structsy".to_string();
    let mut iter = db.search(to_find.clone())?.into_iter();
    let data = iter.next().unwrap();
    assert_eq!(data.address, to_find);

    Ok(())
}
