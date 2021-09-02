use structsy::{Fetch, Filter, Operators, Structsy, StructsyError, StructsyTx};
use structsy_derive::{queries, Persistent};

#[derive(Persistent, Debug, PartialEq)]
struct MyData {
    #[index(mode = "cluster")]
    name: String,
    address: String,
}

#[queries(MyData)]
trait MyDataQuery {
    fn search(self, address: String) -> Self;
    fn search_name_and_address(self, name: &str, address: &str) -> Self;
}

fn main() -> Result<(), StructsyError> {
    let db = Structsy::open("example_advanced_query.db")?;
    db.define::<MyData>()?;

    let my_data = MyData {
        name: "Structsy".to_string(),
        address: "https://gitlab.com/tglman/structsy".to_string(),
    };
    let other_data = MyData {
        name: "Persy".to_string(),
        address: "https://gitlab.com/tglman/persy".to_string(),
    };
    let mut tx = db.begin()?;
    let _id = tx.insert(&my_data)?;
    let _id = tx.insert(&other_data)?;
    tx.commit()?;

    let to_find = "https://gitlab.com/tglman/structsy".to_string();
    let mut iter = Filter::<MyData>::new().search(to_find.clone()).fetch(&db);
    let (_id, data) = iter.next().unwrap();
    assert_eq!(data.address, to_find);
    let mut iter = db
        .query::<MyData>()
        .search_name_and_address("Structsy", &to_find)
        .into_iter();
    let (_id, data) = iter.next().unwrap();
    assert_eq!(data.address, to_find);
    let mut iter = db
        .query::<MyData>()
        .or(move |or| {
            // This as today is adding all the conditions in OR, it may change in future
            or.search_name_and_address("Structsy", "https://gitlab.com/tglman/structsy")
                .search_name_and_address("Persy", "https://gitlab.com/tglman/persy")
        })
        .into_iter();
    let (_id, data) = iter.next().unwrap();
    assert_eq!(data.address, to_find);
    let (_id, data) = iter.next().unwrap();
    assert_eq!(data.address, "https://gitlab.com/tglman/persy");
    Ok(())
}
