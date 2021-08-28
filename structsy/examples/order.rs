use structsy::{Filter, IntoResult, Order, Structsy, StructsyError, StructsyTx};
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
    fn order_by_name(self, name: Order) -> Self;
}

fn main() -> Result<(), StructsyError> {
    // Delete the file if run multiple times, otherwise the asserts will fail
    let db = Structsy::open("order.db")?;
    db.define::<MyData>()?;

    let my_data = MyData {
        name: "Structsy".to_string(),
        address: "http://tglman.com".to_string(),
    };
    let other_data = MyData {
        name: "Persy".to_string(),
        address: "http://tglman.com".to_string(),
    };
    let mut tx = db.begin()?;
    let _id = tx.insert(&my_data)?;
    let _id = tx.insert(&other_data)?;
    tx.commit()?;

    let to_find = "http://tglman.com".to_string();
    let mut iter = Filter::<MyData>::new()
        .search(to_find.clone())
        .order_by_name(Order::Asc)
        .get_results(&db);
    assert_eq!("Persy", iter.next().unwrap().1.name);
    assert_eq!("Structsy", iter.next().unwrap().1.name);

    let mut iter = Filter::<MyData>::new()
        .search(to_find.clone())
        .order_by_name(Order::Desc)
        .get_results(&db);
    assert_eq!("Structsy", iter.next().unwrap().1.name);
    assert_eq!("Persy", iter.next().unwrap().1.name);
    Ok(())
}
