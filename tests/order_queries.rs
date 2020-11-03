use structsy::{Order, SRes, Structsy, StructsyTx};
use structsy_derive::{queries, Persistent};
use tempfile::tempdir;

#[derive(Persistent)]
struct Basic {
    name: String,
}

impl Basic {
    fn new(name: &str) -> Basic {
        Basic { name: name.to_string() }
    }
}

#[queries(Basic)]
trait BasicQuery {
    fn order(self, name: Order) -> Self;
}

fn structsy_inst(name: &str, test: fn(db: &Structsy) -> SRes<()>) {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join(format!("{}.stry", name));

    let db = Structsy::open(&file).expect("can open just create");
    test(&db).expect("test is fine");
}
#[test]
fn basic_order() {
    structsy_inst("basic_order", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("bbb"))?;
        tx.insert(&Basic::new("aaa"))?;
        tx.commit()?;
        let mut iter = db.query::<Basic>().order(Order::Asc).into_iter();
        assert_eq!(iter.next().unwrap().1.name, "aaa");
        assert_eq!(iter.next().unwrap().1.name, "bbb");
        let mut iter = db.query::<Basic>().order(Order::Desc).into_iter();
        assert_eq!(iter.next().unwrap().1.name, "bbb");
        assert_eq!(iter.next().unwrap().1.name, "aaa");
        Ok(())
    });
}
