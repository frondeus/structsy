use structsy::{EmbeddedFilter, Order, SRes, Structsy, StructsyTx};
use structsy_derive::{embedded_queries, queries, Persistent, PersistentEmbedded};
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

#[derive(Persistent)]
struct Parent {
    emb: Embedded,
}

#[derive(PersistentEmbedded)]
struct Embedded {
    name: String,
}

impl Parent {
    fn new(name: &str) -> Parent {
        Parent {
            emb: Embedded { name: name.to_string() },
        }
    }
}

#[queries(Parent)]
trait ParentQuery {
    fn filter_emb(self, emb: EmbeddedFilter<Embedded>) -> Self;
}

#[embedded_queries(Embedded)]
trait EmbeddedQuery {
    fn order_name(self, name: Order) -> Self;
}

#[test]
fn nested_order() {
    structsy_inst("basic_order", |db| {
        db.define::<Parent>()?;
        let mut tx = db.begin()?;
        tx.insert(&Parent::new("bbb"))?;
        tx.insert(&Parent::new("aaa"))?;
        tx.commit()?;
        let mut iter = db
            .query::<Parent>()
            .filter_emb(Structsy::embedded_filter::<Embedded>().order_name(Order::Asc))
            .into_iter();
        assert_eq!(iter.next().unwrap().1.emb.name, "aaa");
        assert_eq!(iter.next().unwrap().1.emb.name, "bbb");
        let mut iter = db
            .query::<Parent>()
            .filter_emb(Structsy::embedded_filter::<Embedded>().order_name(Order::Desc))
            .into_iter();
        assert_eq!(iter.next().unwrap().1.emb.name, "bbb");
        assert_eq!(iter.next().unwrap().1.emb.name, "aaa");
        Ok(())
    });
}