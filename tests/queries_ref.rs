use std::ops::RangeBounds;
use structsy::{EmbeddedFilter, Ref, SRes, Structsy, StructsyQuery, StructsyTx};
use structsy_derive::{embedded_queries, queries, Persistent, PersistentEmbedded};
use tempfile::tempdir;
fn structsy_inst(name: &str, test: fn(db: &Structsy) -> SRes<()>) {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join(format!("{}.stry", name));

    let db = Structsy::open(&file).expect("can open just create");
    test(&db).expect("test is fine");
}

#[derive(Persistent)]
struct Basic {
    to_other: Ref<Other>,
}
impl Basic {
    fn new(to_other: Ref<Other>) -> Basic {
        Basic { to_other }
    }
}

#[derive(Persistent)]
struct Other {
    name: String,
}

impl Other {
    fn new(name: &str) -> Other {
        Other { name: name.to_string() }
    }
}

#[queries(Basic)]
trait BasicQuery {
    fn by_other(self, to_other: Ref<Other>) -> Self;
    fn by_other_range<R: RangeBounds<Ref<Other>>>(self, to_other: R) -> Self;
    fn by_other_query(self, to_other: StructsyQuery<Other>) -> Self;
}

#[queries(Other)]
trait OtherQuery {
    fn by_name(self, name: String) -> Self;
}

#[test]
fn test_ref() {
    structsy_inst("basic_query", |db| {
        db.define::<Basic>()?;
        db.define::<Other>()?;

        let mut tx = db.begin()?;
        let insa = tx.insert(&Other::new("aaa"))?;
        let insb = tx.insert(&Other::new("bbb"))?;
        let insc = tx.insert(&Other::new("ccc"))?;
        tx.insert(&Basic::new(insa.clone()))?;
        tx.insert(&Basic::new(insb.clone()))?;
        tx.insert(&Basic::new(insc))?;
        tx.commit()?;
        let count = db.query::<Basic>().by_other(insa.clone()).into_iter().count();
        assert_eq!(count, 1);

        let count = db.query::<Basic>().by_other_range(insa..=insb).into_iter().count();
        assert_eq!(count, 2);
        let other_query = db.query::<Other>().by_name("aaa".to_string());
        let count = db.query::<Basic>().by_other_query(other_query).into_iter().count();
        assert_eq!(count, 1);
        Ok(())
    });
}

#[derive(Persistent)]
struct Parent {
    emb: Emb,
}

impl Parent {
    fn new(other: Ref<Other>) -> Parent {
        Parent { emb: Emb { other } }
    }
}

#[derive(PersistentEmbedded)]
struct Emb {
    other: Ref<Other>,
}

#[queries(Parent)]
trait ParentQuery {
    fn by_emb(self, emb: EmbeddedFilter<Emb>) -> Self;
}

#[embedded_queries(Emb)]
trait EmbQuery {
    fn by_other(self, other: StructsyQuery<Other>) -> Self;
}

#[test]
fn test_embedded_ref() {
    structsy_inst("basic_query", |db| {
        db.define::<Parent>()?;
        db.define::<Other>()?;

        let mut tx = db.begin()?;
        let insa = tx.insert(&Other::new("aaa"))?;
        let insb = tx.insert(&Other::new("bbb"))?;
        let insc = tx.insert(&Other::new("ccc"))?;
        tx.insert(&Parent::new(insa.clone()))?;
        tx.insert(&Parent::new(insb.clone()))?;
        tx.insert(&Parent::new(insc))?;
        tx.commit()?;
        let other_query = db.query::<Other>().by_name("aaa".to_string());
        let emb_filter = Structsy::embedded_filter::<Emb>().by_other(other_query);
        let count = db.query::<Parent>().by_emb(emb_filter).into_iter().count();
        assert_eq!(count, 1);
        Ok(())
    });
}
