use structsy::{Filter, SRes, Structsy, StructsyTx};
use structsy_derive::{Persistent, PersistentEmbedded, Projection};
use tempfile::tempdir;

#[derive(Persistent)]
struct Laptop {
    height: u32,
    width: u32,
    brand: String,
}

impl Laptop {
    fn new(height: u32, width: u32, brand: &str) -> Laptop {
        Laptop {
            height,
            width,
            brand: brand.to_string(),
        }
    }
}

#[derive(Projection)]
#[projection = "Laptop"]
struct WidthProjection {
    width: u32,
    brand: String,
}

#[test]
fn simple_mapping() {
    use structsy::internal::Projection;
    let lap = Laptop::new(10, 10, "Own");

    let prj = WidthProjection::projection(&lap);
    assert_eq!(prj.width, lap.width);
    assert_eq!(prj.brand, lap.brand);
}

#[derive(Persistent)]
struct Parent {
    field: String,
    emb: Embedded,
}
#[derive(PersistentEmbedded)]
struct Embedded {
    name: String,
}

#[derive(Projection)]
#[projection = "Parent"]
struct ParentProjection {
    emb: EmbeddedProjection,
}

#[derive(Projection)]
#[projection = "Embedded"]
struct EmbeddedProjection {
    name: String,
}

#[test]
fn embedded_mapping() {
    use structsy::internal::Projection;
    let parent = Parent {
        field: "One".to_string(),
        emb: Embedded {
            name: "two".to_string(),
        },
    };

    let prj = ParentProjection::projection(&parent);
    assert_eq!(prj.emb.name, parent.emb.name);
}

fn structsy_inst(name: &str, test: fn(db: &Structsy) -> SRes<()>) {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join(format!("{}.stry", name));

    let db = Structsy::open(&file).expect("can open just create");
    test(&db).expect("test is fine");
}

#[test]
fn query_projection() {
    structsy_inst("projection.test", |db| {
        db.define::<Laptop>()?;
        let mut tx = db.begin()?;
        tx.insert(&Laptop::new(10, 10, "Own"))?;
        tx.commit()?;
        let mut res = db.query::<Laptop>().projection::<WidthProjection>().into_iter();
        assert_eq!(10, res.next().unwrap().width);
        Ok(())
    });
}

#[test]
fn filter_projection() {
    structsy_inst("projection.test", |db| {
        db.define::<Laptop>()?;
        let mut tx = db.begin()?;
        tx.insert(&Laptop::new(10, 10, "Own"))?;
        tx.commit()?;
        let mut res = db.into_iter(Filter::<Laptop>::new().projection::<WidthProjection>());
        assert_eq!(10, res.next().unwrap().width);
        Ok(())
    });
}

#[test]
fn query_projection_tx() {
    structsy_inst("projection.test", |db| {
        db.define::<Laptop>()?;
        let mut tx = db.begin()?;
        tx.insert(&Laptop::new(10, 10, "Own"))?;
        {
            let mut res = tx.query::<Laptop>().projection::<WidthProjection>().into_iter();
            assert_eq!(10, res.next().unwrap().width);
        }
        tx.commit()?;
        Ok(())
    });
}

#[test]
fn filter_projection_tx() {
    structsy_inst("projection.test", |db| {
        db.define::<Laptop>()?;
        let mut tx = db.begin()?;
        tx.insert(&Laptop::new(10, 10, "Own"))?;
        {
            let mut res = tx.into_iter(Filter::<Laptop>::new().projection::<WidthProjection>());
            assert_eq!(10, res.next().unwrap().width);
        }
        tx.commit()?;
        Ok(())
    });
}
