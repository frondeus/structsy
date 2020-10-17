use structsy_derive::{Persistent, PersistentEmbedded, Projection};

#[derive(Persistent)]
struct Laptop {
    height: u32,
    width: u32,
    brand: String,
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
    let lap = Laptop {
        height: 10,
        width: 10,
        brand: "Own".to_string(),
    };

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
