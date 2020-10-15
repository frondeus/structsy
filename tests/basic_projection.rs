use structsy_derive::{Persistent, Projection};

#[derive(Persistent)]
struct Laptop {
    height: u32,
    width: u32,
}

#[derive(Projection)]
#[projection = "Laptop"]
struct WidthProjection {
    width: u32,
}

#[test]
fn simple_mapping() {
    use structsy::internal::Projection;
    let lap = Laptop { height: 10, width: 10 };

    let prj = WidthProjection::projection(&lap);
    assert_eq!(prj.width, lap.width);
}
