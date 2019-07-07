use structsy::Ref;
use structsy_derive::Persistent;

#[derive(Persistent)]
struct One {
    #[index(mode = "exclusive")]
    first: String,
    #[index(mode = "cluster")]
    second: u8,
    third: Option<u8>,
    forth: Vec<String>,
    fifth: Option<Vec<String>>,
    sixth: Ref<Two>,
}

#[derive(Persistent)]
struct Two {
    first: String,
}

#[test()]
fn nothing() {}
