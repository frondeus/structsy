use structsy::Ref;
use structsy_derive::{Persistent, PersistentEmbedded};

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
    seventh: Three,
}

#[derive(Persistent)]
struct Two {
    first: String,
}

#[derive(PersistentEmbedded)]
struct Three {
    name: String,
}

#[test]
fn nothing() {}
