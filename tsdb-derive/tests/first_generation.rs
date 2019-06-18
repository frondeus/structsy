use tsdb::Ref;
use tsdb_derive::Persistent;

#[derive(Persistent)]
struct One {
    #[index(exclusive)]
    first: String,
    second: u8,
    third: Option<u8>,
    forth: Vec<String>,
    fifth: Option<Vec<String>>,
    //sixth: Ref<Two>,
}

#[derive(Persistent)]
struct Two {
    first: String,
}

#[test()]
fn nothing() {}
