#[macro_use]
extern crate tsdb_derive;

#[derive(Persistent)]
struct One {
    first: String,
    second: u8,
}

#[test()]
fn nothing() {}
