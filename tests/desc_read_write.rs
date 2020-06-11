use std::io::Cursor;
use structsy::{internal::StructDescription, Persistent, Ref};
use structsy_derive::{Persistent, PersistentEmbedded};

#[derive(Persistent)]
struct PersistentData {
    test_string: String,
    test_u8: u8,
    test_u16: u16,
    test_u32: u32,
    test_u64: u64,
    test_u128: u128,
    test_i8: i8,
    test_i16: i16,
    test_i32: i32,
    test_i64: i64,
    test_i128: i128,
    test_bool: bool,
    test_f32: f32,
    test_f64: f64,
    test_option: Option<u8>,
    test_vec: Vec<u8>,
    test_ref: Ref<ReferedData>,
    test_vec_bool: Vec<bool>,
    test_embedded: EmbeddedData,
    test_option_string: Option<String>,
}

#[derive(Persistent)]
struct ReferedData {
    name: String,
}

#[derive(PersistentEmbedded)]
struct EmbeddedData {
    test_string: String,
    test_u8: u8,
    test_u16: u16,
    test_u32: u32,
    test_u64: u64,
    test_u128: u128,
    test_i8: i8,
    test_i16: i16,
    test_i32: i32,
    test_i64: i64,
    test_i128: i128,
    test_bool: bool,
    test_f32: f32,
    test_f64: f64,
    test_option: Option<u8>,
    test_vec: Vec<u8>,
    test_ref: Ref<ReferedData>,
    test_vec_bool: Vec<bool>,
    test_other_embedded: OtherEmbedded,
}

#[derive(PersistentEmbedded)]
pub struct OtherEmbedded {}

#[test]
fn test_read_write_desc() {
    let desc = PersistentData::get_description();
    let mut buff = Vec::new();
    desc.write(&mut buff).unwrap();
    let read_desc = StructDescription::read(&mut Cursor::new(buff)).unwrap();
    assert_eq!(desc, read_desc);
}
