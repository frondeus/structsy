use structsy::{FilterBuilder, Ref};
use structsy_derive::Persistent;
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
    test_option_vec: Option<Vec<u8>>,
    test_vec_bool: Vec<bool>,
}

#[derive(Persistent)]
struct ReferedData {
    name: String,
}

#[test]
fn test_condition_filter_builder() {
    //let st = Structsy::open("abc").expect("structsy_can_open");
    let mut bilder = FilterBuilder::<PersistentData>::new();
    PersistentData::field_test_string_string(&mut bilder, String::from("aaa"));
    PersistentData::field_test_u8_u8(&mut bilder, 1u8);
    PersistentData::field_test_u16_u16(&mut bilder, 1u16);
    PersistentData::field_test_u32_u32(&mut bilder, 1u32);
    PersistentData::field_test_u64_u64(&mut bilder, 1u64);
    PersistentData::field_test_u128_u128(&mut bilder, 1u128);
    PersistentData::field_test_i8_i8(&mut bilder, 1i8);
    PersistentData::field_test_i16_i16(&mut bilder, 1i16);
    PersistentData::field_test_i32_i32(&mut bilder, 1i32);
    PersistentData::field_test_i64_i64(&mut bilder, 1i64);
    PersistentData::field_test_i128_i128(&mut bilder, 1i128);
    PersistentData::field_test_f32_f32(&mut bilder, 1.0f32);
    PersistentData::field_test_f64_f64(&mut bilder, 1.0f64);
    PersistentData::field_test_f64_f64(&mut bilder, 1.0f64);
    PersistentData::field_test_bool_bool(&mut bilder, true);
    PersistentData::field_test_vec_vec(&mut bilder, Vec::<u8>::new());
    PersistentData::field_test_vec_u8(&mut bilder, 1u8);
    PersistentData::field_test_option_u8(&mut bilder, 1u8);
    PersistentData::field_test_option_option(&mut bilder, Some(1u8));
    PersistentData::field_test_ref_ref(&mut bilder, "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap());
    // PersistentData::field_test_option_vec_option(&mut bilder, Some(Vec::<u8>::new()));

    PersistentData::field_test_vec_bool_vec(&mut bilder, Vec::<bool>::new());
    PersistentData::field_test_vec_bool_bool(&mut bilder, true);
}
