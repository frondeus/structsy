use structsy::{FieldConditionType, FilterBuilder, Ref, Structsy};
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
    //    test_bool: bool,
    test_f32: f32,
    test_f64: f64,
    test_option: Option<u8>,
    test_vec: Vec<u8>,
    test_ref: Ref<ReferedData>,
}

#[derive(Persistent)]
struct ReferedData {
    name: String,
}

#[test]
fn test_condition_filter_builder() {
    //let st = Structsy::open("abc").expect("structsy_can_open");
    let mut builder = FilterBuilder::<PersistentData>::new();
    builder.indexable_condition("test_string", String::from("aaa"), |x| &x.test_string);
    builder.indexable_condition("test_u8", 1u8, |x| &x.test_u8);
    builder.indexable_condition("test_u16", 1u16, |x| &x.test_u16);
    builder.indexable_condition("test_u32", 1u32, |x| &x.test_u32);
    builder.indexable_condition("test_u64", 1u64, |x| &x.test_u64);
    builder.indexable_condition("test_u128", 1u128, |x| &x.test_u128);
    builder.indexable_condition("test_i8", 1i8, |x| &x.test_i8);
    builder.indexable_condition("test_i16", 1i16, |x| &x.test_i16);
    builder.indexable_condition("test_i32", 1i32, |x| &x.test_i32);
    builder.indexable_condition("test_i64", 1i64, |x| &x.test_i64);
    builder.indexable_condition("test_i128", 1i128, |x| &x.test_i128);

    builder.indexable_condition("test_f32", 1.0f32, |x| &x.test_f32);
    builder.indexable_condition("test_f64", 1.0f64, |x| &x.test_f64);
    //builder.simple_condition("test_bool", true, |x| &x.test_bool);
    PersistentData::field_test_vec_vec(&mut builder, Vec::<u8>::new());
    PersistentData::field_test_vec_u8(&mut builder, 1u8);
    //builder.indexable_condition("test_vec",1u8, |x| &x.test_vec );
    // builder.indexable_condition("test_option",1u8, |x| &x.test_option );
    //builder.simple_condition("test_ref",Ref::new("__".parse().unwrap()), |x| &x.test_ref );
}
