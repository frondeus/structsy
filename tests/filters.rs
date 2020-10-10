use structsy::{
    internal::{EmbeddedFilterBuilder, EqualAction, FilterBuilder, RangeAction},
    EmbeddedFilter, Ref,
};
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
    //    test_option_vec: Option<Vec<u8>>,
    test_vec_bool: Vec<bool>,
    test_embedded: EmbeddedData,
    test_option_string: Option<String>,
}

#[derive(Persistent)]
struct ReferedData {
    name: String,
}

#[test]
fn test_condition_filter_builder() {
    let mut builder = FilterBuilder::<PersistentData>::new();
    EqualAction::equal((PersistentData::field_test_string(), &mut builder), String::from("aaa"));
    EqualAction::equal((PersistentData::field_test_string(), &mut builder), "aaa");
    EqualAction::equal((PersistentData::field_test_u8(), &mut builder), 1u8);
    EqualAction::equal((PersistentData::field_test_u16(), &mut builder), 1u16);
    EqualAction::equal((PersistentData::field_test_u32(), &mut builder), 1u32);
    EqualAction::equal((PersistentData::field_test_u64(), &mut builder), 1u64);
    EqualAction::equal((PersistentData::field_test_u128(), &mut builder), 1u128);
    EqualAction::equal((PersistentData::field_test_i8(), &mut builder), 1i8);
    EqualAction::equal((PersistentData::field_test_i16(), &mut builder), 1i16);
    EqualAction::equal((PersistentData::field_test_i32(), &mut builder), 1i32);
    EqualAction::equal((PersistentData::field_test_i64(), &mut builder), 1i64);
    EqualAction::equal((PersistentData::field_test_i128(), &mut builder), 1i128);
    EqualAction::equal((PersistentData::field_test_f32(), &mut builder), 1f32);
    EqualAction::equal((PersistentData::field_test_f64(), &mut builder), 1f64);
    EqualAction::equal((PersistentData::field_test_bool(), &mut builder), true);
    EqualAction::equal((PersistentData::field_test_vec(), &mut builder), Vec::<u8>::new());
    EqualAction::equal((PersistentData::field_test_vec(), &mut builder), 1u8);
    EqualAction::equal((PersistentData::field_test_option(), &mut builder), Some(1u8));
    EqualAction::equal((PersistentData::field_test_option(), &mut builder), 1u8);
    EqualAction::equal(
        (PersistentData::field_test_ref(), &mut builder),
        "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap(),
    );
    // PersistentData::field_test_option_vec_option(&mut bilder, Some(Vec::<u8>::new()));
    EqualAction::equal(
        (PersistentData::field_test_vec_bool(), &mut builder),
        Vec::<bool>::new(),
    );
    EqualAction::equal((PersistentData::field_test_vec_bool(), &mut builder), true);

    RangeAction::range(
        (PersistentData::field_test_string(), &mut builder),
        String::from("aaa")..String::from("b"),
    );
    RangeAction::range((PersistentData::field_test_string(), &mut builder), "aaa".."b");
    RangeAction::range((PersistentData::field_test_u8(), &mut builder), 1u8..2u8);
    RangeAction::range((PersistentData::field_test_u16(), &mut builder), 1u16..2u16);
    RangeAction::range((PersistentData::field_test_u32(), &mut builder), 1u32..2u32);
    RangeAction::range((PersistentData::field_test_u64(), &mut builder), 1u64..2u64);
    RangeAction::range((PersistentData::field_test_u128(), &mut builder), 1u128..2u128);
    RangeAction::range((PersistentData::field_test_i8(), &mut builder), 1i8..2i8);
    RangeAction::range((PersistentData::field_test_i16(), &mut builder), 1i16..2i16);
    RangeAction::range((PersistentData::field_test_i32(), &mut builder), 1i32..2i32);
    RangeAction::range((PersistentData::field_test_i64(), &mut builder), 1i64..2i64);
    RangeAction::range((PersistentData::field_test_i128(), &mut builder), 1i128..2i128);
    RangeAction::range((PersistentData::field_test_f32(), &mut builder), 1f32..2f32);
    RangeAction::range((PersistentData::field_test_f64(), &mut builder), 1f64..2f64);
    RangeAction::range(
        (PersistentData::field_test_vec(), &mut builder),
        Vec::<u8>::new()..Vec::<u8>::new(),
    );
    RangeAction::range((PersistentData::field_test_vec(), &mut builder), 1u8..2u8);
    RangeAction::range(
        (PersistentData::field_test_option(), &mut builder),
        Some(1u8)..Some(2u8),
    );
    RangeAction::range((PersistentData::field_test_option(), &mut builder), 1u8..2u8);
    let first = "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap();
    let second = "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap();
    RangeAction::range((PersistentData::field_test_ref(), &mut builder), first..second);

    EqualAction::equal(
        (PersistentData::field_test_embedded(), &mut builder),
        EmbeddedFilter::<EmbeddedData>::new(),
    );
    EqualAction::equal(
        (PersistentData::field_test_option_string(), &mut builder),
        Some(String::from("aaa")),
    );
    EqualAction::equal(
        (PersistentData::field_test_option_string(), &mut builder),
        String::from("aaa"),
    );
    EqualAction::equal((PersistentData::field_test_option_string(), &mut builder), "aaa");
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
    //test_option_vec: Option<Vec<u8>>,
    test_vec_bool: Vec<bool>,
    test_other_embedded: OtherEmbedded,
}

#[derive(PersistentEmbedded)]
pub struct OtherEmbedded {}

#[test]
fn test_embeddd_condition_filter_builder() {
    //let st = Structsy::open("abc").expect("structsy_can_open");
    let mut builder = EmbeddedFilterBuilder::<EmbeddedData>::new();
    EqualAction::equal((EmbeddedData::field_test_string(), &mut builder), String::from("aaa"));
    EqualAction::equal((EmbeddedData::field_test_string(), &mut builder), "aaa");
    EqualAction::equal((EmbeddedData::field_test_u8(), &mut builder), 1u8);
    EqualAction::equal((EmbeddedData::field_test_u16(), &mut builder), 1u16);
    EqualAction::equal((EmbeddedData::field_test_u32(), &mut builder), 1u32);
    EqualAction::equal((EmbeddedData::field_test_u64(), &mut builder), 1u64);
    EqualAction::equal((EmbeddedData::field_test_u128(), &mut builder), 1u128);
    EqualAction::equal((EmbeddedData::field_test_i8(), &mut builder), 1i8);
    EqualAction::equal((EmbeddedData::field_test_i16(), &mut builder), 1i16);
    EqualAction::equal((EmbeddedData::field_test_i32(), &mut builder), 1i32);
    EqualAction::equal((EmbeddedData::field_test_i64(), &mut builder), 1i64);
    EqualAction::equal((EmbeddedData::field_test_i128(), &mut builder), 1i128);
    EqualAction::equal((EmbeddedData::field_test_f32(), &mut builder), 1f32);
    EqualAction::equal((EmbeddedData::field_test_f64(), &mut builder), 1f64);
    EqualAction::equal((EmbeddedData::field_test_bool(), &mut builder), true);
    EqualAction::equal((EmbeddedData::field_test_vec(), &mut builder), Vec::<u8>::new());
    EqualAction::equal((EmbeddedData::field_test_vec(), &mut builder), 1u8);
    EqualAction::equal((EmbeddedData::field_test_option(), &mut builder), Some(1u8));
    EqualAction::equal((EmbeddedData::field_test_option(), &mut builder), 1u8);
    EqualAction::equal(
        (EmbeddedData::field_test_ref(), &mut builder),
        "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap(),
    );
    // EmbeddedData::field_test_option_vec_option(&mut bilder, Some(Vec::<u8>::new()));
    EqualAction::equal((EmbeddedData::field_test_vec_bool(), &mut builder), Vec::<bool>::new());
    EqualAction::equal((EmbeddedData::field_test_vec_bool(), &mut builder), true);

    RangeAction::range(
        (EmbeddedData::field_test_string(), &mut builder),
        String::from("aaa")..String::from("b"),
    );
    RangeAction::range((EmbeddedData::field_test_string(), &mut builder), "aaa".."b");
    RangeAction::range((EmbeddedData::field_test_u8(), &mut builder), 1u8..2u8);
    RangeAction::range((EmbeddedData::field_test_u16(), &mut builder), 1u16..2u16);
    RangeAction::range((EmbeddedData::field_test_u32(), &mut builder), 1u32..2u32);
    RangeAction::range((EmbeddedData::field_test_u64(), &mut builder), 1u64..2u64);
    RangeAction::range((EmbeddedData::field_test_u128(), &mut builder), 1u128..2u128);
    RangeAction::range((EmbeddedData::field_test_i8(), &mut builder), 1i8..2i8);
    RangeAction::range((EmbeddedData::field_test_i16(), &mut builder), 1i16..2i16);
    RangeAction::range((EmbeddedData::field_test_i32(), &mut builder), 1i32..2i32);
    RangeAction::range((EmbeddedData::field_test_i64(), &mut builder), 1i64..2i64);
    RangeAction::range((EmbeddedData::field_test_i128(), &mut builder), 1i128..2i128);
    RangeAction::range((EmbeddedData::field_test_f32(), &mut builder), 1f32..2f32);
    RangeAction::range((EmbeddedData::field_test_f64(), &mut builder), 1f64..2f64);
    RangeAction::range(
        (EmbeddedData::field_test_vec(), &mut builder),
        Vec::<u8>::new()..Vec::<u8>::new(),
    );
    RangeAction::range((EmbeddedData::field_test_vec(), &mut builder), 1u8..2u8);
    RangeAction::range((EmbeddedData::field_test_option(), &mut builder), Some(1u8)..Some(2u8));
    RangeAction::range((EmbeddedData::field_test_option(), &mut builder), 1u8..2u8);
    let first = "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap();
    let second = "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap();
    RangeAction::range((EmbeddedData::field_test_ref(), &mut builder), first..second);

    EqualAction::equal(
        (EmbeddedData::field_test_other_embedded(), &mut builder),
        EmbeddedFilter::<OtherEmbedded>::new(),
    );
    // EmbeddedData::field_test_option_vec_option(&mut bilder, Some(Vec::<u8>::new()));
}
