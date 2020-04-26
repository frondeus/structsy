use structsy::{EmbeddedFilter, EmbeddedFilterBuilder, FilterBuilder, Ref};
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
    test_option_vec: Option<Vec<u8>>,
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
    //let st = Structsy::open("abc").expect("structsy_can_open");
    let mut bilder = FilterBuilder::<PersistentData>::new();
    PersistentData::field_test_string_string(&mut bilder, String::from("aaa"));
    PersistentData::field_test_string_str(&mut bilder, "aaa");
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
    PersistentData::field_test_vec_vec_u8(&mut bilder, Vec::<u8>::new());
    PersistentData::field_test_vec_u8(&mut bilder, 1u8);
    PersistentData::field_test_option_u8(&mut bilder, 1u8);
    PersistentData::field_test_option_option_u8(&mut bilder, Some(1u8));
    PersistentData::field_test_ref_ref_refereddata(
        &mut bilder,
        "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap(),
    );
    // PersistentData::field_test_option_vec_option(&mut bilder, Some(Vec::<u8>::new()));

    PersistentData::field_test_vec_bool_vec_bool(&mut bilder, Vec::<bool>::new());
    PersistentData::field_test_vec_bool_bool(&mut bilder, true);

    PersistentData::field_test_string_string_range(&mut bilder, String::from("aaa")..String::from("b"));
    PersistentData::field_test_u8_u8_range(&mut bilder, 1u8..2u8);
    PersistentData::field_test_u16_u16_range(&mut bilder, 1u16..2u16);
    PersistentData::field_test_u32_u32_range(&mut bilder, 1u32..2u32);
    PersistentData::field_test_u64_u64_range(&mut bilder, 1u64..2u64);
    PersistentData::field_test_u128_u128_range(&mut bilder, 1u128..2u128);
    PersistentData::field_test_i8_i8_range(&mut bilder, 1i8..2i8);
    PersistentData::field_test_i16_i16_range(&mut bilder, 1i16..2i16);
    PersistentData::field_test_i32_i32_range(&mut bilder, 1i32..2i32);
    PersistentData::field_test_i64_i64_range(&mut bilder, 1i64..2i64);
    PersistentData::field_test_i128_i128_range(&mut bilder, 1i128..2i128);
    PersistentData::field_test_f32_f32_range(&mut bilder, 1.0f32..2.0f32);
    PersistentData::field_test_f64_f64_range(&mut bilder, 1.0f64..2.0f64);
    PersistentData::field_test_f64_f64_range(&mut bilder, 1.0f64..2.0);
    PersistentData::field_test_vec_vec_u8_range(&mut bilder, Vec::<u8>::new()..Vec::<u8>::new());
    PersistentData::field_test_vec_u8_range(&mut bilder, 1u8..2u8);
    PersistentData::field_test_option_u8_range(&mut bilder, 1u8..2u8);
    PersistentData::field_test_option_option_u8_range(&mut bilder, Some(1u8)..Some(2u8));
    let first = "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap();
    let second = "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap();
    PersistentData::field_test_ref_ref_refereddata_range(&mut bilder, first..second);
    PersistentData::field_test_embedded_embeddedfilter_embeddeddata(&mut bilder, EmbeddedFilter::<EmbeddedData>::new());
    PersistentData::field_test_option_string_option_string(&mut bilder, Some(String::from("aaa")));
    PersistentData::field_test_option_string_string(&mut bilder, String::from("aaa"));
    PersistentData::field_test_option_string_str(&mut bilder, "aaa");
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
    test_option_vec: Option<Vec<u8>>,
    test_vec_bool: Vec<bool>,
    test_other_embedded: OtherEmbedded,
}

#[derive(PersistentEmbedded)]
pub struct OtherEmbedded {}

#[test]
fn test_embeddd_condition_filter_builder() {
    //let st = Structsy::open("abc").expect("structsy_can_open");
    let mut bilder = EmbeddedFilterBuilder::<EmbeddedData>::new();
    EmbeddedData::field_test_string_string(&mut bilder, String::from("aaa"));
    EmbeddedData::field_test_string_str(&mut bilder, "aaa");
    EmbeddedData::field_test_u8_u8(&mut bilder, 1u8);
    EmbeddedData::field_test_u16_u16(&mut bilder, 1u16);
    EmbeddedData::field_test_u32_u32(&mut bilder, 1u32);
    EmbeddedData::field_test_u64_u64(&mut bilder, 1u64);
    EmbeddedData::field_test_u128_u128(&mut bilder, 1u128);
    EmbeddedData::field_test_i8_i8(&mut bilder, 1i8);
    EmbeddedData::field_test_i16_i16(&mut bilder, 1i16);
    EmbeddedData::field_test_i32_i32(&mut bilder, 1i32);
    EmbeddedData::field_test_i64_i64(&mut bilder, 1i64);
    EmbeddedData::field_test_i128_i128(&mut bilder, 1i128);
    EmbeddedData::field_test_f32_f32(&mut bilder, 1.0f32);
    EmbeddedData::field_test_f64_f64(&mut bilder, 1.0f64);
    EmbeddedData::field_test_f64_f64(&mut bilder, 1.0f64);
    EmbeddedData::field_test_bool_bool(&mut bilder, true);
    EmbeddedData::field_test_vec_vec_u8(&mut bilder, Vec::<u8>::new());
    EmbeddedData::field_test_vec_u8(&mut bilder, 1u8);
    EmbeddedData::field_test_option_u8(&mut bilder, 1u8);
    EmbeddedData::field_test_option_option_u8(&mut bilder, Some(1u8));
    EmbeddedData::field_test_ref_ref_refereddata(
        &mut bilder,
        "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap(),
    );
    // EmbeddedData::field_test_option_vec_option(&mut bilder, Some(Vec::<u8>::new()));

    EmbeddedData::field_test_vec_bool_vec_bool(&mut bilder, Vec::<bool>::new());
    EmbeddedData::field_test_vec_bool_bool(&mut bilder, true);

    EmbeddedData::field_test_string_string_range(&mut bilder, String::from("aaa")..String::from("b"));
    EmbeddedData::field_test_u8_u8_range(&mut bilder, 1u8..2u8);
    EmbeddedData::field_test_u16_u16_range(&mut bilder, 1u16..2u16);
    EmbeddedData::field_test_u32_u32_range(&mut bilder, 1u32..2u32);
    EmbeddedData::field_test_u64_u64_range(&mut bilder, 1u64..2u64);
    EmbeddedData::field_test_u128_u128_range(&mut bilder, 1u128..2u128);
    EmbeddedData::field_test_i8_i8_range(&mut bilder, 1i8..2i8);
    EmbeddedData::field_test_i16_i16_range(&mut bilder, 1i16..2i16);
    EmbeddedData::field_test_i32_i32_range(&mut bilder, 1i32..2i32);
    EmbeddedData::field_test_i64_i64_range(&mut bilder, 1i64..2i64);
    EmbeddedData::field_test_i128_i128_range(&mut bilder, 1i128..2i128);
    EmbeddedData::field_test_f32_f32_range(&mut bilder, 1.0f32..2.0f32);
    EmbeddedData::field_test_f64_f64_range(&mut bilder, 1.0f64..2.0f64);
    EmbeddedData::field_test_f64_f64_range(&mut bilder, 1.0f64..2.0);
    EmbeddedData::field_test_vec_vec_u8_range(&mut bilder, Vec::<u8>::new()..Vec::<u8>::new());
    EmbeddedData::field_test_vec_u8_range(&mut bilder, 1u8..2u8);
    EmbeddedData::field_test_option_u8_range(&mut bilder, 1u8..2u8);
    EmbeddedData::field_test_option_option_u8_range(&mut bilder, Some(1u8)..Some(2u8));
    let first = "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap();
    let second = "ReferedData@s0c5a58".parse::<Ref<ReferedData>>().unwrap();
    EmbeddedData::field_test_ref_ref_refereddata_range(&mut bilder, first..second);
    EmbeddedData::field_test_other_embedded_embeddedfilter_otherembedded(
        &mut bilder,
        EmbeddedFilter::<OtherEmbedded>::new(),
    );
}
