use crate::{Persistent, Ref};

pub trait Projection<T> {
    fn projection(source: &T) -> Self;
}

macro_rules! projections {
    ($($t:ty),+) => {
        $(
        impl Projection<$t> for $t {
            fn projection(source:&$t) -> Self {
                *source
            }
        }
        )+
    }
}

projections!(bool, u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

impl Projection<String> for String {
    fn projection(source: &String) -> Self {
        source.clone()
    }
}
impl<T: Persistent> Projection<Ref<T>> for Ref<T> {
    fn projection(source: &Ref<T>) -> Self {
        source.clone()
    }
}
impl<T: Projection<T>> Projection<Vec<T>> for Vec<T> {
    fn projection(source: &Vec<T>) -> Self {
        source.iter().map(Projection::projection).collect()
    }
}

impl<T: Projection<T>> Projection<Option<T>> for Option<T> {
    fn projection(source: &Option<T>) -> Self {
        source.as_ref().map(Projection::projection)
    }
}
