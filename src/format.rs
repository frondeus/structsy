use crate::{Persistent, Ref, TRes};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::marker::PhantomData;
pub trait PeristentEmbedded {
    fn write(&self, write: &mut Write) -> TRes<()>;
    fn read(read: &mut Read) -> TRes<Self>
    where
        Self: Sized;
}

impl PeristentEmbedded for u8 {
    fn write(&self, write: &mut Write) -> TRes<()> {
        WriteBytesExt::write_u8(write, *self)?;
        Ok(())
    }
    fn read(read: &mut Read) -> TRes<u8> {
        Ok(ReadBytesExt::read_u8(read)?)
    }
}
impl PeristentEmbedded for i8 {
    fn write(&self, write: &mut Write) -> TRes<()> {
        WriteBytesExt::write_i8(write, *self)?;
        Ok(())
    }
    fn read(read: &mut Read) -> TRes<i8> {
        Ok(ReadBytesExt::read_i8(read)?)
    }
}
impl PeristentEmbedded for bool {
    fn write(&self, write: &mut Write) -> TRes<()> {
        if *self {
            WriteBytesExt::write_u8(write, 1)?;
        } else {
            WriteBytesExt::write_u8(write, 0)?;
        }
        Ok(())
    }
    fn read(read: &mut Read) -> TRes<bool> {
        Ok(ReadBytesExt::read_u8(read)? == 1)
    }
}

impl PeristentEmbedded for String {
    fn write(&self, write: &mut Write) -> TRes<()> {
        let b = self.as_bytes();
        WriteBytesExt::write_u32::<BigEndian>(write, b.len() as u32)?;
        write.write_all(b)?;
        Ok(())
    }
    fn read(read: &mut Read) -> TRes<String> {
        let size = ReadBytesExt::read_u32::<BigEndian>(read)? as u64;
        let mut s = String::new();
        read.take(size).read_to_string(&mut s)?;
        Ok(s)
    }
}

impl<T: PeristentEmbedded> PeristentEmbedded for Option<T> {
    fn write(&self, write: &mut Write) -> TRes<()> {
        if let Some(to_write) = self {
            WriteBytesExt::write_u8(write, 1)?;
            to_write.write(write)?;
        } else {
            WriteBytesExt::write_u8(write, 0)?;
        }
        Ok(())
    }
    fn read(read: &mut Read) -> TRes<Option<T>> {
        if ReadBytesExt::read_u8(read)? == 1 {
            Ok(Some(T::read(read)?))
        } else {
            Ok(None)
        }
    }
}

impl<T: PeristentEmbedded> PeristentEmbedded for Vec<T> {
    fn write(&self, write: &mut Write) -> TRes<()> {
        WriteBytesExt::write_u32::<BigEndian>(write, self.len() as u32)?;
        for v in self {
            T::write(v, write)?;
        }
        Ok(())
    }
    fn read(read: &mut Read) -> TRes<Vec<T>> {
        let len = ReadBytesExt::read_u32::<BigEndian>(read)?;
        let mut v = Vec::new();
        for _ in 0..len {
            v.push(T::read(read)?);
        }
        Ok(v)
    }
}
impl<T: Persistent> PeristentEmbedded for Ref<T> {
    fn write(&self, write: &mut Write) -> TRes<()> {
        format!("{}", self.raw_id).write(write)?;
        Ok(())
    }

    fn read(read: &mut Read) -> TRes<Ref<T>> {
        let s_id = String::read(read)?;
        Ok(Ref {
            type_name: T::get_description().name.clone(),
            raw_id: s_id.parse()?,
            ph: PhantomData,
        })
    }
}

macro_rules! impl_persist_emp {
    ($t:ident,$w:ident,$r:ident) => {
        impl PeristentEmbedded for $t {
            fn write(&self, write: &mut Write) -> TRes<()> {
                WriteBytesExt::$w::<BigEndian>(write, *self)?;
                Ok(())
            }
            fn read(read: &mut Read) -> TRes<$t> {
                Ok(ReadBytesExt::$r::<BigEndian>(read)?)
            }
        }
    };
}
impl_persist_emp!(u16, write_u16, read_u16);
impl_persist_emp!(u32, write_u32, read_u32);
impl_persist_emp!(u64, write_u64, read_u64);
impl_persist_emp!(u128, write_u128, read_u128);
impl_persist_emp!(i16, write_i16, read_i16);
impl_persist_emp!(i32, write_i32, read_i32);
impl_persist_emp!(i64, write_i64, read_i64);
impl_persist_emp!(i128, write_i128, read_i128);
impl_persist_emp!(f32, write_f32, read_f32);
impl_persist_emp!(f64, write_f64, read_f64);
