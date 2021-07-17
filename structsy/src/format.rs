use crate::{Persistent, Ref, SRes};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Base trait implemented by all types that can be persisted inside a struct.
pub trait PersistentEmbedded {
    fn write(&self, write: &mut dyn Write) -> SRes<()>;
    fn read(read: &mut dyn Read) -> SRes<Self>
    where
        Self: Sized;
}

impl PersistentEmbedded for u8 {
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        WriteBytesExt::write_u8(write, *self)?;
        Ok(())
    }
    fn read(read: &mut dyn Read) -> SRes<u8> {
        Ok(ReadBytesExt::read_u8(read)?)
    }
}
impl PersistentEmbedded for i8 {
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        WriteBytesExt::write_i8(write, *self)?;
        Ok(())
    }
    fn read(read: &mut dyn Read) -> SRes<i8> {
        Ok(ReadBytesExt::read_i8(read)?)
    }
}
impl PersistentEmbedded for bool {
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        if *self {
            WriteBytesExt::write_u8(write, 1)?;
        } else {
            WriteBytesExt::write_u8(write, 0)?;
        }
        Ok(())
    }
    fn read(read: &mut dyn Read) -> SRes<bool> {
        Ok(ReadBytesExt::read_u8(read)? == 1)
    }
}

impl PersistentEmbedded for String {
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        let b = self.as_bytes();
        WriteBytesExt::write_u32::<BigEndian>(write, b.len() as u32)?;
        write.write_all(b)?;
        Ok(())
    }
    fn read(read: &mut dyn Read) -> SRes<String> {
        let size = ReadBytesExt::read_u32::<BigEndian>(read)? as u64;
        let mut s = String::new();
        read.take(size).read_to_string(&mut s)?;
        Ok(s)
    }
}

impl<T: PersistentEmbedded> PersistentEmbedded for Option<T> {
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        if let Some(to_write) = self {
            WriteBytesExt::write_u8(write, 1)?;
            to_write.write(write)?;
        } else {
            WriteBytesExt::write_u8(write, 0)?;
        }
        Ok(())
    }
    fn read(read: &mut dyn Read) -> SRes<Option<T>> {
        if ReadBytesExt::read_u8(read)? == 1 {
            Ok(Some(T::read(read)?))
        } else {
            Ok(None)
        }
    }
}

impl<T: PersistentEmbedded> PersistentEmbedded for Vec<T> {
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        WriteBytesExt::write_u32::<BigEndian>(write, self.len() as u32)?;
        for v in self {
            T::write(v, write)?;
        }
        Ok(())
    }
    fn read(read: &mut dyn Read) -> SRes<Vec<T>> {
        let len = ReadBytesExt::read_u32::<BigEndian>(read)?;
        let mut v = Vec::new();
        for _ in 0..len {
            v.push(T::read(read)?);
        }
        Ok(v)
    }
}
impl<T: Persistent> PersistentEmbedded for Ref<T> {
    fn write(&self, write: &mut dyn Write) -> SRes<()> {
        format!("{}", self.raw_id).write(write)?;
        Ok(())
    }

    fn read(read: &mut dyn Read) -> SRes<Ref<T>> {
        let s_id = String::read(read)?;
        Ok(Ref::new(s_id.parse()?))
    }
}

macro_rules! impl_persistent_embedded {
    ($t:ident,$w:ident,$r:ident) => {
        impl PersistentEmbedded for $t {
            fn write(&self, write: &mut dyn Write) -> SRes<()> {
                WriteBytesExt::$w::<BigEndian>(write, *self)?;
                Ok(())
            }
            fn read(read: &mut dyn Read) -> SRes<$t> {
                Ok(ReadBytesExt::$r::<BigEndian>(read)?)
            }
        }
    };
}
impl_persistent_embedded!(u16, write_u16, read_u16);
impl_persistent_embedded!(u32, write_u32, read_u32);
impl_persistent_embedded!(u64, write_u64, read_u64);
impl_persistent_embedded!(u128, write_u128, read_u128);
impl_persistent_embedded!(i16, write_i16, read_i16);
impl_persistent_embedded!(i32, write_i32, read_i32);
impl_persistent_embedded!(i64, write_i64, read_i64);
impl_persistent_embedded!(i128, write_i128, read_i128);
impl_persistent_embedded!(f32, write_f32, read_f32);
impl_persistent_embedded!(f64, write_f64, read_f64);
