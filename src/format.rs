use crate::{Ref, TRes};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::marker::PhantomData;

pub trait TWrite: Write {
    fn write_u8(&mut self, val: u8) -> TRes<()> {
        WriteBytesExt::write_u8(self, val)?;
        Ok(())
    }
    fn write_u16(&mut self, val: u16) -> TRes<()> {
        WriteBytesExt::write_u16::<BigEndian>(self, val)?;
        Ok(())
    }
    fn write_u32(&mut self, val: u32) -> TRes<()> {
        WriteBytesExt::write_u32::<BigEndian>(self, val)?;
        Ok(())
    }
    fn write_u64(&mut self, val: u64) -> TRes<()> {
        WriteBytesExt::write_u64::<BigEndian>(self, val)?;
        Ok(())
    }
    fn write_u128(&mut self, val: u128) -> TRes<()> {
        WriteBytesExt::write_u128::<BigEndian>(self, val)?;
        Ok(())
    }
    fn write_i8(&mut self, val: i8) -> TRes<()> {
        WriteBytesExt::write_i8(self, val)?;
        Ok(())
    }
    fn write_i16(&mut self, val: i16) -> TRes<()> {
        WriteBytesExt::write_i16::<BigEndian>(self, val)?;
        Ok(())
    }
    fn write_i32(&mut self, val: i32) -> TRes<()> {
        WriteBytesExt::write_i32::<BigEndian>(self, val)?;
        Ok(())
    }
    fn write_i64(&mut self, val: i64) -> TRes<()> {
        WriteBytesExt::write_i64::<BigEndian>(self, val)?;
        Ok(())
    }
    fn write_i128(&mut self, val: i128) -> TRes<()> {
        WriteBytesExt::write_i128::<BigEndian>(self, val)?;
        Ok(())
    }
    fn write_f32(&mut self, val: f32) -> TRes<()> {
        WriteBytesExt::write_f32::<BigEndian>(self, val)?;
        Ok(())
    }

    fn write_f64(&mut self, val: f64) -> TRes<()> {
        WriteBytesExt::write_f64::<BigEndian>(self, val)?;
        Ok(())
    }

    fn write_bool(&mut self, val: bool) -> TRes<()> {
        if val {
            WriteBytesExt::write_u8(self, 1)?;
        } else {
            WriteBytesExt::write_u8(self, 0)?;
        }
        Ok(())
    }

    fn write_string(&mut self, val: &str) -> TRes<()> {
        let b = val.as_bytes();
        TWrite::write_u32(self, b.len() as u32)?;
        self.write_all(b)?;
        Ok(())
    }
    /*
    fn write_ref<T>(&mut self, val: Ref<T>) -> TRes<()> {
        self.write_string(&format!("{}", val.raw_id))
    }

    fn write_option<V, F>(&mut self, val: Option<V>, writer: F) -> TRes<()>
    where
        F: Fn(&mut Self, V) -> TRes<()>,
    {
        if let Some(to_write) = val {
            self.write_bool(true)?;
            writer(self, to_write)?;
        } else {
            self.write_bool(false)?;
        }
        Ok(())
    }
    fn write_array<V, F>(&mut self, val: Vec<V>, writer: F) -> TRes<()>
    where
        F: Fn(&mut Self, V) -> TRes<()>,
    {
        TWrite::write_u32(self, val.len() as u32)?;
        for v in val {
            writer(self, v)?;
        }
        Ok(())
    }

    fn write_option_array<V, F>(&mut self, val: Option<Vec<V>>, writer: F) -> TRes<()>
    where
        F: Fn(&mut Self, V) -> TRes<()>,
    {
        if let Some(to_write) = val {
            self.write_bool(true)?;
            self.write_array(to_write, writer)?;
        } else {
            self.write_bool(false)?;
        }
        Ok(())
    }
    */
}

impl<W: Write + ?Sized> TWrite for W {}

pub trait TRead: Read {
    fn read_u8(&mut self) -> TRes<u8> {
        Ok(ReadBytesExt::read_u8(self)?)
    }
    fn read_u16(&mut self) -> TRes<u16> {
        Ok(ReadBytesExt::read_u16::<BigEndian>(self)?)
    }
    fn read_u32(&mut self) -> TRes<u32> {
        Ok(ReadBytesExt::read_u32::<BigEndian>(self)?)
    }

    fn read_u64(&mut self) -> TRes<u64> {
        Ok(ReadBytesExt::read_u64::<BigEndian>(self)?)
    }

    fn read_u128(&mut self) -> TRes<u128> {
        Ok(ReadBytesExt::read_u128::<BigEndian>(self)?)
    }

    fn read_i8(&mut self) -> TRes<i8> {
        Ok(ReadBytesExt::read_i8(self)?)
    }

    fn read_i16(&mut self) -> TRes<i16> {
        Ok(ReadBytesExt::read_i16::<BigEndian>(self)?)
    }

    fn read_i32(&mut self) -> TRes<i32> {
        Ok(ReadBytesExt::read_i32::<BigEndian>(self)?)
    }

    fn read_i64(&mut self) -> TRes<i64> {
        Ok(ReadBytesExt::read_i64::<BigEndian>(self)?)
    }

    fn read_i128(&mut self) -> TRes<i128> {
        Ok(ReadBytesExt::read_i128::<BigEndian>(self)?)
    }

    fn read_f32(&mut self) -> TRes<f32> {
        Ok(ReadBytesExt::read_f32::<BigEndian>(self)?)
    }

    fn read_f64(&mut self) -> TRes<f64> {
        Ok(ReadBytesExt::read_f64::<BigEndian>(self)?)
    }

    fn read_bool(&mut self) -> TRes<bool> {
        Ok(ReadBytesExt::read_u8(self)? == 1)
    }
    fn read_string(&mut self) -> TRes<String> {
        let size = TRead::read_u32(self)? as u64;
        let mut s = String::new();
        self.take(size).read_to_string(&mut s)?;
        Ok(s)
    }
    fn read_ref<T>(&mut self) -> TRes<Ref<T>> {
        //TODO: get type name,
        Ok(Ref {
            type_name: "".to_string(),
            raw_id: self.read_string()?.parse()?,
            ph: PhantomData,
        })
    }
    fn read_option<V, F>(&mut self, reader: F) -> TRes<Option<V>>
    where
        F: Fn(&mut Self) -> TRes<V>,
    {
        if self.read_bool()? {
            Ok(Some(reader(self)?))
        } else {
            Ok(None)
        }
    }
    fn read_array<V, F>(&mut self, reader: F) -> TRes<Vec<V>>
    where
        F: Fn(&mut Self) -> TRes<V>,
    {
        let len = TRead::read_u32(self)?;
        let mut v = Vec::new();
        for _ in 0..len {
            v.push(reader(self)?);
        }
        Ok(v)
    }
    fn read_option_array<V, F>(&mut self, reader: F) -> TRes<Option<Vec<V>>>
    where
        F: Fn(&mut Self) -> TRes<V>,
    {
        if self.read_bool()? {
            Ok(Some(TRead::read_array(self, reader)?))
        } else {
            Ok(None)
        }
    }
}

impl<R: Read + ?Sized> TRead for R {}
