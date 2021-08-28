use crate::{FilterBuilder, IntoResult, Persistent, Ref, SRes, StructsyImpl, StructsyIter, StructsyQueryTx};
use persy::Transaction;
use std::{io::Cursor, marker::PhantomData, sync::Arc};

/// Owned transation to use with [`StructsyTx`] trait
///
/// [`StructsyTx`]: trait.StructsyTx.html
pub struct OwnedSytx {
    pub(crate) structsy_impl: Arc<StructsyImpl>,
    pub(crate) trans: Transaction,
}

impl OwnedSytx {
    /// Query for a persistent struct considering in transaction changes.
    ///
    /// # Example
    /// ```
    /// use structsy::{ Structsy, StructsyTx, StructsyError};
    /// use structsy_derive::{queries, Persistent};
    /// #[derive(Persistent)]
    /// struct Basic {
    ///     name: String,
    /// }
    /// impl Basic {
    ///     fn new(name: &str) -> Basic {
    ///         Basic { name: name.to_string() }
    ///     }
    /// }
    ///
    /// #[queries(Basic)]
    /// trait BasicQuery {
    ///     fn by_name(self, name: String) -> Self;
    /// }
    ///
    ///
    /// fn basic_query() -> Result<(), StructsyError> {
    ///     let structsy = Structsy::open("file.structsy")?;
    ///     structsy.define::<Basic>()?;
    ///     let mut tx = structsy.begin()?;
    ///     tx.insert(&Basic::new("aaa"))?;
    ///     let count = tx.query::<Basic>().by_name("aaa".to_string()).into_iter().count();
    ///     assert_eq!(count, 1);
    ///     tx.commit()?;
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn query<T: Persistent>(&mut self) -> StructsyQueryTx<T> {
        StructsyQueryTx {
            tx: self,
            builder: FilterBuilder::new(),
        }
    }

    pub fn into_iter<R: IntoResult<T>, T>(&mut self, filter: R) -> StructsyIter<T> {
        filter.get_results_tx(self)
    }
    pub(crate) fn reference(&mut self) -> RefSytx {
        RefSytx {
            trans: &mut self.trans,
            structsy_impl: self.structsy_impl.clone(),
        }
    }
}

/// Reference transaction to use with [`StructsyTx`] trait
///
/// [`StructsyTx`]: trait.StructsyTx.html
pub struct RefSytx<'a> {
    pub(crate) structsy_impl: Arc<StructsyImpl>,
    pub(crate) trans: &'a mut Transaction,
}

/// Internal use transaction reference
pub struct TxRef<'a> {
    pub(crate) trans: &'a mut Transaction,
}

/// Internal use implementation reference
pub struct ImplRef {
    pub(crate) structsy_impl: Arc<StructsyImpl>,
}

pub trait Sytx {
    /// Internal Use Only
    ///
    #[doc(hidden)]
    fn tx(&mut self) -> TxRef;
    /// Internal Use Only
    ///
    #[doc(hidden)]
    fn structsy(&self) -> ImplRef;
}

impl Sytx for OwnedSytx {
    fn tx(&mut self) -> TxRef {
        TxRef { trans: &mut self.trans }
    }
    fn structsy(&self) -> ImplRef {
        ImplRef {
            structsy_impl: self.structsy_impl.clone(),
        }
    }
}
impl StructsyTx for OwnedSytx {
    fn commit(self) -> SRes<()> {
        let prepared = self.trans.prepare()?;
        prepared.commit()?;
        Ok(())
    }

    fn prepare_commit(self) -> SRes<Prepared> {
        Ok(Prepared {
            prepared: self.trans.prepare()?,
        })
    }
}

impl<'a> Sytx for RefSytx<'a> {
    fn tx(&mut self) -> TxRef {
        TxRef { trans: self.trans }
    }
    fn structsy(&self) -> ImplRef {
        ImplRef {
            structsy_impl: self.structsy_impl.clone(),
        }
    }
}
impl<'a> StructsyTx for RefSytx<'a> {
    fn commit(self) -> SRes<()> {
        unreachable!();
    }
    fn prepare_commit(self) -> SRes<Prepared> {
        unreachable!();
    }
}

///
/// Transaction prepared state ready to be committed if the second phase is considered successful
///
pub struct Prepared {
    prepared: persy::TransactionFinalize,
}
impl Prepared {
    /// Commit all the prepared changes
    pub fn commit(self) -> SRes<()> {
        self.prepared.commit()?;
        Ok(())
    }
    /// Rollback all the prepared changes
    pub fn rollback(self) -> SRes<()> {
        self.prepared.rollback()?;
        Ok(())
    }
}

/// Transaction behaviour trait.
pub trait StructsyTx: Sytx + Sized {
    /// Persist a new struct instance.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// tx.insert(&Example{value:10})?;
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn insert<T: Persistent>(&mut self, sct: &T) -> SRes<Ref<T>> {
        let def = self.structsy().structsy_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        let id = self.tx().trans.insert(def.segment_name(), &buff)?;
        let id_ref = Ref::new(id);
        sct.put_indexes(self, &id_ref)?;
        Ok(id_ref)
    }

    /// Update a persistent instance with a new value.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// let id = tx.insert(&Example{value:10})?;
    /// tx.update(&id, &Example{value:20})?;
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn update<T: Persistent>(&mut self, sref: &Ref<T>, sct: &T) -> SRes<()> {
        let def = self.structsy().structsy_impl.check_defined::<T>()?;
        let mut buff = Vec::new();
        sct.write(&mut buff)?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        self.tx().trans.update(def.segment_name(), &sref.raw_id, &buff)?;
        sct.put_indexes(self, &sref)?;
        Ok(())
    }

    /// Delete a persistent instance.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// let id = tx.insert(&Example{value:10})?;
    /// tx.delete(&id)?;
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn delete<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<()> {
        let def = self.structsy().structsy_impl.check_defined::<T>()?;
        let old = self.read::<T>(sref)?;
        if let Some(old_rec) = old {
            old_rec.remove_indexes(self, &sref)?;
        }
        self.tx().trans.delete(def.segment_name(), &sref.raw_id)?;
        Ok(())
    }

    /// Read a persistent instance considering changes in transaction.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// let id = tx.insert(&Example{value:10})?;
    /// let read = tx.read(&id)?;
    /// assert_eq!(10,read.unwrap().value);
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn read<T: Persistent>(&mut self, sref: &Ref<T>) -> SRes<Option<T>> {
        let def = self.structsy().structsy_impl.check_defined::<T>()?;
        crate::structsy::tx_read(def.segment_name(), &mut self.tx().trans, &sref.raw_id)
    }

    /// Scan persistent instances of a struct considering changes in transaction.
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// use structsy_derive::Persistent;
    /// #[derive(Persistent)]
    /// struct Example {
    ///     value:u8,
    /// }
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// # let structsy = Structsy::open("path/to/file.stry")?;
    /// //.. open structsy etc.
    /// let mut tx = structsy.begin()?;
    /// for (id, inst) in tx.scan::<Example>()? {
    ///     // logic
    /// }
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn scan<T: Persistent>(&mut self) -> SRes<TxRecordIter<T>> {
        let def = self.structsy().structsy_impl.check_defined::<T>()?;
        let implc = self.structsy().structsy_impl;
        let iter = self.tx().trans.scan(def.segment_name())?;
        Ok(TxRecordIter::new(iter, implc))
    }

    /// Commit a transaction
    ///
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// //....
    /// let mut tx = stry.begin()?;
    /// // ... operate on tx.
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn commit(self) -> SRes<()>;

    /// Prepare Commit a transaction
    ///
    ///
    /// # Example
    /// ```
    /// use structsy::{Structsy,StructsyTx};
    /// # use structsy::SRes;
    /// # fn example() -> SRes<()> {
    /// let stry = Structsy::open("path/to/file.stry")?;
    /// //....
    /// let mut tx = stry.begin()?;
    /// // ... operate on tx.
    /// let prepared = tx.prepare_commit()?;
    /// prepared.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    fn prepare_commit(self) -> SRes<Prepared>;
}

pub trait TxIterator<'a>: Iterator {
    fn tx(&mut self) -> RefSytx;
}

impl<'a, T: Persistent> TxIterator<'a> for TxRecordIter<'a, T> {
    fn tx(&mut self) -> RefSytx {
        self.tx()
    }
}

/// Iterator for record instances aware of transactions changes
pub struct TxRecordIter<'a, T> {
    iter: persy::TxSegmentIter<'a>,
    marker: PhantomData<T>,
    structsy_impl: Arc<StructsyImpl>,
}

impl<'a, T> TxRecordIter<'a, T> {
    fn new(iter: persy::TxSegmentIter<'a>, structsy_impl: Arc<StructsyImpl>) -> TxRecordIter<'a, T> {
        TxRecordIter {
            iter,
            marker: PhantomData,
            structsy_impl,
        }
    }

    pub fn tx(&mut self) -> RefSytx {
        RefSytx {
            trans: self.iter.tx(),
            structsy_impl: self.structsy_impl.clone(),
        }
    }
}

impl<'a, T: Persistent> TxRecordIter<'a, T> {
    pub fn next_tx(&mut self) -> Option<(Ref<T>, T, RefSytx)> {
        if let Some((id, buff, tx)) = self.iter.next_tx() {
            if let Ok(x) = T::read(&mut Cursor::new(buff)) {
                let stx = RefSytx {
                    trans: tx,
                    structsy_impl: self.structsy_impl.clone(),
                };
                Some((Ref::new(id), x, stx))
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<'a, T: Persistent> Iterator for TxRecordIter<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, buff)) = self.iter.next() {
            if let Ok(x) = T::read(&mut Cursor::new(buff)) {
                Some((Ref::new(id), x))
            } else {
                None
            }
        } else {
            None
        }
    }
}
