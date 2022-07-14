use crate::internal::{EmbeddedDescription, FilterDefinition};
use crate::{
    filter_builder::{EmbeddedFilterBuilder, FilterBuilder, Reader},
    projection::Projection,
    queries::ProjectionResult,
    queries::{EmbeddedQuery, Query},
    snapshot::Snapshot,
    transaction::OwnedSytx,
    Fetch, Persistent, Ref, Structsy, StructsyIter,
};
/// Generic filter for any Persistent structures
///
///
/// # Example
/// ```rust
/// use structsy::{ Structsy, StructsyTx, StructsyError, Filter};
/// use structsy_derive::{queries, embedded_queries, Persistent, PersistentEmbedded};
///
/// #[derive(Persistent)]
/// struct WithEmbedded {
///     embedded: Embedded,
/// }
///
/// #[derive(PersistentEmbedded)]
/// struct Embedded {
///     name: String,
/// }
/// impl WithEmbedded {
///     fn new(name: &str) -> WithEmbedded {
///         WithEmbedded {
///             embedded: Embedded { name: name.to_string() },
///         }
///     }
/// }
///
/// #[queries(WithEmbedded)]
/// trait WithEmbeddedQuery {
///     fn embedded(self, embedded: Filter<Embedded>) -> Self;
/// }
///
/// #[embedded_queries(Embedded)]
/// trait EmbeddedQuery {
///     fn by_name(self, name: String) -> Self;
/// }
///
/// fn main() -> Result<(), StructsyError> {
///     let structsy = Structsy::memory()?;
///     structsy.define::<WithEmbedded>()?;
///     let mut tx = structsy.begin()?;
///     tx.insert(&WithEmbedded::new("aaa"))?;
///     tx.commit()?;
///     let embedded_filter = Filter::<Embedded>::new().by_name("aaa".to_string());
///     let query = Filter::<WithEmbedded>::new().embedded(embedded_filter);
///     assert_eq!(structsy.fetch(query).count(), 1);
///     Ok(())
/// }
/// ```
pub struct Filter<T: FilterDefinition> {
    filter_builder: T::Filter,
}

impl<T: FilterDefinition> Filter<T> {
    pub fn new() -> Self {
        Filter {
            filter_builder: T::Filter::default(),
        }
    }
    pub(crate) fn extract_filter(self) -> T::Filter {
        self.filter_builder
    }
}

impl<T: Persistent> Filter<T> {
    /// Make a projection from filtered structs.
    ///
    ///
    /// # Example
    /// ```rust
    /// use structsy::{ Structsy, StructsyTx, StructsyError, Filter};
    /// use structsy_derive::{queries, Projection, Persistent};
    ///
    /// #[derive(Persistent)]
    /// struct Person {
    ///     name:String,
    ///     surname:String,
    /// }
    ///
    /// impl Person {
    ///     fn new(name:&str, surname:&str) -> Self {
    ///         Person {
    ///             name: name.to_string(),
    ///             surname: surname.to_string(),
    ///         }
    ///     }
    /// }
    ///
    /// #[queries(Person)]
    /// trait PersonQuery {
    ///     fn by_name(self, name:&str) -> Self;
    /// }
    ///
    /// #[derive(Projection)]
    /// #[projection = "Person" ]
    /// struct NameProjection {
    ///     name:String,
    /// }
    ///
    ///
    /// fn main() -> Result<(), StructsyError> {
    ///     let structsy = Structsy::memory()?;
    ///     structsy.define::<Person>()?;
    ///     let mut tx = structsy.begin()?;
    ///     tx.insert(&Person::new("a_name", "a_surname"))?;
    ///     tx.commit()?;
    ///     let query =
    ///     Filter::<Person>::new().by_name("a_name").projection::<NameProjection>();
    ///     assert_eq!(structsy.fetch(query).next().unwrap().name, "a_name");
    ///     Ok(())
    /// }
    /// ```
    pub fn projection<P: Projection<T>>(self) -> ProjectionResult<P, T> {
        ProjectionResult::new(self.filter_builder)
    }
}

impl<T: FilterDefinition> Default for Filter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Persistent + 'static> Fetch<(Ref<T>, T)> for Filter<T> {
    fn into(self, structsy: &Structsy) -> StructsyIter<(Ref<T>, T)> {
        self.fetch(structsy)
    }

    fn into_tx(self, tx: &mut OwnedSytx) -> StructsyIter<(Ref<T>, T)> {
        self.fetch_tx(tx)
    }

    fn fetch(self, structsy: &Structsy) -> StructsyIter<(Ref<T>, T)> {
        let data = self.extract_filter().finish(Reader::Structsy(structsy.clone()));
        StructsyIter::new(data)
    }

    fn fetch_tx(self, tx: &mut OwnedSytx) -> StructsyIter<(Ref<T>, T)> {
        let data = self.extract_filter().finish(Reader::Tx(tx.reference()));
        StructsyIter::new(data)
    }

    fn fetch_snapshot(self, snapshot: &Snapshot) -> StructsyIter<(Ref<T>, T)> {
        let data = self.extract_filter().finish(Reader::Snapshot(snapshot.clone()));
        StructsyIter::new(data)
    }
}

impl<T: EmbeddedDescription + FilterDefinition + 'static> EmbeddedQuery<T> for Filter<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.filter_builder
    }
    fn add_group(&mut self, filter: Filter<T>) {
        let base = self.filter_builder();
        base.and(filter.extract_filter());
    }
}

impl<T: Persistent + 'static> Query<T> for Filter<T> {
    fn filter_builder(&mut self) -> &mut FilterBuilder<T> {
        &mut self.filter_builder
    }
    fn add_group(&mut self, filter: Filter<T>) {
        let base = self.filter_builder();
        base.and_filter(filter.extract_filter());
    }
}
