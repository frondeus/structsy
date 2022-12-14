use structsy::internal::Description;
use structsy::record::Record;
use structsy::{RawAccess, RawRead, Snapshot, Structsy, StructsyError};

/// Enum of all possible data types in structsy, use 'serde_integration' to allow
/// to serialize them with serde
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Data {
    Definition(Description),
    Record(Record),
}

/// Produce and iterator that allow to iterate all the data in a structsy database
/// for then write some data on an external target.
///
pub fn export(structsy: &Structsy) -> Result<impl Iterator<Item = Data>, StructsyError> {
    let definitions = structsy.list_defined()?.map(|def| Data::Definition(def));
    let st = structsy.clone();
    let data_iter = structsy
        .list_defined()?
        .map(move |def| {
            RawRead::raw_scan(&st, &def.get_name())
                .ok()
                .map(|it| it.map(|(_, record)| Data::Record(record)))
        })
        .flatten()
        .flatten();
    Ok(definitions.chain(data_iter))
}

/// Produce and iterator that allow to iterate all the data in a structsy database
/// for then write some data on an external target.
///
pub fn export_from_snapshot(snapshot: Snapshot) -> Result<impl Iterator<Item = Data>, StructsyError> {
    let definitions = snapshot.list_defined()?.map(|def| Data::Definition(def));
    let snap = snapshot.clone();
    let data_iter = snapshot
        .list_defined()?
        .map(move |def| {
            RawRead::raw_scan(&snap, &def.get_name())
                .ok()
                .map(|it| it.map(|(_, record)| Data::Record(record)))
        })
        .flatten()
        .flatten();
    Ok(definitions.chain(data_iter))
}
///Import all the data provided by the iterator to a structsy database.
///
pub fn import(structsy: &Structsy, iter: impl Iterator<Item = Data>) -> Result<(), StructsyError> {
    for values in iter {
        match values {
            Data::Definition(def) => {
                structsy.raw_define(def)?;
            }
            Data::Record(rec) => {
                let mut raw_tx = structsy.raw_begin()?;
                raw_tx.raw_insert(&rec)?;
                raw_tx.prepare()?.commit()?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use structsy_derive::{queries, Persistent};

    use super::{export, import};
    use structsy::{Structsy, StructsyTx};
    #[derive(Persistent)]
    struct Simple {
        #[index(mode = "cluster")]
        name: String,
        size: u32,
    }

    #[queries(Simple)]
    trait SimpleQueries {
        fn by_name(self, name: &str) -> Self;
    }

    #[test]
    fn simple_export_import() {
        let db = Structsy::memory().unwrap();
        db.define::<Simple>().unwrap();
        let mut tx = db.begin().unwrap();
        tx.insert(&Simple {
            name: "first".to_owned(),
            size: 10,
        })
        .unwrap();
        tx.commit().unwrap();

        let loaded = Structsy::memory().unwrap();
        let data = export(&db).unwrap();
        import(&loaded, data).unwrap();

        assert_eq!(loaded.query::<Simple>().by_name("first").into_iter().count(), 1);
    }
}
