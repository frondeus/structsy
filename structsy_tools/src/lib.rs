use structsy::internal::Description;
use structsy::record::Record;
use structsy::{RawAccess, Structsy, StructsyError};

pub enum Data {
    Definition(Description),
    Record(Record),
}

pub fn export(structsy: &Structsy) -> Result<impl Iterator<Item = Data>, StructsyError> {
    let definitions = structsy.list_defined()?.map(|def| Data::Definition(def));
    let st = structsy.clone();
    let data_iter = structsy
        .list_defined()?
        .map(move |def| {
            st.raw_scan(&def.get_name())
                .ok()
                .map(|it| it.map(|(_, record)| Data::Record(record)))
        })
        .flatten()
        .flatten();
    Ok(definitions.chain(data_iter))
}

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
