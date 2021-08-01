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
