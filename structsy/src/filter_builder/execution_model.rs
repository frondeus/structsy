use crate::{
    filter_builder::{
        plan_model::{QueryPlan, Source},
        reader::Reader,
    },
    Order, Persistent, Ref, SRes,
};

fn start<'a, T: Persistent + 'static>(
    source: Source,
    reader: Reader<'a>,
) -> SRes<Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>> {
    Ok(match source {
        Source::Index(index) => {
            // TODO: get bound out :index.index_range.unwrap_or(..);
            let found = reader.find_range(&index.index_name, (0u8)..todo!())?;
            if Order::Desc == index.ordering_mode {
                Box::new(found.reader_rev())
            } else {
                Box::new(found)
            }
        }
        Source::Scan(_scan) => Box::new(reader.scan()?),
    })
}
fn execute<'a, T: Persistent + 'static>(
    plan: QueryPlan,
    reader: Reader<'a>,
) -> SRes<Box<dyn Iterator<Item = (Ref<T>, T)>>> {
    let QueryPlan {
        source,
        filter,
        orders,
        projections,
    } = plan;

    let start = start::<T>(source, reader)?;
    if let Some(f) = filter {}

    todo!()
}

struct IterExecution<T> {
    iter: Box<dyn Iterator<Item = T>>,
}

trait Filter<T> {
    fn check(&self, value: T) -> bool;
}

struct FilterExecution<T> {
    filter: Box<dyn Filter<T>>,
}

struct Accumulator {}
