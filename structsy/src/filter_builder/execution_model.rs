use crate::{
    filter_builder::{
        plan_model::{FilterPlan, QueryPlan, Source},
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
) -> SRes<Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>> {
    let QueryPlan {
        source,
        filter,
        orders,
        projections,
    } = plan;

    let iter = start::<T>(source, reader)?;
    let iter = if let Some(f) = filter {
        Box::new(FilterExecution {
            source: iter,
            filter: f,
        })
    } else {
        iter
    };

    Ok(iter)
}

struct FilterExecution<'a, T> {
    source: Box<dyn Iterator<Item = (Ref<T>, T)> + 'a>,
    filter: FilterPlan,
}

trait FilterCheck<T> {
    fn check(&self, value: &T) -> bool;
}

impl<T> FilterCheck<T> for FilterPlan {
    fn check(&self, value: &T) -> bool {
        false
    }
}

impl<'a, T> Iterator for FilterExecution<'a, T> {
    type Item = (Ref<T>, T);
    fn next(&mut self) -> Option<Self::Item> {
        self.source.next().filter(|v| self.filter.check(&v))
    }
}

struct Accumulator {}
