use crate::{index::find, FieldDescription, Persistent, Ref, StructDescription, Structsy};
use persy::IndexType;
use std::marker::PhantomData;

trait FilterBuilderStep {
    type Target;
    fn score(&self) -> u32;
    fn filter(
        self,
        structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>;
    fn first(self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>;
}

struct IndexFilter<V: IndexType + 'static, T: Persistent + 'static> {
    index_name: String,
    index_value: V,
    phantom: PhantomData<T>,
}

impl<V: IndexType + 'static, T: Persistent + 'static> IndexFilter<V, T> {
    fn new(index_name: String, index_value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(IndexFilter {
            index_name,
            index_value,
            phantom: PhantomData,
        })
    }
}

impl<V: IndexType + 'static, T: Persistent + 'static> FilterBuilderStep for IndexFilter<V, T> {
    type Target = T;
    fn score(&self) -> u32 {
        1
    }
    fn filter(
        self,
        structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = find(&structsy, &self.index_name, &self.index_value) {
            let to_filter = found.into_iter().map(|(r, _)| r).collect::<Vec<_>>();
            Box::new(iter.filter(move |(r, _x)| to_filter.contains(r)))
        } else {
            iter
        }
    }
    fn first(self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = find(&structsy, &self.index_name, &self.index_value) {
            Box::new(found.into_iter())
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}


struct ConditionSingleFilter<V: PartialEq + 'static, T: Persistent + 'static> {
    value: V,
    access: fn(&T) -> &Vec<V>,
}

impl<V: PartialEq + 'static, T: Persistent + 'static> ConditionSingleFilter<V, T> {
    fn new(access: fn(&T) -> &Vec<V>, value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(ConditionSingleFilter { access, value })
    }
}
impl<V: PartialEq + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionSingleFilter<V, T> {
    type Target = T;
    fn score(&self) -> u32 {
        1
    }
    fn filter(
        self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        Box::new(iter.filter(move |(_, x)| (self.access)(x).contains(&self.value)))
    }
    fn first(self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = structsy.scan::<T>() {
            self.filter(structsy, Box::new(found))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct ConditionFilter<V: PartialEq + 'static, T: Persistent + 'static> {
    value: V,
    access: fn(&T) -> &V,
}

impl<V: PartialEq + 'static, T: Persistent + 'static> ConditionFilter<V, T> {
    fn new(access: fn(&T) -> &V, value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(ConditionFilter { access, value })
    }
}
impl<V: PartialEq + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionFilter<V, T> {
    type Target = T;
    fn score(&self) -> u32 {
        1
    }
    fn filter(
        self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        Box::new(iter.filter(move |(_, x)| *(self.access)(x) == self.value))
    }
    fn first(self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = structsy.scan::<T>() {
            self.filter(structsy, Box::new(found))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

pub struct FilterBuilder<T: Persistent + 'static> {
    steps: Vec<Box<dyn FilterBuilderStep<Target = T>>>,
}

impl<T: Persistent + 'static> FilterBuilder<T> {
    pub fn new() -> FilterBuilder<T> {
        FilterBuilder { steps: Vec::new() }
    }

    fn add(&mut self, filter: Box<dyn FilterBuilderStep<Target = T>>) {
        self.steps.push(filter);
    }

    pub fn indexable_condition<V: IndexType + PartialEq + 'static>(
        &mut self,
        name: &str,
        value: V,
        access: fn(&T) -> &V,
    ) {
        let desc = T::get_description();
        if let Some(f) = desc.get_field(name) {
            if f.indexed.is_some() {
                let index_name = format!("{}.{}", desc.name, f.name);
                self.add(IndexFilter::new(index_name, value))
            } else {
                self.add(ConditionFilter::new(access, value))
            }
        } else {
            panic!("field with name:'{}' not found", name)
        }
    }
    pub fn simple_condition<V: PartialEq + 'static>(&mut self, name: &str, value: V, access: fn(&T) -> &V) {
        self.add(ConditionFilter::new(access, value))
    }
}
