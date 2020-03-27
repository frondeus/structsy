use crate::{index::find, FieldDescription, Persistent, Ref, StructDescription, Structsy};
use persy::IndexType;
use std::marker::PhantomData;

trait FilterBuilderStep {
    type Target;
    fn score(&self) -> u32;
    fn filter(
        &self,
        structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>;
    fn first(&self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>;
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
        &self,
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
    fn first(&self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = find(&structsy, &self.index_name, &self.index_value) {
            Box::new(found.into_iter())
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct ConditionSingleFilter<V: PartialEq + Clone + 'static, T: Persistent + 'static> {
    value: V,
    access: fn(&T) -> &Vec<V>,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionSingleFilter<V, T> {
    fn new(access: fn(&T) -> &Vec<V>, value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(ConditionSingleFilter { access, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionSingleFilter<V, T> {
    type Target = T;
    fn score(&self) -> u32 {
        1
    }
    fn filter(
        &self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let value = self.value.clone();
        let access = self.access.clone();
        Box::new(iter.filter(move |(_, x)| (access)(x).contains(&value)))
    }
    fn first(&self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        if let Ok(found) = structsy.scan::<T>() {
            self.filter(structsy, Box::new(found))
        } else {
            Box::new(Vec::new().into_iter())
        }
    }
}

struct ConditionFilter<V: PartialEq + Clone + 'static, T: Persistent + 'static> {
    value: V,
    access: fn(&T) -> &V,
}

impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> ConditionFilter<V, T> {
    fn new(access: fn(&T) -> &V, value: V) -> Box<dyn FilterBuilderStep<Target = T>> {
        Box::new(ConditionFilter { access, value })
    }
}
impl<V: PartialEq + Clone + 'static, T: Persistent + 'static> FilterBuilderStep for ConditionFilter<V, T> {
    type Target = T;
    fn score(&self) -> u32 {
        1
    }
    fn filter(
        &self,
        _structsy: &Structsy,
        iter: Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>>,
    ) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
        let val = self.value.clone();
        let access = self.access.clone();
        Box::new(iter.filter(move |(_, x)| *(access)(x) == val))
    }

    fn first(&self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<Self::Target>, Self::Target)>> {
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

    pub fn finish(mut self, structsy: &Structsy) -> Box<dyn Iterator<Item = (Ref<T>, T)>> {
        self.steps.sort_by_key(|x| x.score());
        let mut res = None;
        for s in self.steps.into_iter() {
            res = Some(if let Some(prev) = res {
                s.filter(structsy, prev)
            } else {
                s.first(structsy)
            });
        }
        res.expect("there is every time at least one element")
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

    pub fn simple_condition<V: PartialEq + Clone + 'static>(&mut self, name: &str, value: V, access: fn(&T) -> &V) {
        self.add(ConditionFilter::new(access, value))
    }

    pub fn indexable_vec_condition<V: IndexType + PartialEq + 'static>(
        &mut self,
        name: &str,
        value: Vec<V>,
        access: fn(&T) -> &Vec<V>,
    ) {
        //TODO: support lookup in index
        self.add(ConditionFilter::new(access, value))
    }

    pub fn simple_vec_single_condition<V: PartialEq + Clone + 'static>(
        &mut self,
        _name: &str,
        value: V,
        access: fn(&T) -> &Vec<V>,
    ) {
        self.add(ConditionSingleFilter::new(access, value))
    }

    pub fn indexable_vec_single_condition<V: IndexType + PartialEq + 'static>(
        &mut self,
        name: &str,
        value: V,
        access: fn(&T) -> &Vec<V>,
    ) {
        let desc = T::get_description();
        if let Some(f) = desc.get_field(name) {
            if f.indexed.is_some() {
                let index_name = format!("{}.{}", desc.name, f.name);
                self.add(IndexFilter::new(index_name, value))
            } else {
                self.add(ConditionSingleFilter::new(access, value))
            }
        } else {
            panic!("field with name:'{}' not found", name)
        }
    }

    pub fn indexable_option_single_condition<V: IndexType + PartialEq + 'static>(
        &mut self,
        name: &str,
        value: V,
        access: fn(&T) -> &Option<V>,
    ) {
        self.indexable_option_condition(name, Some(value), access);
    }
    pub fn indexable_option_condition<V: IndexType + PartialEq + 'static>(
        &mut self,
        name: &str,
        value: Option<V>,
        access: fn(&T) -> &Option<V>,
    ) {
        let desc = T::get_description();
        if let Some(f) = desc.get_field(name) {
            if f.indexed.is_some() {
                let index_name = format!("{}.{}", desc.name, f.name);
                if let Some(v) = value {
                    self.add(IndexFilter::new(index_name, v));
                } else {
                    //TODO: index Check for  not present;
                }
            } else {
                self.add(ConditionFilter::<Option<V>, T>::new(access, value));
            }
        } else {
            panic!("field with name:'{}' not found", name)
        }
    }
}

pub trait FieldConditionType<T: Persistent, V, F> {
    fn add_to_filter(&self, filter_builder: &mut FilterBuilder<T>, v: V);
}
