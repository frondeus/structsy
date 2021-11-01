use crate::{
    filter_builder::{
        filter_builder::Item,
        reader::Reader,
        start::{DataStartStep, ScanStartStep, StartStep},
    },
    Persistent, Ref,
};
use std::marker::PhantomData;

pub(crate) trait ExecutionStep {
    type Target: 'static;
    fn get_score(&self) -> u32;

    fn as_start<'a>(
        self: Box<Self>,
    ) -> (
        Option<Box<dyn ExecutionStep<Target = Self::Target>>>,
        Box<dyn StartStep<'a, Self::Target>>,
    );

    fn check(&self, item: &Item<Self::Target>, reader: &mut Reader) -> bool;
}

pub(crate) struct DataExecution<T> {
    score: u32,
    data: Vec<(Ref<T>, T)>,
}
impl<T> DataExecution<T> {
    pub(crate) fn new(data: Vec<(Ref<T>, T)>, score: u32) -> Self {
        Self { score, data }
    }
}

impl<T: 'static> ExecutionStep for DataExecution<T> {
    type Target = T;
    fn get_score(&self) -> u32 {
        self.score
    }

    fn as_start<'a>(
        self: Box<Self>,
    ) -> (
        Option<Box<dyn ExecutionStep<Target = Self::Target>>>,
        Box<dyn StartStep<'a, Self::Target>>,
    ) {
        (None, Box::new(DataStartStep::new(Box::new(self.data.into_iter()))))
    }

    fn check(&self, item: &Item<Self::Target>, _reader: &mut Reader) -> bool {
        for (id, _) in &self.data {
            if id == &item.id {
                return true;
            }
        }
        false
    }
}
pub(crate) struct FilterExecution<T, F>
where
    F: Fn(&Item<T>, &mut Reader) -> bool + 'static,
{
    condition: F,
    phantom: PhantomData<T>,
    score: u32,
}
impl<T, F> FilterExecution<T, F>
where
    F: Fn(&Item<T>, &mut Reader) -> bool + 'static,
{
    pub(crate) fn new(condition: F, score: u32) -> Self {
        Self {
            score,
            condition,
            phantom: PhantomData,
        }
    }
}

impl<T: 'static + Persistent, F> ExecutionStep for FilterExecution<T, F>
where
    F: Fn(&Item<T>, &mut Reader) -> bool + 'static,
{
    type Target = T;
    fn get_score(&self) -> u32 {
        self.score
    }

    fn as_start<'a>(
        self: Box<Self>,
    ) -> (
        Option<Box<dyn ExecutionStep<Target = Self::Target>>>,
        Box<dyn StartStep<'a, Self::Target>>,
    ) {
        (Some(self), Box::new(ScanStartStep::new()))
    }

    fn check(&self, item: &Item<Self::Target>, reader: &mut Reader) -> bool {
        (self.condition)(item, reader)
    }
}
