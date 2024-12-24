use bytes::Bytes;
use std::marker::PhantomData;

pub enum Value<'a> {
    Null,
    Bool(bool),
    Number(f64),
    String(Bytes),
    Array(Vec<&'a Value<'a>>),
    Object(Vec<(Bytes, &'a Value<'a>)>),
    _Marker(PhantomData<&'a ()>),
}

pub enum ValueIter<'a> {
    Array(std::slice::Iter<'a, &'a Value<'a>>),
    Object(std::slice::Iter<'a, (Bytes, &'a Value<'a>)>),
    Empty,
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = ValueIterationItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ValueIter::Array(iter) => iter.next().map(|v| ValueIterationItem::Value(v)),
            ValueIter::Object(iter) => iter
                .next()
                .map(|(k, v)| ValueIterationItem::KeyValue(k.clone(), v)),
            ValueIter::Empty => None,
        }
    }
}

pub enum ValueIterationItem<'a> {
    Value(&'a Value<'a>),
    KeyValue(Bytes, &'a Value<'a>),
}

impl<'a> Value<'a> {
    pub fn iter(&self) -> ValueIter<'_> {
        match self {
            Value::Array(vec) => ValueIter::Array(vec.iter()),
            Value::Object(vec) => ValueIter::Object(vec.iter()),
            _ => ValueIter::Empty,
        }
    }
}
