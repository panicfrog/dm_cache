use bytes::Bytes;
use std::marker::PhantomData;

#[derive(Debug, PartialEq)]
pub enum Value<'a, 'b: 'a, 'c: 'a> {
    Null,
    Bool(bool),
    Number(f64),
    String(Bytes),
    Array(Vec<&'b Value<'a, 'b, 'c>>),
    Object(Vec<(Bytes, &'c Value<'a, 'b, 'c>)>),
    _Marker(PhantomData<&'a ()>),
}
