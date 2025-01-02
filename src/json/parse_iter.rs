use super::error::JsonError;
use simd_json::{self, BorrowedValue, StaticNode};

#[derive(Debug)]
pub enum IterItem<'a, T> {
    KV(&'a str),
    IV(usize),
    Array(&'a T),
    Object(&'a T),
    String(&'a str),
    Static(&'a StaticNode),
}

pub fn parse_and_iter<T, F>(buf: &mut [u8], root: T, mut iter_fn: F) -> Result<(), JsonError>
where
    F: FnMut(&IterItem<T>, &T) -> T,
{
    // 1. 使用 simd_json 解析 JSON
    let value = simd_json::to_borrowed_value(buf)?;

    // 2. 使用栈进行深度遍历
    let mut stack = Vec::with_capacity(16);
    stack.push((&value, root));

    let mut is_root = true;

    while let Some((node, state)) = stack.pop() {
        match node {
            BorrowedValue::Object(obj) => {
                if is_root {
                    is_root = false;
                    // 遍历对象本身
                    let _ = iter_fn(&IterItem::Object(&state), &state);
                }
                // 遍历其每个 key-value
                for (k, v) in obj.iter() {
                    let child_state = iter_fn(&IterItem::KV(k), &state);
                    stack.push((v, child_state));
                }
            }
            BorrowedValue::Array(arr) => {
                if is_root {
                    is_root = false;
                    // 遍历数组本身
                    let _ = iter_fn(&IterItem::Array(&state), &state);
                }
                // 遍历其每个元素
                for (idx, v) in arr.iter().enumerate() {
                    let child_state = iter_fn(&IterItem::IV(idx), &state);
                    stack.push((v, child_state));
                }
            }
            BorrowedValue::String(s) => {
                iter_fn(&IterItem::String(s), &state);
            }
            BorrowedValue::Static(s) => {
                iter_fn(&IterItem::Static(s), &state);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn test_parse() {
        let mut d = br#"{
            "library": {
                "books": [
                    {"title": "To Kill a Mockingbird", "author": "Harper Lee", "genre": "Novel", "yearPublished": 1960, "stock": 25, "borrowed": 3},
                    {"title": "1984", "author": "George Orwell", "genre": "Dystopian Fiction", "yearPublished": 1949, "stock": 20, "borrowed": 5},
                    {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "genre": "Novel", "yearPublished": 1925, "stock": 30, "borrowed": 8},
                    {"title": "Pride and Prejudice", "author": "Jane Austen", "genre": "Romance Novel", "yearPublished": 1813, "stock": 15, "borrowed": 2},
                    {"title": "War and Peace", "author": "Leo Tolstoy", "genre": "Historical Fiction", "yearPublished": 1869, "stock": 5, "borrowed": 2}
                ],
                "totalBooks": 75, "totalBorrowed": 18
            }
        }"#
        .to_vec();
        let index = Cell::new(0_u32);
        parse_and_iter(
            d.as_mut_slice(),
            vec![index.get()],
            |item, idx| match item {
                IterItem::Array(i) => {
                    let mut result = Vec::with_capacity(i.len());
                    println!("{:?} - array", i);
                    result.extend_from_slice(i);
                    result
                }
                IterItem::Object(i) => {
                    println!("{:?} - object", i);
                    let mut result = Vec::with_capacity(i.len());
                    result.extend_from_slice(i);
                    result
                }
                IterItem::KV(k) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(idx.len() + 1);
                    idxes.extend_from_slice(idx);
                    idxes.push(current_idx);
                    println!("{:?} - {} - kv", idxes, k);
                    idxes
                }
                IterItem::IV(i) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(idx.len() + 1);
                    idxes.extend_from_slice(idx);
                    idxes.push(current_idx);
                    println!("{:?} - {} - iv", idxes, i);
                    idxes
                }
                IterItem::String(s) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(idx.len() + 1);
                    idxes.extend_from_slice(idx);
                    idxes.push(current_idx);
                    println!("{:?} - {}", idxes, s);
                    idxes
                }
                IterItem::Static(s) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(idx.len() + 1);
                    idxes.extend_from_slice(idx);
                    idxes.push(current_idx);
                    println!("{:?} - {}", idxes, s);
                    idxes
                }
            },
        )
        .unwrap();
    }
}
