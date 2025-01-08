use super::error::JsonError;
use simd_json::{self, BorrowedValue, StaticNode};

#[derive(Debug)]
pub enum ItemValue<'a> {
    Object,
    Array,
    String(&'a str),
    Static(&'a StaticNode),
}

#[derive(Debug)]
pub enum IterItem<'a, T> {
    KV(&'a str, ItemValue<'a>),
    IV(usize, ItemValue<'a>),
    Array(&'a T),
    Object(&'a T),
    String(&'a str),
    Static(&'a StaticNode),
}

#[derive(Debug)]
pub enum IterItem2<'a> {
    KV(&'a str, ItemValue<'a>),
    IV(usize, ItemValue<'a>),
    Array,
    Object,
    String(&'a str),
    Static(&'a StaticNode),
}

/// 带状态的 DFS 迭代器，不再返回错误。
pub struct JsonDfsIter<'a, T, F>
where
    T: 'a,
    F: FnMut(&'a IterItem2<'a>, &T) -> T,
{
    stack: Vec<(&'a BorrowedValue<'a>, T)>,
    iter_fn: F,
    is_root: bool,
}

impl<'a, T, F> JsonDfsIter<'a, T, F>
where
    T: 'a,
    F: FnMut(&IterItem2<'a>, &T) -> T,
{
    pub fn new(root_value: &'a BorrowedValue<'a>, root_state: T, iter_fn: F) -> Self {
        Self {
            stack: vec![(root_value, root_state)],
            iter_fn,
            is_root: true,
        }
    }
}
impl<'a, T, F> Iterator for JsonDfsIter<'a, T, F>
where
    T: 'a,
    F: FnMut(&IterItem2<'a>, &T) -> T,
{
    // 不返回错误，直接产出一个 (IterItem2, T)
    type Item = (IterItem2<'a>, T);

    fn next(&mut self) -> Option<Self::Item> {
        // 如果 stack 里没东西，就结束
        let (node, state) = self.stack.pop()?;

        // 闭包的辅助调用函数，不再处理任何错误，直接返回新状态
        let mut call_iter_fn = |item: IterItem2<'a>, s: &T| -> T { (self.iter_fn)(&item, s) };

        match node {
            BorrowedValue::Object(obj) => {
                // 如果是 root，就先对“Object”调用闭包
                if self.is_root {
                    self.is_root = false;
                }

                for (k, v) in obj.iter() {
                    let value = match v {
                        BorrowedValue::Object(_) => ItemValue::Object,
                        BorrowedValue::Array(_) => ItemValue::Array,
                        BorrowedValue::String(s) => ItemValue::String(s),
                        BorrowedValue::Static(s) => ItemValue::Static(s),
                    };

                    let kv_item = IterItem2::KV(k, value);
                    // 为子节点生成新状态
                    let child_state = call_iter_fn(kv_item, &state);

                    self.stack.push((v, child_state));
                }

                // 最后返回一条：这里我们就产出 `Object` + `state`
                // 你也可以不产出，或者产出新状态，看业务需求
                Some((IterItem2::Object, state))
            }

            BorrowedValue::Array(arr) => {
                if self.is_root {
                    self.is_root = false;
                }

                for (idx, v) in arr.iter().enumerate() {
                    let value = match v {
                        BorrowedValue::Object(_) => ItemValue::Object,
                        BorrowedValue::Array(_) => ItemValue::Array,
                        BorrowedValue::String(s) => ItemValue::String(s),
                        BorrowedValue::Static(s) => ItemValue::Static(s),
                    };
                    let iv_item = IterItem2::IV(idx, value);
                    let child_state = call_iter_fn(iv_item, &state);

                    self.stack.push((v, child_state));
                }
                Some((IterItem2::Array, state))
            }

            BorrowedValue::String(s) => {
                if self.is_root {
                    self.is_root = false;
                }
                Some((IterItem2::String(s), state))
            }

            BorrowedValue::Static(s) => {
                if self.is_root {
                    self.is_root = false;
                }
                Some((IterItem2::Static(&s), state))
            }
        }
    }
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
                    let (value, is_container) = match v {
                        BorrowedValue::Object(_) => (ItemValue::Object, true),
                        BorrowedValue::Array(_) => (ItemValue::Array, true),
                        BorrowedValue::String(s) => (ItemValue::String(s), false),
                        BorrowedValue::Static(s) => (ItemValue::Static(s), false),
                    };
                    let child_state = iter_fn(&IterItem::KV(k, value), &state);
                    if is_container {
                        stack.push((v, child_state));
                    }
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
                    let (value, is_container) = match v {
                        BorrowedValue::Object(_) => (ItemValue::Object, true),
                        BorrowedValue::Array(_) => (ItemValue::Array, true),
                        BorrowedValue::String(s) => (ItemValue::String(s), false),
                        BorrowedValue::Static(s) => (ItemValue::Static(s), false),
                    };
                    let child_state = iter_fn(&IterItem::IV(idx, value), &state);
                    if is_container {
                        stack.push((v, child_state));
                    }
                }
            }
            BorrowedValue::String(s) => {
                if is_root {
                    iter_fn(&IterItem::String(s), &state);
                }
            }
            BorrowedValue::Static(s) => {
                if is_root {
                    iter_fn(&IterItem::Static(s), &state);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn item_value_description(value: &ItemValue) -> String {
        match value {
            ItemValue::Object => "Object".to_string(),
            ItemValue::Array => "Array".to_string(),
            ItemValue::String(s) => format!("String: {}", s),
            ItemValue::Static(s) => format!("Static: {:?}", s),
        }
    }

    #[test]
    fn test_json_dfs_iter() {
        let mut d: Vec<u8> = br#"{
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
        let value = simd_json::to_borrowed_value(d.as_mut_slice()).unwrap();
        let json_iter = JsonDfsIter::new(&value, vec![index.get()], |iter_item, key| {
            let result = match iter_item {
                IterItem2::KV(k, value) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(key.len() + 1);
                    idxes.extend_from_slice(key);
                    idxes.push(current_idx);
                    idxes
                }
                IterItem2::IV(i, value) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(key.len() + 1);
                    idxes.extend_from_slice(key);
                    idxes.push(current_idx);
                    idxes
                }
                IterItem2::Array => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    ids
                }
                IterItem2::Object => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    ids
                }
                IterItem2::String(s) => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    ids
                }
                IterItem2::Static(s) => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    ids
                }
            };
            result
        });

        for (item, key) in json_iter {
            println!("{:?} - {:?}", key, item);
        }
    }

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
                    result.extend_from_slice(i);
                    result
                }
                IterItem::Object(i) => {
                    let mut result = Vec::with_capacity(i.len());
                    result.extend_from_slice(i);
                    result
                }
                IterItem::KV(k, value) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(idx.len() + 1);
                    idxes.extend_from_slice(idx);
                    idxes.push(current_idx);
                    println!(
                        "{:?} - {} - {} - kv",
                        idxes,
                        k,
                        item_value_description(value)
                    );
                    idxes
                }
                IterItem::IV(i, value) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(idx.len() + 1);
                    idxes.extend_from_slice(idx);
                    idxes.push(current_idx);
                    println!(
                        "{:?} - {} - {} - iv",
                        idxes,
                        i,
                        item_value_description(value)
                    );
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
