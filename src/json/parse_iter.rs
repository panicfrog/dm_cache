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

///
/// 这是我们的迭代器结构：
/// - `stack` 存的是“下一步要遍历的节点 + 这个节点对应的状态 T”
/// - `iter_fn` 是用户给的闭包：`FnMut(&IterItem, &T) -> Result<T, E>`
/// - `is_root` 用来标识下次弹出的是否是 root
/// - `error` 存储了一旦出现的错误，如果有的话，迭代器会停止产出新的节点
///
pub struct JsonDfsIter<'a, T, E, F>
where
    T: 'a,
    F: FnMut(&'a IterItem2<'a>, &'a T) -> Result<T, E>,
{
    stack: Vec<(&'a BorrowedValue<'a>, T)>,
    iter_fn: F,
    is_root: bool,
    error: Option<E>,
}

impl<'a, T, E, F> JsonDfsIter<'a, T, E, F>
where
    F: FnMut(&IterItem2<'a>, &T) -> Result<T, E>,
{
    pub fn new(root_value: &'a BorrowedValue<'a>, root_state: T, iter_fn: F) -> Self {
        Self {
            stack: vec![(root_value, root_state)],
            iter_fn,
            is_root: true,
            error: None,
        }
    }
}

impl<'a, T, E, F> Iterator for JsonDfsIter<'a, T, E, F>
where
    F: FnMut(&IterItem2<'a>, &T) -> Result<T, E>,
{
    type Item = Result<(IterItem2<'a>, T), E>;

    fn next(&mut self) -> Option<Self::Item> {
        // 如果之前已经出错了，后面都不再产出了
        if self.error.is_some() {
            return None;
        }

        // 没有可遍历的节点了
        let (node, state) = self.stack.pop()?;

        // 封装一个辅助函数，用来调用闭包并在出错时记录错误
        let mut call_iter_fn = |item: IterItem2<'a>, s: &T| -> Option<T> {
            match (self.iter_fn)(&item, s) {
                Ok(child_state) => Some(child_state),
                Err(e) => {
                    self.error = Some(e);
                    None
                }
            }
        };

        match node {
            BorrowedValue::Object(obj) => {
                // 如果是 root，就先产出一次 "Object(&root_state)"
                if self.is_root {
                    self.is_root = false;
                    // 调用闭包
                    let root_item = IterItem2::Object;
                    if call_iter_fn(root_item, &state).is_none() {
                        return Some(Err(self.error.take().unwrap()));
                    }
                }

                // 遍历子节点 (k,v)
                for (k, v) in obj.iter() {
                    let (value, is_container) = match v {
                        BorrowedValue::Object(_) => (ItemValue::Object, true),
                        BorrowedValue::Array(_) => (ItemValue::Array, true),
                        BorrowedValue::String(s) => (ItemValue::String(s), false),
                        BorrowedValue::Static(s) => (ItemValue::Static(s), false),
                    };

                    // KV 的 iter_item
                    let kv_item = IterItem2::KV(k, value);
                    // 调用闭包
                    let child_state = match call_iter_fn(kv_item, &state) {
                        Some(cs) => cs,
                        None => {
                            return Some(Err(self.error.take().unwrap()));
                        }
                    };
                    // if is_container {
                    self.stack.push((v, child_state));
                    // }
                }

                // 最后我们产出一条 (IterItem::Object(&state), state)
                // 也可以只在 root 的时候产出；看你业务需求
                Some(Ok((IterItem2::Object, state)))
            }

            BorrowedValue::Array(arr) => {
                if self.is_root {
                    self.is_root = false;
                    let root_item = IterItem2::Array;
                    if call_iter_fn(root_item, &state).is_none() {
                        return Some(Err(self.error.take().unwrap()));
                    }
                }

                for (idx, v) in arr.iter().enumerate() {
                    let (value, is_container) = match v {
                        BorrowedValue::Object(_) => (ItemValue::Object, true),
                        BorrowedValue::Array(_) => (ItemValue::Array, true),
                        BorrowedValue::String(s) => (ItemValue::String(s), false),
                        BorrowedValue::Static(s) => (ItemValue::Static(s), false),
                    };
                    let iv_item = IterItem2::IV(idx, value);
                    let child_state = match call_iter_fn(iv_item, &state) {
                        Some(cs) => cs,
                        None => {
                            return Some(Err(self.error.take().unwrap()));
                        }
                    };
                    // if is_container {
                    self.stack.push((v, child_state));
                    // }
                }

                Some(Ok((IterItem2::Array, state)))
            }

            BorrowedValue::String(s) => {
                if self.is_root {
                    self.is_root = false;
                    let root_item = IterItem2::String(s);
                    if call_iter_fn(root_item, &state).is_none() {
                        return Some(Err(self.error.take().unwrap()));
                    }
                }
                Some(Ok((IterItem2::String(s), state)))
            }

            BorrowedValue::Static(s) => {
                if self.is_root {
                    self.is_root = false;
                    let root_item = IterItem2::Static(&s);
                    if call_iter_fn(root_item, &state).is_none() {
                        return Some(Err(self.error.take().unwrap()));
                    }
                }
                Some(Ok((IterItem2::Static(&s), state)))
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
            let mut result: Result<Vec<u32>, JsonError> = Ok(vec![]);
            result = match iter_item {
                IterItem2::KV(k, value) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(key.len() + 1);
                    idxes.extend_from_slice(key);
                    idxes.push(current_idx);
                    Ok(idxes)
                }
                IterItem2::IV(i, value) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(key.len() + 1);
                    idxes.extend_from_slice(key);
                    idxes.push(current_idx);
                    Ok(idxes)
                }
                IterItem2::Array => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    Ok(ids)
                }
                IterItem2::Object => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    Ok(ids)
                }
                IterItem2::String(s) => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    Ok(ids)
                }
                IterItem2::Static(s) => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    Ok(ids)
                }
            };
            result
        });

        for item in json_iter {
            println!("{:?}", item);
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
