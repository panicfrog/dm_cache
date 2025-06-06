use simd_json::{self, BorrowedValue, StaticNode};

#[derive(Debug)]
pub enum ItemValue<'a> {
    Object,
    Array,
    String(&'a str),
    Static(&'a StaticNode),
}

#[derive(Debug)]
pub enum IterItem<'a> {
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
    F: for<'b> FnMut(&'b IterItem<'b>, &T) -> T,
{
    stack: Vec<(&'a BorrowedValue<'a>, T)>,
    iter_fn: F,
}

impl<'a, T, F> JsonDfsIter<'a, T, F>
where
    F: for<'b> FnMut(&'b IterItem<'b>, &T) -> T,
{
    pub fn new(root_value: &'a BorrowedValue<'a>, root_state: T, iter_fn: F) -> Self {
        Self {
            stack: vec![(root_value, root_state)],
            iter_fn,
        }
    }
}
impl<'a, T, F> Iterator for JsonDfsIter<'a, T, F>
where
    F: for<'b> FnMut(&'b IterItem<'b>, &T) -> T,
{
    // 不返回错误，直接产出一个 (IterItem2, T)
    type Item = (IterItem<'a>, T);

    fn next(&mut self) -> Option<Self::Item> {
        // 如果 stack 里没东西，就结束
        let (node, state) = self.stack.pop()?;

        // 闭包的辅助调用函数，不再处理任何错误，直接返回新状态
        let mut call_iter_fn = |item: IterItem<'a>, s: &T| -> T { (self.iter_fn)(&item, s) };

        match node {
            BorrowedValue::Object(obj) => {
                // 如果是 root，就先对“Object”调用闭包

                for (k, v) in obj.iter() {
                    let value = match v {
                        BorrowedValue::Object(_) => ItemValue::Object,
                        BorrowedValue::Array(_) => ItemValue::Array,
                        BorrowedValue::String(s) => ItemValue::String(s),
                        BorrowedValue::Static(s) => ItemValue::Static(s),
                    };

                    let kv_item = IterItem::KV(k, value);
                    // 为子节点生成新状态
                    let child_state = call_iter_fn(kv_item, &state);

                    self.stack.push((v, child_state));
                }

                // 最后返回一条：这里我们就产出 `Object` + `state`
                // 你也可以不产出，或者产出新状态，看业务需求
                Some((IterItem::Object, state))
            }

            BorrowedValue::Array(arr) => {
                for (idx, v) in arr.iter().enumerate() {
                    let value = match v {
                        BorrowedValue::Object(_) => ItemValue::Object,
                        BorrowedValue::Array(_) => ItemValue::Array,
                        BorrowedValue::String(s) => ItemValue::String(s),
                        BorrowedValue::Static(s) => ItemValue::Static(s),
                    };
                    let iv_item = IterItem::IV(idx, value);
                    let child_state = call_iter_fn(iv_item, &state);

                    self.stack.push((v, child_state));
                }
                Some((IterItem::Array, state))
            }

            BorrowedValue::String(s) => Some((IterItem::String(s), state)),

            BorrowedValue::Static(s) => Some((IterItem::Static(&s), state)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

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
                IterItem::KV(_, _) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(key.len() + 1);
                    idxes.extend_from_slice(key);
                    idxes.push(current_idx);
                    idxes
                }
                IterItem::IV(_, _) => {
                    index.set(index.get() + 1);
                    let current_idx = index.get();
                    let mut idxes = Vec::with_capacity(key.len() + 1);
                    idxes.extend_from_slice(key);
                    idxes.push(current_idx);
                    idxes
                }
                IterItem::Array => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    ids
                }
                IterItem::Object => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    ids
                }
                IterItem::String(_) => {
                    let mut ids = Vec::with_capacity(key.len());
                    ids.extend_from_slice(key);
                    ids
                }
                IterItem::Static(_) => {
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
}
