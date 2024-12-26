use super::error::JsonError;
use simd_json::{self, BorrowedValue, StaticNode};

pub enum IterItem<'a> {
    KV((&'a str, &'a BorrowedValue<'a>)),
    IV((usize, &'a BorrowedValue<'a>)),
    String(&'a str),
    Static(&'a StaticNode),
}

pub fn parse_and_iter<T, F>(s: &mut [u8], root: T, iter_fn: F) -> Result<(), JsonError>
where
    F: Fn(&IterItem, &T) -> T,
{
    let v: simd_json::BorrowedValue = simd_json::to_borrowed_value(s)?;
    // 遍历对象
    let mut stack = vec![(&v, root)];
    while let Some((v, t)) = stack.pop() {
        match v {
            BorrowedValue::Object(obj) => {
                for (k, v) in obj.iter() {
                    let child_t = iter_fn(&IterItem::KV((&k, &v)), &t);
                    stack.push((v, child_t));
                }
            }
            BorrowedValue::Array(arr) => {
                for (idx, v) in arr.iter().enumerate() {
                    let child_t = iter_fn(&IterItem::IV((idx, &v)), &t);
                    stack.push((v, child_t));
                }
            }
            BorrowedValue::String(s) => {
                let _ = iter_fn(&IterItem::String(s), &t);
            }
            BorrowedValue::Static(s) => {
                let _ = iter_fn(&IterItem::Static(s), &t);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
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
        parse_and_iter(d.as_mut_slice(), 0, |item, idx| {
            match item {
                IterItem::KV((k, v)) => {
                    println!("{}", k);
                }
                IterItem::IV((i, v)) => {
                    println!("{}", i);
                }
                IterItem::String(s) => {
                    println!("{}", s);
                }
                IterItem::Static(s) => {
                    println!("{:?}", s);
                }
            }
            *idx
        })
        .unwrap();
    }
}
