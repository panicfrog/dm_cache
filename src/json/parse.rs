use simd_json::{self, BorrowedValue, StaticNode};

fn parse() {
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
    let v: simd_json::BorrowedValue = simd_json::to_borrowed_value(&mut d).unwrap();
    // 遍历对象
    let mut stack = vec![&v];
    while let Some(v) = stack.pop() {
        match v {
            BorrowedValue::Object(obj) => {
                for (k, v) in obj.iter() {
                    println!("key: {:?}", k);
                    stack.push(v);
                }
            }
            BorrowedValue::Array(arr) => {
                for v in arr.iter() {
                    stack.push(v);
                }
            }
            BorrowedValue::String(s) => {
                println!("string: {:?}", s);
            }
            BorrowedValue::Static(StaticNode::I64(i)) => {
                println!("value: {:?}", i);
            }
            BorrowedValue::Static(StaticNode::Bool(b)) => {
                println!("value: {:?}", b);
            }
            BorrowedValue::Static(StaticNode::F64(f)) => {
                println!("value: {:?}", f);
            }
            BorrowedValue::Static(StaticNode::Null) => {
                println!("value: null");
            }
            BorrowedValue::Static(StaticNode::U64(u)) => {
                println!("value: {:?}", u);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        parse();
    }
}
