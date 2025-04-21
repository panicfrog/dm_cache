use crate::kv::{read_variable_sized_id, EncodeError, VariableSizedId};
use jsonpath_rust::parser::parse_json_path;

// TODO: 添加解析jsonpath的方法
fn parse(path: &str) {
    let jp = parse_json_path(path).unwrap();
    for seg in jp.segments.iter() {
        println!("{:?}", seg);
    }
}

// pub(crate) struct LazyValue {
//     pub(crate) id: VariableSizedId,
// }
