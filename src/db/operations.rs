// 这些导入暂时未使用，为未来扩展保留
// use crate::kv::{read_variable_sized_id, EncodeError, VariableSizedId};
use jsonpath_rust::parser::parse_json_path;
use thiserror::Error;

use crate::{kv::{Key, VariableSizedId}, DBError, Database};

/// JSONPath 路径段，表示路径中的一个访问操作
/// 
/// 目前只支持两种基本的访问模式：
/// - `Key(String)`: 对象属性访问，如 `.name` 或 `["name"]`
/// - `Index(usize)`: 数组索引访问，如 `[0]` 或 `[1]`
#[derive(Debug, Clone)]
pub enum JsonPathSegment {
    /// 对象键访问，例如 $.user.name 中的 "user" 和 "name"
    Key(String),
    /// 数组索引访问，例如 $.users[0] 中的 0
    Index(usize),
}

/// JSONPath 解析错误类型
#[derive(Error, Debug)]
pub enum JsonPathParseError {
    #[error("JSONPath解析失败: {0}")]
    ParseFailed(String),
    #[error("不支持的段类型，只支持 object.key 和 array[index] 访问")]
    UnsupportedSegmentType,
    #[error("不支持的选择器类型")]
    UnsupportedSelectorType,
}

/// 解析 JSONPath 字符串为 JsonPathSegment 向量
/// 
/// 该函数将 JSONPath 表达式解析为内部使用的路径段序列。
/// 目前只支持以下两种访问模式的组合：
/// - 对象属性访问：`$.user.name` 或 `$.user["name"]`
/// - 数组索引访问：`$.users[0]` 或 `$.data[1]`
/// 
/// # 参数
/// * `path` - JSONPath 字符串，如 "$.user.name" 或 "$.users[0].name"
/// 
/// # 返回值
/// * `Ok(Vec<JsonPathSegment>)` - 解析成功时返回路径段序列
/// * `Err(JsonPathParseError)` - 解析失败时返回错误信息
/// 
/// # 示例
/// ```rust
/// use dm_cache::{parse, JsonPathSegment};
/// 
/// let segments = parse("$.user.name").unwrap();
/// assert_eq!(segments.len(), 2);
/// 
/// let segments = parse("$.users[0].name").unwrap();
/// assert_eq!(segments.len(), 3);
/// ```
/// 
/// # 不支持的特性
/// - 后代操作符 (`$..name`)
/// - 通配符 (`$.users[*]`)
/// - 负数索引 (`$.users[-1]`)
/// - 过滤器表达式 (`$.users[?(@.active)]`)
/// - 切片操作 (`$.users[0:2]`)
pub fn parse(path: &str) -> Result<Vec<JsonPathSegment>, JsonPathParseError> {
    let jp = parse_json_path(path)
        .map_err(|e| JsonPathParseError::ParseFailed(format!("{:?}", e)))?;
    
    let mut segments = Vec::new();
    
    // 只支持 object.key 和 array[index] 这两种组合的path，其他暂时不支持
    for seg in jp.segments.iter() {
        match seg {
            // 处理普通选择器
            jsonpath_rust::parser::model::Segment::Selector(selector) => {
                match selector {
                    // 处理对象键访问，例如 .name 或 ["name"]
                    jsonpath_rust::parser::model::Selector::Name(name) => {
                        // 处理括号表示法中的引号：去除 'key' 或 "key" 中的引号
                        let clean_name = if name.starts_with('\'') && name.ends_with('\'') && name.len() > 1 {
                            &name[1..name.len()-1]
                        } else if name.starts_with('"') && name.ends_with('"') && name.len() > 1 {
                            &name[1..name.len()-1]
                        } else {
                            name
                        };
                        segments.push(JsonPathSegment::Key(clean_name.to_string()));
                    },
                    // 处理数组索引访问，例如 [0] 或 [1]
                    jsonpath_rust::parser::model::Selector::Index(index) => {
                        if *index < 0 {
                            return Err(JsonPathParseError::UnsupportedSelectorType);
                        }
                        segments.push(JsonPathSegment::Index(*index as usize));
                    },
                    // 其他选择器类型暂不支持
                    _ => {
                        return Err(JsonPathParseError::UnsupportedSelectorType);
                    }
                }
            },
            // 其他段类型暂不支持（如 Descendant、Selectors）
            _ => {
                return Err(JsonPathParseError::UnsupportedSegmentType);
            }
        }
    }
    
    Ok(segments)
}

/*
TODO: 1. 创建根据db + root_key + Vec<JsonPathSegment> 创建 kv::node::Key 的方法 
 */
 pub fn json_path_key(db: &Database, root_key: &str, segments: &Vec<JsonPathSegment>) -> Result<Key, DBError> {
    unimplemented!()
 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_object_access() {
        // 测试简单的对象属性访问
        let result = parse("$.name").unwrap();
        assert_eq!(result.len(), 1);
        match &result[0] {
            JsonPathSegment::Key(key) => assert_eq!(key, "name"),
            _ => panic!("Expected Key segment"),
        }
    }

    #[test]
    fn test_nested_object_access() {
        // 测试嵌套对象访问
        let result = parse("$.user.profile.name").unwrap();
        assert_eq!(result.len(), 3);
        
        match &result[0] {
            JsonPathSegment::Key(key) => assert_eq!(key, "user"),
            _ => panic!("Expected Key segment at index 0"),
        }
        match &result[1] {
            JsonPathSegment::Key(key) => assert_eq!(key, "profile"),
            _ => panic!("Expected Key segment at index 1"),
        }
        match &result[2] {
            JsonPathSegment::Key(key) => assert_eq!(key, "name"),
            _ => panic!("Expected Key segment at index 2"),
        }
    }

    #[test]
    fn test_simple_array_access() {
        // 测试简单的数组索引访问
        let result = parse("$.users[0]").unwrap();
        assert_eq!(result.len(), 2);
        
        match &result[0] {
            JsonPathSegment::Key(key) => assert_eq!(key, "users"),
            _ => panic!("Expected Key segment at index 0"),
        }
        match &result[1] {
            JsonPathSegment::Index(index) => assert_eq!(*index, 0),
            _ => panic!("Expected Index segment at index 1"),
        }
    }

    #[test]
    fn test_mixed_object_array_access() {
        // 测试对象和数组访问的混合
        let result = parse("$.data.items[2].title").unwrap();
        assert_eq!(result.len(), 4);
        
        match &result[0] {
            JsonPathSegment::Key(key) => assert_eq!(key, "data"),
            _ => panic!("Expected Key segment at index 0"),
        }
        match &result[1] {
            JsonPathSegment::Key(key) => assert_eq!(key, "items"),
            _ => panic!("Expected Key segment at index 1"),
        }
        match &result[2] {
            JsonPathSegment::Index(index) => assert_eq!(*index, 2),
            _ => panic!("Expected Index segment at index 2"),
        }
        match &result[3] {
            JsonPathSegment::Key(key) => assert_eq!(key, "title"),
            _ => panic!("Expected Key segment at index 3"),
        }
    }

    #[test]
    fn test_multiple_array_indices() {
        // 测试多个数组索引
        let result = parse("$.matrix[1][3]").unwrap();
        assert_eq!(result.len(), 3);
        
        match &result[0] {
            JsonPathSegment::Key(key) => assert_eq!(key, "matrix"),
            _ => panic!("Expected Key segment at index 0"),
        }
        match &result[1] {
            JsonPathSegment::Index(index) => assert_eq!(*index, 1),
            _ => panic!("Expected Index segment at index 1"),
        }
        match &result[2] {
            JsonPathSegment::Index(index) => assert_eq!(*index, 3),
            _ => panic!("Expected Index segment at index 2"),
        }
    }

    #[test]
    fn test_complex_path() {
        // 测试复杂路径
        let result = parse("$.library.books[0].chapters[5].title").unwrap();
        assert_eq!(result.len(), 6);
        
        let expected = vec![
            JsonPathSegment::Key("library".to_string()),
            JsonPathSegment::Key("books".to_string()),
            JsonPathSegment::Index(0),
            JsonPathSegment::Key("chapters".to_string()),
            JsonPathSegment::Index(5),
            JsonPathSegment::Key("title".to_string()),
        ];
        
        for (i, (actual, expected)) in result.iter().zip(expected.iter()).enumerate() {
            match (actual, expected) {
                (JsonPathSegment::Key(a), JsonPathSegment::Key(e)) => {
                    assert_eq!(a, e, "Key mismatch at index {}", i);
                },
                (JsonPathSegment::Index(a), JsonPathSegment::Index(e)) => {
                    assert_eq!(a, e, "Index mismatch at index {}", i);
                },
                _ => panic!("Type mismatch at index {}", i),
            }
        }
    }

    #[test]
    fn test_root_only() {
        // 测试只有根路径的情况
        let result = parse("$").unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_negative_index_error() {
        // 测试负数索引应该返回错误
        let result = parse("$.users[-1]");
        assert!(result.is_err());
        match result.unwrap_err() {
            JsonPathParseError::UnsupportedSelectorType => {},
            _ => panic!("Expected UnsupportedSelectorType error"),
        }
    }

    #[test]
    fn test_wildcard_error() {
        // 测试通配符应该返回错误
        let result = parse("$.users[*]");
        assert!(result.is_err());
        match result.unwrap_err() {
            JsonPathParseError::UnsupportedSelectorType => {},
            _ => panic!("Expected UnsupportedSelectorType error"),
        }
    }

    #[test]
    fn test_descendant_operator_error() {
        // 测试后代操作符应该返回错误
        let result = parse("$..name");
        assert!(result.is_err());
        match result.unwrap_err() {
            JsonPathParseError::UnsupportedSegmentType => {},
            _ => panic!("Expected UnsupportedSegmentType error"),
        }
    }

    #[test]
    fn test_filter_expression_error() {
        // 测试过滤器表达式应该返回错误
        let result = parse("$.users[?(@.active)]");
        assert!(result.is_err());
        match result.unwrap_err() {
            JsonPathParseError::UnsupportedSelectorType => {},
            _ => panic!("Expected UnsupportedSelectorType error"),
        }
    }

    #[test]
    fn test_slice_operator_error() {
        // 测试切片操作符应该返回错误
        let result = parse("$.users[0:2]");
        assert!(result.is_err());
        match result.unwrap_err() {
            JsonPathParseError::UnsupportedSelectorType => {},
            _ => panic!("Expected UnsupportedSelectorType error"),
        }
    }

    #[test]
    fn test_invalid_syntax_error() {
        // 测试无效语法应该返回解析错误
        let result = parse("$.[invalid");
        assert!(result.is_err());
        match result.unwrap_err() {
            JsonPathParseError::ParseFailed(_) => {},
            _ => panic!("Expected ParseFailed error"),
        }
    }

    #[test]
    fn test_bracket_notation() {
        // 测试括号表示法
        let result = parse("$['user']['name']").unwrap();
        assert_eq!(result.len(), 2);
        
        match &result[0] {
            JsonPathSegment::Key(key) => assert_eq!(key, "user"),
            _ => panic!("Expected Key segment at index 0"),
        }
        match &result[1] {
            JsonPathSegment::Key(key) => assert_eq!(key, "name"),
            _ => panic!("Expected Key segment at index 1"),
        }
    }

    #[test]
    fn test_mixed_notation() {
        // 测试混合表示法（点号和括号）
        let result = parse("$.user['profile'].settings[0]").unwrap();
        assert_eq!(result.len(), 4);
        
        match &result[0] {
            JsonPathSegment::Key(key) => assert_eq!(key, "user"),
            _ => panic!("Expected Key segment at index 0"),
        }
        match &result[1] {
            JsonPathSegment::Key(key) => assert_eq!(key, "profile"),
            _ => panic!("Expected Key segment at index 1"),
        }
        match &result[2] {
            JsonPathSegment::Key(key) => assert_eq!(key, "settings"),
            _ => panic!("Expected Key segment at index 2"),
        }
        match &result[3] {
            JsonPathSegment::Index(index) => assert_eq!(*index, 0),
            _ => panic!("Expected Index segment at index 3"),
        }
    }

    #[test]
    fn test_large_index() {
        // 测试大索引值
        let result = parse("$.data[999]").unwrap();
        assert_eq!(result.len(), 2);
        
        match &result[1] {
            JsonPathSegment::Index(index) => assert_eq!(*index, 999),
            _ => panic!("Expected Index segment"),
        }
    }

    #[test]
    fn test_special_characters_in_key() {
        // 测试键名中包含特殊字符的情况
        let result = parse("$['user-name']['first_name']").unwrap();
        assert_eq!(result.len(), 2);
        
        match &result[0] {
            JsonPathSegment::Key(key) => assert_eq!(key, "user-name"),
            _ => panic!("Expected Key segment at index 0"),
        }
        match &result[1] {
            JsonPathSegment::Key(key) => assert_eq!(key, "first_name"),
            _ => panic!("Expected Key segment at index 1"),
        }
    }

    #[test]
    fn test_empty_path() {
        // 测试空路径应该返回解析错误
        let result = parse("");
        assert!(result.is_err());
        match result.unwrap_err() {
            JsonPathParseError::ParseFailed(_) => {},
            _ => panic!("Expected ParseFailed error"),
        }
    }
}
