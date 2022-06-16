use crate::Json;
use serde_json::{Number};


fn scalars_op<F:Fn(i64, i64) -> i64, G:Fn(f64, f64) -> f64>(x: &Number, y: &Number, f: F, g: G) -> Json {
    match (x.as_i64(), y.as_i64()) {
        (Some(x), Some(y)) => Json::from(f(x, y)),
        (Some(x), None) => Json::from(g(x as f64, y.as_f64().unwrap())),
        (None, Some(y)) => Json::from(g(x.as_f64().unwrap(), y as f64)),
        (None, None) => Json::from(g(x.as_f64().unwrap(), y.as_f64().unwrap())),
    }
}

fn vec_vec_op<F:Fn(&Json, &Json) -> Json>(lhs: &[Json], rhs: &[Json], op: F) -> Json {
    Json::Array(lhs.iter().zip(rhs.iter()).map(|(x,y)| op(x, y)).collect())
}

fn vec_scalar_op<F:Fn(&Json, &Json) -> Json>(lhs: &[Json], rhs:&Json, op: F) -> Json {
    Json::Array(lhs.iter().map(|x| op(x, rhs)).collect())
} 

fn scalar_vec_op<F:Fn(&Json, &Json) -> Json>(lhs: &Json, rhs:&[Json], op: F) -> Json {
    Json::Array(rhs.iter().map(|y| op(lhs, y)).collect())
} 

pub fn add(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x + y, &|x,y| x + y),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &add),
        (Json::Array(x), y) => vec_scalar_op(x, y, &add),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &add),
        (Json::String(x), Json::String(y)) => Json::String(x.clone() + y),
        (Json::String(x), y) => Json::String(x.clone() + &y.to_string()),
        (x, Json::String(y)) => Json::String(x.to_string() + &y),
        _ => unimplemented!()
    }
}

pub fn mul(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x * y, &|x,y| x * y),
        _ => unimplemented!()
    }
}

pub fn div(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x / y, &|x,y| x / y),
        _ => unimplemented!()
    }
}

pub fn sub(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x - y, &|x,y| x - y),
        _ => unimplemented!()
    }
}

pub fn avg(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            let total = sum_arr(arr);
            div(&total, &Json::from(arr.len()))
        }
        x@Json::Number(_) => x.clone(),
        _ => unimplemented!()
    }
}


pub fn first(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            if arr.is_empty() {
                Json::Null
            } else {
                arr[0].clone()
            }
        }
        val => val.clone(),
    }
}

pub fn last(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            if arr.is_empty() {
                Json::Null
            } else {
                let pos = arr.len() - 1;
                arr[pos].clone()
            }
        }
        val => val.clone(),
    }
}

pub fn max(val: &Json) -> Json {
    match val {
        Json::Array(_arr) => {
            unimplemented!()
        }
        val => val.clone(),
    }
}

pub fn min(val: &Json) -> Json {
    match val {
        Json::Array(_arr) => {
            unimplemented!()
        }
        val => val.clone(),
    }
}

fn sum_arr(arr: &[Json]) -> Json {
    let mut sum = Json::from(0i64);
    for e in arr {
        sum = add(&sum, e);
    }
    sum
}

pub fn sum(val: &Json) -> Json {
    match val {
        Json::Array(arr) => sum_arr(arr),
        Json::Number(val) => Json::from(val.clone()),
        _ => Json::from(0),
    }
}

fn reduce_sum((mut vec, sum): (Vec<Json>, Json), e: &Json) -> (Vec<Json>, Json) {
    let total = add(&sum, e);
    vec.push(total.clone());
    (vec, total)
}

pub fn sums(val: &Json) -> Json {
    match val {
        Json::Array(arr) =>  {
            let (v, _) = arr.iter().fold((Vec::new(), Json::from(0i64)), reduce_sum);
            Json::Array(v)
        }
        Json::Number(val) => Json::Array(vec![Json::from(val.clone())]),
        _ => Json::Array(vec![Json::from(0)]),
    }
}

pub fn key(k: &str, val: &Json) -> Json {
    match val {
        Json::Array(arr) => Json::Array(arr.iter().map(|e| key(k, e)).collect()),
        Json::Object(obj) => obj.get(k).cloned().unwrap_or(Json::Null),
        _ => Json::Null,
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    

    #[test]
    fn json_add() {
        assert_eq!(add(&json!(1i64), &json!(2i64)), json!(1 + 2));
        assert_eq!(add(&json!(1.1f64), &json!(2.3)), 1.1 + 2.3);
        assert_eq!(add(&json!([1,2,3]), &json!(2)), json!([1+2,2+2,3+2]));
        assert_eq!(add(&json!(2),&json!([1,2,3])), json!([2+1,2+2,2+3]));
        assert_eq!(add(&json!("abc"),&json!("def")), json!("abcdef"));
    }

    #[test]
    fn json_sum() {
        assert_eq!(sum(&json!(1i64)), json!(1));
        assert_eq!(sum(&json!(1.23f64)), json!(1.23));
        assert_eq!(sum(&json!(vec![1i64,2,3])), json!(6));
    }    

    #[test]
    fn json_sums() {
        assert_eq!(sums(&json!(1i64)), json!([1]));
        assert_eq!(sums(&json!(1.23f64)), json!([1.23]));
        assert_eq!(sums(&json!(vec![1i64,2,3])), json!([1, 1+2, 1+2+3]));
    }        

    #[test]
    fn json_key() {
        assert_eq!(key("a", &json!({"a":1,"b":2})), json!(1));
    }
}