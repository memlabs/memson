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


pub fn add(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x + y, &|x,y| x + y),
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

pub fn sums(val: &Json) -> Json {
    match val {
        Json::Array(_arr) => unimplemented!(),
        Json::Number(_val) => unimplemented!(),
        _ => Json::from(0),
    }
}

#[cfg(test)]
mod tests {
    use super::{Json, sum};
    
    #[test]
    fn json_sums() {
        assert_eq!(sum(&Json::from(1i64)), 1);
        assert_eq!(sum(&Json::from(1.23f64)), 1.23);
        assert_eq!(sum(&Json::from(vec![1i64,2,3])), 6);
    }
}