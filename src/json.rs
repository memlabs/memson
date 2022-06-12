use crate::Json;
use serde_json::{Number};

fn add_scalars(x: &Number, y: &Number) -> Json {
    match (x.as_i64(), y.as_i64()) {
        (Some(x), Some(y)) => Json::from(x + y),
        (Some(x), None) => Json::from(x as f64 + y.as_f64().unwrap()),
        (None, Some(y)) => Json::from(x.as_f64().unwrap() + y as f64),
        (None, None) => Json::from(x.as_f64().unwrap() + y.as_f64().unwrap()),
    }
}

pub fn add(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => add_scalars(x, y),
        _ => unimplemented!()
    }
}

pub fn sum(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            let mut sum = Json::from(0i64);
            for e in arr {
                sum = add(&sum, e);
            }
            sum
        }
        Json::Number(val) => Json::from(val.clone()),
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