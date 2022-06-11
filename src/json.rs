use crate::Json;

pub fn json_sum(val: &Json) -> f64 {
    match val {
        Json::Array(arr) => {
            let mut sum = 0f64;
            for e in arr {
                if let Some(val) = val.as_f64() {
                    sum += val;
                }
            }
            sum
        }
        Json::Number(val) => val.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}