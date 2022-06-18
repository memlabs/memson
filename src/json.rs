
use std::sync::Arc;
use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
pub use serde_json::Value as Json; 
use serde_json::Number;
use serde_json::json;
use serde_json::Map;

#[derive(Debug, Serialize, Deserialize)]
pub enum Cmd {
    #[serde(rename="+")]
    Add(Box<Cmd>, Box<Cmd>),    
    #[serde(rename="avg")]
    Avg(Box<Cmd>),
    #[serde(rename="/")]
    Div(Box<Cmd>, Box<Cmd>),
    #[serde(rename="eval")]
    Eval(Json),
    #[serde(rename="==")]
    Eq(Box<Cmd>, Box<Cmd>),      
    #[serde(rename="first")]
    First(Box<Cmd>),            
    #[serde(rename="get")]
    Get(String),
    #[serde(rename="if")]
    If(Box<Cmd>, Box<Cmd>, Box<Cmd>),    
    #[serde(rename="key")]
    Key(String, Box<Cmd>),
    #[serde(rename="last")]    
    Last(Box<Cmd>),  
    #[serde(rename="len")]    
    Len(Box<Cmd>),   
    #[serde(rename="lt")]    
    Lt(Box<Cmd>, Box<Cmd>),     
    #[serde(rename="max")]
    Max(Box<Cmd>),        
    #[serde(rename="min")]
    Min(Box<Cmd>),
    #[serde(rename="*")]
    Mul(Box<Cmd>, Box<Cmd>), 
    #[serde(rename="!=")]
    NotEq(Box<Cmd>, Box<Cmd>),        
    #[serde(rename="set")]
    Set(String, Box<Cmd>),
    #[serde(rename="-")]
    Sub(Box<Cmd>, Box<Cmd>),    
    #[serde(rename="sum")]
    Sum(Box<Cmd>),
    #[serde(rename="sums")]
    Sums(Box<Cmd>),   
    #[serde(rename="val")]
    Val(Json),
}

fn parse_unr_cmd<F: Fn(Box<Cmd>) -> Cmd>(f: F, arg: Json) -> Cmd {
    f(parse_arg(arg))
}

fn parse_bin_cmd<F: Fn(Box<Cmd>, Box<Cmd>) -> Cmd>(f: F, val: Json) -> Cmd {
    match val {
        Json::Array(mut arr) if arr.len() == 2 => {
            let rhs = parse_arg(arr.remove(1));
            let lhs = parse_arg(arr.remove(0));
            f(lhs, rhs)
        }
        val => Cmd::Val(val),
    }
}

fn parse_tern_cmd<F: Fn(Box<Cmd>, Box<Cmd>, Box<Cmd>) -> Cmd>(f: F, val: Json) -> Cmd {
    match val {
        Json::Array(mut arr) if arr.len() == 3 => {
            let z = parse_arg(arr.remove(2));
            let y = parse_arg(arr.remove(1));
            let x = parse_arg(arr.remove(0));
            f(x, y, z)
        }
        val => Cmd::Val(val),
    }
}

fn parse_arg(val: Json) -> Box<Cmd> {
    Box::new(Cmd::parse(val))
}

fn parse_get(val: Json) -> Cmd {
    match val {
        Json::String(s) => Cmd::Get(s),
        val => Cmd::Val(val),
    }
}

fn parse_op<F:Fn(String, Box<Cmd>) -> Cmd>(f: F, val: Json) -> Cmd {
    match val {
        Json::Array(mut arr) if arr.len() == 2 => {
            let arg = arr.remove(1);
            let key = match arr.remove(0) {
                Json::String(s) => s,
                val => return Cmd::Val(Json::Array(vec![val, arg])),
            };
            f(key, parse_arg(arg))
        }
        val => Cmd::Val(val),
    }
}

fn parse_key(val: Json) -> Cmd {
    parse_op(Cmd::Key, val)
}

fn parse_set(val: Json) -> Cmd {
    parse_op(Cmd::Set, val)
}

impl Cmd {
    pub fn parse(val: Json) -> Cmd {
        match val {
            Json::Object(obj) => {
                if obj.len() == 1 {
                    let mut it = obj.into_iter();
                    let (key, val) = it.next().unwrap();
                    match key.as_str() {
                        "+" => parse_bin_cmd(Cmd::Add, val),
                        "-" => parse_bin_cmd(Cmd::Sub, val),
                        "*" => parse_bin_cmd(Cmd::Mul, val),
                        "/" => parse_bin_cmd(Cmd::Div, val),
                        "==" => parse_bin_cmd(Cmd::Eq, val),
                        "!=" => parse_bin_cmd(Cmd::NotEq, val),
                        "<" => parse_bin_cmd(Cmd::Lt, val),
                        "avg" => parse_unr_cmd(Cmd::Avg, val),
                        "div" => parse_bin_cmd(Cmd::Div, val),
                        "eval" => Cmd::Eval(val),
                        "first" => parse_unr_cmd(Cmd::First, val),
                        "get" => parse_get(val),
                        "if" => parse_tern_cmd(Cmd::If, val),
                        "key" => parse_key(val),
                        "last" => parse_unr_cmd(Cmd::Last, val),
                        "len" => parse_unr_cmd(Cmd::Len, val),
                        "max" => parse_unr_cmd(Cmd::Max, val),
                        "min" => parse_unr_cmd(Cmd::Min, val),
                        "mul" => parse_bin_cmd(Cmd::Mul, val),
                        "set" => parse_set(val),
                        "sub" => parse_bin_cmd(Cmd::Sub, val),
                        "sum" => parse_unr_cmd(Cmd::Sum, val),
                        "sums" => parse_unr_cmd(Cmd::Sums, val),
                        "val" => Cmd::Val(val),
                        _ => Cmd::Val(json!({key: val}))
                    }
                } else {
                    Cmd::Val(Json::Object(obj))
                }
            }
            val => Cmd::Val(val),
        }
    }
}

pub struct Db {
    data: BTreeMap<String, Arc<Json>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    fn get(&self, key: &str) -> Option<Arc<Json>> {
        self.data.get(key).cloned()
    }

    fn get_val(&self, key: &str) -> JsonVal {
        self.get(&key).map(JsonVal::Arc).unwrap_or(JsonVal::Val(Json::Null))
    }
    
    fn set(&mut self, key: String, val: Arc<Json>) -> Option<Arc<Json>> {
        self.data.insert(key, val)
    }

    fn set_val(&mut self, key: String, arg: Cmd) -> JsonVal {
        let val = self.eval(arg);
        self.set(key, val.to_arc()).map(JsonVal::Arc).unwrap_or(JsonVal::Arc(Arc::new(Json::Null)))
    }    

    pub fn eval(&mut self, cmd: Cmd) -> JsonVal {
        match cmd {
            Cmd::Add(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &add),
            Cmd::Avg(arg) => self.eval_unary_cmd(*arg, &avg),
            Cmd::Div(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &div),
            Cmd::Eval(arg) => self.eval_eval_cmd(arg),
            Cmd::Eq(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &eq),
            Cmd::First(arg) => self.eval_unary_cmd(*arg, &first),
            Cmd::Get(key) => self.get_val(&key),
            Cmd::If(pred, lhs, rhs) => self.eval_if(*pred, *lhs, *rhs),
            Cmd::Key(key, arg) => {
                let val = self.eval(*arg);
                JsonVal::Val(self.eval_key(&key, val.as_ref()))
            }
            Cmd::Last(arg) => self.eval_unary_cmd(*arg, &last),
            Cmd::Len(arg) => self.eval_unary_cmd(*arg, &len),
            Cmd::Lt(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &lt),
            Cmd::Max(arg) => self.eval_unary_cmd(*arg, &max),
            Cmd::Min(arg) => self.eval_unary_cmd(*arg, &min),
            Cmd::Mul(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &mul),
            Cmd::NotEq(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &not_eq),
            Cmd::Set(key, arg) => self.set_val(key, *arg),
            Cmd::Sub(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &sub),
            Cmd::Sum(arg) => self.eval_unary_cmd(*arg, &sum),
            Cmd::Sums(arg) => self.eval_unary_cmd(*arg, &sums),
            Cmd::Val(val) => JsonVal::Val(val),
        }
    }

    fn eval_key(&self, key: &str, val: &Json) -> Json {
        json_key(key, val)
    }

    fn eval_eval_cmd(&mut self, arg: Json) -> JsonVal {
        match arg {
            Json::Array(arr) => {
                let mut out = Vec::with_capacity(arr.len());
                for e in arr {
                    let cmd = Cmd::parse(e);
                    let val = self.eval(cmd).to_json();
                    out.push(val);
                }
                JsonVal::Val(Json::Array(out))
            }
            Json::Object(obj) => {
                let mut out = Map::new();
                for (key, val) in obj {
                    let cmd = Cmd::parse(val);
                    out.insert(key, self.eval(cmd).to_json());
                }
                JsonVal::Val(Json::Object(out))
            }
            val => JsonVal::Val(val)  
        }
    }

    fn eval_unary_cmd<F:Fn(&Json) -> Json>(&mut self, arg: Cmd, f: F) -> JsonVal {
        let val = self.eval(arg);
        JsonVal::Val(f(val.as_ref()))
    }

    fn eval_binary_cmd<F:Fn(&Json, &Json) -> Json>(&mut self, lhs: Cmd, rhs: Cmd, f: F) -> JsonVal {
        let (x, y)  = (self.eval(lhs), self.eval(rhs));
        JsonVal::Val(f(x.as_ref(), y.as_ref()))
    }

    fn eval_if(&mut self, pred: Cmd, lhs: Cmd, rhs: Cmd) -> JsonVal {
        match self.eval(pred).as_ref() {
            Json::Bool(true) => self.eval(lhs),
            _ => self.eval(rhs),
        }
    }
}

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


// fn vec_op<F:Fn(&Json, &Json) -> Json>(x: &Json, y: &Json, op: F) -> Json {
//     match (x, y) {
//         (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, op),
//         (Json::Array(x), y) => vec_scalar_op(x, y, op),
//         (x, Json::Array(y)) => scalar_vec_op(x, y, op),
//         (x, y) => op(x, y)
//     }
// }


pub fn add(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x + y, &|x,y| x + y),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &add),
        (Json::Array(x), y) => vec_scalar_op(x, y, &add),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &add),
        (Json::String(x), Json::String(y)) => Json::String(x.clone() + y),
        (Json::String(x), Json::Number(y)) => Json::String(x.to_string() + &y.to_string()),
        (Json::Number(lhs), Json::String(rhs)) => Json::String(lhs.to_string() + rhs),
        _ => Json::Null
    }
}

pub fn mul(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x * y, &|x,y| x * y),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &mul),
        (Json::Array(x), y) => vec_scalar_op(x, y, &mul),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &mul),
        _ => Json::Null,
    }
}

pub fn div(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x * y, &|x,y| x / y),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &div),
        (Json::Array(x), y) => vec_scalar_op(x, y, &div),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &div),
        _ => Json::Null,
    }
}

pub fn sub(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => scalars_op(x, y, &|x,y| x + y, &|x,y| x - y),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &sub),
        (Json::Array(x), y) => vec_scalar_op(x, y, &sub),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &sub),
        _ => Json::Null
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

pub fn eq(x: &Json, y: &Json) -> Json {
    match (x,y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(x.iter().zip(y.iter()).map(|(x,y)| eq(x,y)).collect()),
        (Json::Array(x), y) => Json::Array(x.iter().map(|x| eq(x, y)).collect()),
        (x, Json::Array(y)) => Json::Array(y.iter().map(|y| eq(x, y)).collect()),
        (x, y) => Json::Bool(x == y)
    }
}


pub fn not_eq(x: &Json, y: &Json) -> Json {
    match (x,y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(x.iter().zip(y.iter()).map(|(x,y)| not_eq(x,y)).collect()),
        (Json::Array(x), y) => Json::Array(x.iter().map(|x| not_eq(x, y)).collect()),
        (x, Json::Array(y)) => Json::Array(y.iter().map(|y| not_eq(x, y)).collect()),
        (x, y) => Json::Bool(x != y)
    }
}


//TODO (make generic)
fn num_lt(x: &Number, y: &Number) -> bool {
    let val = match (x.as_i64(), y.as_i64()) {
        (Some(x), Some(y)) => x < y,
        (Some(x), None) => {
            let lhs = x as f64;
            let rhs = y.as_f64().unwrap();
            lhs < rhs
        }
        (None, Some(y)) => {
            let lhs = x.as_f64().unwrap();
            let rhs = y as f64;
            lhs < rhs
        }
        (None, None) => x.as_f64().unwrap() < y.as_f64().unwrap()
    };
    val
}

fn num_gt(x: &Number, y: &Number) -> bool {
    match (x.as_i64(), y.as_i64()) {
        (Some(x), Some(y)) => x < y,
        (Some(x), None) => {
            let lhs = x as f64;
            let rhs = y.as_f64().unwrap();
            lhs > rhs
        }
        (None, Some(y)) => {
            let lhs = x.as_f64().unwrap();
            let rhs = y as f64;
            lhs > rhs
        }
        (None, None) => x.as_f64().unwrap() > y.as_f64().unwrap()
    }
}

pub fn lt(x: &Json, y: &Json) -> Json {
    match (x,y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(x.iter().zip(y.iter()).map(|(x,y)| lt(x,y)).collect()),
        (Json::Array(x), y) => Json::Array(x.iter().map(|e| lt(e, y)).collect()),
        (x, Json::Array(y)) => Json::Array(y.iter().map(|e| lt(x, e)).collect()),
        (Json::String(x), Json::String(y)) => Json::Bool(x < y),
        (Json::Number(x), Json::Number(y)) => Json::Bool(num_lt(x, y)),
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

fn last(val: &Json) -> Json {
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

fn len(val: &Json) -> Json {
    match val {
        Json::Array(arr) => Json::from(arr.len()),
        _ => Json::from(1),
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

pub fn json_key(k: &str, val: &Json) -> Json {
    match val {
        Json::Array(arr) => Json::Array(arr.iter().map(|e| json_key(k, e)).collect()),
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
    fn json_key_ok() {
        assert_eq!(json_key("a", &json!({"a":1,"b":2})), json!(1));
    }
}

pub enum JsonVal {
    Val(Json),
    Arc(Arc<Json>),
}

impl JsonVal {
    pub fn as_ref(&self) -> &Json {
        match self {
            JsonVal::Val(val) => val,
            JsonVal::Arc(val) => val.as_ref(),
        }
    }

    pub fn to_arc(self) -> Arc<Json> {
        match self {
            JsonVal::Val(val) => Arc::new(val),
            JsonVal::Arc(val) => val.clone(),
        }
    }

    pub fn to_json(self) -> Json {
        match self {
            JsonVal::Arc(val) => val.as_ref().clone(),
            JsonVal::Val(val) => val,
        }
    }
}