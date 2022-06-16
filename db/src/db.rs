use crate::JsonVal;
use crate::json;
use std::sync::Arc;
use std::collections::BTreeMap;
use crate::{Result,Json};
use serde::{Deserialize,Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Cmd {
    #[serde(rename="+")]
    Add(Box<Cmd>, Box<Cmd>),    
    #[serde(rename="avg")]
    Avg(Box<Cmd>),
    #[serde(rename="/")]
    Div(Box<Cmd>, Box<Cmd>),
    #[serde(rename="first")]
    First(Box<Cmd>),            
    #[serde(rename="get")]
    Get(String),
    #[serde(rename="last")]
    Last(Box<Cmd>),  
    #[serde(rename="max")]
    Max(Box<Cmd>),        
    #[serde(rename="min")]
    Min(Box<Cmd>),
    #[serde(rename="*")]
    Mul(Box<Cmd>, Box<Cmd>),    
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

    fn get_val(&self, key: &str) -> Result<JsonVal> {
        Ok(self.get(&key).map(JsonVal::Arc).unwrap_or(JsonVal::Val(Json::Null)))
    }
    
    fn set(&mut self, key: String, val: Arc<Json>) -> Option<Arc<Json>> {
        self.data.insert(key, val)
    }

    fn set_val(&mut self, key: String, arg: Cmd) -> Result<JsonVal> {
        let val = self.eval(arg)?;
        let old_val = self.set(key, val.to_arc()).map(JsonVal::Arc);
        Ok(old_val.unwrap_or(JsonVal::Val(Json::Null)))
    }    

    pub fn eval(&mut self, cmd: Cmd) -> Result<JsonVal> {
        match cmd {
            Cmd::Add(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &json::add),
            Cmd::Avg(arg) => self.eval_unary_cmd(*arg, &json::avg),
            Cmd::Div(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &json::div),
            Cmd::First(arg) => self.eval_unary_cmd(*arg, &json::first),
            Cmd::Get(key) => self.get_val(&key),
            Cmd::Last(arg) =>self.eval_unary_cmd(*arg, &json::last),
            Cmd::Max(arg) => self.eval_unary_cmd(*arg, &json::max),
            Cmd::Min(arg) => self.eval_unary_cmd(*arg, &json::min),
            Cmd::Mul(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &json::mul),
            Cmd::Set(key, arg) => self.set_val(key, *arg),
            Cmd::Sub(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, &json::sub),
            Cmd::Sum(arg) => self.eval_unary_cmd(*arg, &json::sum),
            Cmd::Sums(arg) => self.eval_unary_cmd(*arg, &json::sums),
            Cmd::Val(val) => Ok(JsonVal::Val(val)),
        }
    }

    fn eval_unary_cmd<F:Fn(&Json) -> Json>(&mut self, arg: Cmd, f: F) -> Result<JsonVal> {
        let val = self.eval(arg)?;
        Ok(JsonVal::Val(f(val.as_ref())))
    }

    fn eval_binary_cmd<F:Fn(&Json, &Json) -> Json>(&mut self, lhs: Cmd, rhs: Cmd, f: F) -> Result<JsonVal> {
        let (x, y)  = (self.eval(lhs)?, self.eval(rhs)?);
        Ok(JsonVal::Val(f(x.as_ref(), y.as_ref())))
    }
}

