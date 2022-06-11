use crate::json::json_sum;
use std::sync::Arc;
use std::collections::BTreeMap;
use crate::Json;
use serde::{Deserialize,Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Cmd {
    #[serde(rename="get")]
    Get(String),
    #[serde(rename="set")]
    Set(String, Box<Cmd>),
    #[serde(rename="val")]
    Val(Json),
    #[serde(rename="sum")]
    Sum(Box<Cmd>)  
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

    pub fn get(&self, key: &str) -> Option<Arc<Json>> {
        self.data.get(key).cloned()
    }

    pub fn set(&mut self, key: String, val: Arc<Json>) -> Option<Arc<Json>> {
        self.data.insert(key, val)
    }

    pub fn eval(&mut self, cmd: Cmd) -> Result<Arc<Json>, String> {
        match cmd {
            Cmd::Get(key) => Ok(self.get(&key).unwrap_or(Arc::new(Json::Null))),
            Cmd::Set(key, arg) => {
                let val = self.eval(*arg)?;
                Ok(self.set(key, val).unwrap_or(Arc::new(Json::Null)))
            }
            Cmd::Val(val) => Ok(Arc::new(val).clone()),
            Cmd::Sum(arg) => {
                let arg = self.eval(*arg)?;
                let val = json_sum(arg.as_ref());
                Ok(Arc::new(Json::from(val)))
            }
        }
    }
}