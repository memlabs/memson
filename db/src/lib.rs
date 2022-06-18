use std::collections::HashMap;
use std::sync::Arc;

pub mod json;
pub mod db;

type Result<T> = std::result::Result<T, &'static str>;

pub use db::{Db,Cmd};

use serde::{Deserialize, Serialize};


#[derive(Debug, Deserialize, Serialize)]
pub enum Number {
    Int(i64),
    Float(f64),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Json {
    Number(Number),
    Float(f64),
    Array(JsonVec),
    Map(JsonMap),
    String(String),
}

type JsonMap = HashMap<String, Json>;
type JsonVec = Vec<Json>;

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
}