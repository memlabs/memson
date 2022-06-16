use std::sync::Arc;

pub mod json;
pub mod db;

type Result<T> = std::result::Result<T, &'static str>;
type Json = serde_json::Value;

pub use db::{Db,Cmd};

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