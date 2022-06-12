mod db;
mod json;

use actix_web::Responder;
use std::sync::Arc;
use std::sync::Mutex;
use actix_web::{post, web, App, HttpServer};
use db::{Cmd, Db};

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

type Result<T> = std::result::Result<T, &'static str>;
type Json = serde_json::Value;

struct DbState {
    db: Mutex<Db>
}

impl DbState {
    fn new() -> Self {
        DbState { db: Mutex::new(Db::new()) }
    }
}

#[post("/eval")]
async fn eval(cmd: web::Json<Cmd>, db_state: web::Data<DbState>) -> impl Responder {
    println!("{:?}", cmd);
    let mut db = db_state.db.lock().unwrap();
    match db.eval(cmd.into_inner()) {
        Ok(JsonVal::Val(val)) => web::Json(val),
        Ok(JsonVal::Arc(val)) =>  web::Json(val.as_ref().clone()),
        _ => unimplemented!()
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_state = web::Data::new(DbState::new());
    HttpServer::new(move || {
        App::new()
            .app_data(db_state.clone())
            .service(eval)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}