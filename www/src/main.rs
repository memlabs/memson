// actix-web = "4"
extern crate db;

use std::sync::Arc;
use std::sync::Mutex;
use actix_web::{post, web, App, HttpServer};
use db::{Cmd,Db};




struct DbState {
    db: Mutex<Db>
}

impl DbState {
    fn new() -> Self {
        DbState { db: Mutex::new(Db::new()) }
    }
}

#[post("/eval")]
async fn eval(cmd: web::Json<Cmd>, db_state: web::Data<DbState>) -> String {
    println!("{:?}", cmd);
    let mut db = db_state.db.lock().unwrap();
    match db.eval(cmd.into_inner()) {
         Ok(val) => serde_json::to_string(val.as_ref()).unwrap(),
         Err(val) => val.to_string(),
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