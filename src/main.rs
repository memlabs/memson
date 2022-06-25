// actix-web = "4"
mod db;

use actix_web::{HttpResponse};
use actix_web::Responder;
use std::sync::Mutex;
use actix_web::{get, post, web, App, HttpServer};
use db::{Cmd, Db, Json};
use actix_web::http::StatusCode;


struct DbState {
    db: Mutex<Db>
}

impl DbState {
    fn new() -> Self {
        DbState { db: Mutex::new(Db::new()) }
    }
}

#[get("/pulse")]
async fn healthcheck() -> impl Responder {
    HttpResponse::build(StatusCode::OK)
}

#[post("/eval")]
async fn eval(cmd: web::Json<Json>, db_state: web::Data<DbState>) -> impl Responder {
    let mut db = db_state.db.lock().unwrap();
    let cmd = Cmd::parse(cmd.into_inner());
    match db.eval(cmd) {
        Ok(val) => HttpResponse::build(StatusCode::OK).json(val.as_ref()),
        Err(err) => HttpResponse::build(StatusCode::OK).json(err),
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