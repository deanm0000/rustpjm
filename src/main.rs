mod errors;
mod pjmendpoints;
mod routes;
mod structs;
use axum::{
    routing::{get, post},
    Router,
};
use routes::queue_triggers::axum_handlers::*;
use std::sync::Arc;
use std::{collections::HashSet, env};
use structs::cust::*;
use tokio::sync::Mutex;
mod utils;
use jemallocator::Jemalloc;
use utils::*;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

async fn root() -> &'static str {
    "Hello, World!"
}

#[tokio::main]
async fn main() {
    let state: Arc<AppState> = Arc::new(AppState {
        active_tasks: Arc::new(Mutex::new(0)),
        object_store: Arc::new(make_object_store()),
        req_client: Arc::new(make_req_client()),
        active_combines: Arc::new(Mutex::new(HashSet::<RtToDa>::new())),
        combines_add_when_done: Arc::new(Mutex::new(HashSet::<RtToDa>::new())),
    });
    let app = Router::new()
        .route("/", get(root))
        .route("/queueTrigger:queue", post(queue_trigger_wrapper))
        // .route("/TimerTrigger:queue", post(queue_trigger_wrapper))
        .with_state(state)
        .fallback(not_found);
    let port_key = "FUNCTIONS_CUSTOMHANDLER_PORT";
    let port: u16 = match env::var(port_key) {
        Ok(val) => val.parse().expect("Custom Handler port is not a number!"),
        Err(_) => 3000,
    };
    let bind_address = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .expect("binding listener");
    axum::serve(listener, app).await.expect("serving axum");
}
