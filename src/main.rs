mod errors;
mod pjmendpoints;
mod routes;
mod structs;
use axum::{
    routing::{get, post},
    Router,
};
use routes::queue_triggers::axum_handlers::*;
use routes::refresher::axum_handlers::*;
use std::sync::Arc;
use std::{collections::HashSet, env};
use structs::cust::*;
use tokio::sync::Mutex;
mod utils;
use jemallocator::Jemalloc;
use polars_io::pl_async::get_runtime;
use utils::*;

use crate::errors::Errors;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

async fn root() -> &'static str {
    "Hello, World!"
}

fn main() -> Result<(), Errors> {
    Ok(get_runtime().block_on(async_main())?)
}

async fn async_main() -> Result<(), Errors> {
    let state: Arc<AppState> = Arc::new(AppState {
        active_tasks: Arc::new(Mutex::new(0)),
        object_store: Arc::new(make_object_store()?),
        req_client: Arc::new(make_req_client()?),
        active_combines: Arc::new(Mutex::new(HashSet::<RtToDa>::new())),
        combines_add_when_done: Arc::new(Mutex::new(HashSet::<RtToDa>::new())),
    });
    let app = Router::new()
        .route("/", get(root))
        .route("/queueTrigger:queue", post(queue_trigger_wrapper))
        .route("/refresher", post(refresher))
        // .route("/TimerTrigger:queue", post(queue_trigger_wrapper))
        .with_state(state)
        .fallback(not_found);
    let port_key = "FUNCTIONS_CUSTOMHANDLER_PORT";
    let port: u32 = env::var(port_key)
        .or_else(|_| -> Result<String, Errors> { Ok("3000".to_string()) })
        .map_err(|_| Errors::MissingEnvVar(format!("missing {}", port_key)))
        .and_then(|s| {
            s.parse::<u32>()
                .map_err(|_| Errors::MissingEnvVar(format!("can't convert {} to u32", s)))
        })?;

    let bind_address = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .map_err(|e| Errors::Axum(e.to_string()))?;
    axum::serve(listener, app)
        .await
        .map_err(|e| Errors::Axum(e.to_string()))?;
    Ok(())
}
