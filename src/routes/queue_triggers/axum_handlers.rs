use super::parsers::*;
use crate::errors::*;
use crate::routes::timer_triggers::rt_to_day::parse_rt_to_da_starter;
use crate::structs::{az_functions::*, cust::*};
use crate::utils::*;
use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;

async fn queue_trigger(
    _queue: String,
    state: &Arc<AppState>,
    result: Result<Json<FuncRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<StatusCode, Errors> {
    // the wrapper will decrement the active_tasks count
    let result = match result {
        Ok(result) => result,
        Err(_) => return Err(Errors::QTJson),
    };
    let data = &result.Data;
    let (_key, val) = match data.iter().next() {
        Some((key, val)) => (key, val),
        _ => {
            return Err(Errors::HashMapkey);
        }
    };
    eprintln!("{:?}", val);
    let in_msg: Result<InMsg, Errors> = val.clone().try_into();

    let res = match in_msg {
        Ok(in_msg) => {
            let next_time = pjm(&in_msg, state).await;
            match next_time {
                Ok(next_time) => {
                    if in_msg.queue_next {
                        let new_queue_item = &InMsg {
                            begin_time: next_time,
                            pjm_end_point: in_msg.pjm_end_point.clone(),
                            queue_next: true,
                        };
                        let when_next_expected = in_msg.pjm_end_point.expected(next_time);
                        put_to_queue(new_queue_item, when_next_expected).await?;
                    };
                }
                Err(Errors::PJM0Rows) => {
                    let pjm_end_point = in_msg.pjm_end_point.clone();
                    let when_next_res = pjm_end_point.expected(in_msg.begin_time);
                    eprintln!(
                        "got 0 rows will add new item to queue in {} sec",
                        when_next_res
                    );
                    put_to_queue(&in_msg, when_next_res).await?;
                }
                Err(e) => {
                    eprintln!("{:?}", e);
                }
            }

            Ok(StatusCode::OK)
        }
        Err(e) => Err(e),
    };

    if res.is_ok() {
        return res;
    };

    eprintln!("not in_msg");

    let rt_to_da: Result<RtToDa, Errors> = val.clone().try_into();

    match rt_to_da {
        Ok(rt_to_da) => parse_rt_to_da_starter(rt_to_da, Arc::clone(state)).await,
        Err(e) => Err(e),
    }

    // let timer_msg: Result<TimerMsg, Errors> = val.clone().try_into();
    // let res = timer_trigger(key, &state).await;
    // let res = match timer_msg {
    //     Ok(_) => {
    //         eprintln!("in match");
    //         let a=timer_trigger(key, &state).await;
    //         eprintln!("after timer");
    //         a},
    //     Err(e) => Err(e),
    // };
}

pub async fn queue_trigger_wrapper(
    Path(queue): Path<String>,
    State(state): State<Arc<AppState>>,
    result: Result<Json<FuncRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    {
        let mut active_tasks = state.active_tasks.lock().await;
        *active_tasks += 1;
        eprintln!("Active tasks: {}", active_tasks);
        // I want this next copy because I don't want to hold the lock, I just want what it was at the start
        // #[allow(clippy::clone_on_copy)]
        // active_tasks.clone()
    };

    let final_resp = OutResponse {
        Outputs: None,
        Logs: None,
        ReturnValue: None,
    };

    let pre_code = panic::catch_unwind(AssertUnwindSafe(|| async {
        queue_trigger(queue, &state, result).await
    }));
    // let pre_code = queue_trigger(queue, &state, result).await;
    {
        let mut active_tasks = state.active_tasks.lock().await;
        *active_tasks -= 1;
    };
    // let status = match pre_code {
    //     Ok(res) => res,
    //     Err(Errors::QTJson) => StatusCode::CONFLICT,
    //     Err(Errors::HashMapkey) => StatusCode::FAILED_DEPENDENCY,
    //     _ => StatusCode::GONE,
    // };
    let status = match pre_code {
        Ok(code) => match code.await {
            Ok(res) => res,
            Err(Errors::QTJson) => StatusCode::CONFLICT,
            Err(Errors::HashMapkey) => StatusCode::FAILED_DEPENDENCY,
            _ => StatusCode::GONE,
        },
        Err(_) => StatusCode::GATEWAY_TIMEOUT,
    };

    (status, Json(final_resp))
}

pub async fn not_found(OriginalUri(uri): OriginalUri, body: Bytes) -> (StatusCode, String) {
    // Print the requested URL
    eprintln!("404 - Route not found for path: {}", uri);
    let body_string = String::from_utf8_lossy(&body).to_string();
    let parsed: Result<FuncRequest, serde_json::Error> = serde_json::from_slice(&body);
    if parsed.is_ok() {
        eprintln!("parsed {:?}", parsed);
    }

    eprintln!("Body content: {}", body_string);
    // Print the body (if any)

    // Respond with 404 Not Found and a custom message
    (StatusCode::NOT_FOUND, format!("404 Not Found: {}", uri))
}
