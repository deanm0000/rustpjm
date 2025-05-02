use crate::pjmendpoints::PJMEndPoint;
use crate::structs::{az_functions::*, cust::*};
use crate::utils::*;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::{http::StatusCode, Json};
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use object_store::path::Path as obPath;
use std::fmt;
use std::sync::Arc;
#[derive(Clone)]
pub enum Queues {
    DaPrices,
    RtVerf,
    RtUnverf,
}

impl fmt::Display for Queues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Queues::DaPrices => write!(f, "DaPrices"),
            Queues::RtVerf => write!(f, "RtVerf"),
            Queues::RtUnverf => write!(f, "RtUnverf"),
        }
    }
}

impl Queues {
    fn trigger(&self) -> String {
        match &self {
            Queues::DaPrices => String::from("dahrllmps"),
            Queues::RtVerf => String::from("rtfiveminhrllmps"),
            Queues::RtUnverf => String::from("rtunverifiedfiveminlmps"),
        }
    }
    fn storage(&self) -> String {
        match &self {
            Queues::DaPrices => String::from("apidata/da_hrl_lmps/_input/"),
            Queues::RtVerf => String::from("apidata/rt_fivemin_hrl_lmps/_input/"),
            Queues::RtUnverf => String::from("apidata/rt_unverified_fivemin_lmps/_input/"),
        }
    }
    async fn check_queue(&self) -> usize {
        let queue_client = make_queue_client(self.trigger().as_str());
        let meta = queue_client.get_metadata().into_future().await.unwrap();
        meta.approximate_messages_count
    }

    const ALL: [Queues; 3] = [Queues::DaPrices, Queues::RtVerf, Queues::RtUnverf];
}

pub async fn refresher(
    // _queue: Path<String>,
    State(state): State<Arc<AppState>>,
    // _result: Result<Json<FuncRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    let final_resp = OutResponse {
        Outputs: None,
        Logs: None,
        ReturnValue: None,
    };
    eprintln!("refresher");
    for q in Queues::ALL {
        match q.check_queue().await {
            0 => make_next_queue(&q, Arc::clone(&state)).await,
            1 => {
                eprintln!("doing nothing");
            }
            _ => peak_multiple_jobs(&q).await,
        }
    }
    (StatusCode::OK, Json(final_resp))
}

async fn make_next_queue(queue: &Queues, state: Arc<AppState>) {
    eprintln!("make next {} queue", queue);
    let state = Arc::clone(&state);
    let obj_store = Arc::clone(&state.object_store);
    let path = obPath::from(queue.storage());
    let stream = obj_store.list(Some(&path));
    let mut files: Vec<obPath> = Vec::new();

    let mut batches = stream.try_chunks(100);
    while let Some(Ok(chunk)) = batches.next().await {
        for file_meta in chunk {
            files.push(file_meta.location)
        }
    }
    eprintln!("have {} files", files.len());
    match files.len() {
        0 => get_lazy_frame_last_time(queue).await,
        _ => parse_file_list_last_time(queue, &files).await,
    }
}

async fn get_lazy_frame_last_time(queue: &Queues) {
    eprintln!("ignoring get_lazy_frame_last_time {}", queue.trigger())
}

async fn parse_file_list_last_time(queue: &Queues, files: &[obPath]) {
    let last = files
        .iter()
        .filter_map(|path| {
            path.filename()
                .and_then(|filename| filename.get(..20))
                .and_then(|datetime_str| datetime_str.parse::<DateTime<Utc>>().ok())
        })
        .max();
    if last.is_none() {
        eprintln!("{} refresher couldn't parse datetimes {}", queue, files[0]);
        return get_lazy_frame_last_time(queue).await;
    }
    let last = last.unwrap();
    let end_point = PJMEndPoint::from(queue);
    let next_time = last + end_point.next_time;
    let msg = InMsg {
        begin_time: next_time,
        pjm_end_point: end_point,
        queue_next: true,
        last_retry: None,
    };
    put_to_queue(&msg, 0).await.unwrap();
}

async fn peak_multiple_jobs(queue: &Queues) {
    eprintln!("ignoring peak_multiple_jobs {}", queue.trigger())
}
