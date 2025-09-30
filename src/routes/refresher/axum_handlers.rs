use crate::errors::Errors;
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
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Clone, EnumIter, Debug)]
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
    async fn check_queue(&self) -> Result<usize, Errors> {
        let queue_client = make_queue_client(self.trigger().as_str())?;
        let meta = queue_client
            .get_metadata()
            .into_future()
            .await
            .map_err(|_| Errors::QTMeta)?;
        Ok(meta.approximate_messages_count)
    }
    async fn make_next_queue(self, state: Arc<AppState>) -> Result<(), Errors> {
        eprintln!("make next {} queue", self);
        let state = Arc::clone(&state);
        let obj_store = Arc::clone(&state.object_store);
        let path = obPath::from(self.storage());
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
            0 => Ok(self.get_lazy_frame_last_time().await?),
            _ => Ok(self.parse_file_list_last_time(&files).await?),
        }
    }
    async fn get_lazy_frame_last_time(self) -> Result<(), Errors> {
        eprintln!("ignoring get_lazy_frame_last_time {}", self.trigger());
        Ok(())
    }
    async fn parse_file_list_last_time(self, files: &[obPath]) -> Result<(), Errors> {
        let last = files
            .iter()
            .filter_map(|path| {
                path.filename()
                    .and_then(|filename| filename.get(..20))
                    .and_then(|datetime_str| datetime_str.parse::<DateTime<Utc>>().ok())
            })
            .max();
        match last {
            Some(last) => {
                let end_point = PJMEndPoint::from(&self);
                let next_time = last + end_point.next_time;
                let msg = InMsg {
                    begin_time: next_time,
                    pjm_end_point: end_point,
                    queue_next: true,
                    last_retry: None,
                };
                Ok(put_to_queue(&msg, 0).await?)
            }
            None => {
                eprintln!("{} refresher couldn't parse datetimes {}", self, files[0]);
                Ok(self.get_lazy_frame_last_time().await?)
            }
        }
    }
    async fn peak_multiple_jobs(self) {
        eprintln!("ignoring peak_multiple_jobs {}", self.trigger())
    }
}

pub async fn refresher(
    // _queue: Path<String>,
    State(state): State<Arc<AppState>>,
    // _result: Result<Json<FuncRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<impl IntoResponse, Errors> {
    let final_resp = OutResponse {
        Outputs: None,
        Logs: None,
        ReturnValue: None,
    };

    let mut did_nothing = true;
    for q in Queues::iter() {
        match q.check_queue().await? {
            0 => {
                q.make_next_queue(Arc::clone(&state)).await?;
                did_nothing = false;
            }
            1 => {}
            _ => {
                q.peak_multiple_jobs().await;
                did_nothing = false;
            }
        }
    }
    if did_nothing {
        eprintln!("refresher did nothing");
    };
    Ok((StatusCode::OK, Json(final_resp)))
}
