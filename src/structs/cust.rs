use crate::{errors::*, pjmendpoints::PJMEndPoint};
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InMsgJson {
    pub begin_time_iso_str: String,
    pub pjm_end_point: String,
    pub queue_next: bool,
}

#[derive(Clone)]
pub struct InMsg {
    pub begin_time: DateTime<Utc>,
    pub pjm_end_point: PJMEndPoint,
    pub queue_next: bool,
}
#[derive(Eq, Hash, PartialEq, Deserialize, Serialize, Debug, Clone)]
pub struct RtToDa {
    pub date: String,
    pub endpoint: String,
}
pub struct AppState {
    pub active_tasks: Arc<Mutex<usize>>,
    pub object_store: Arc<dyn ObjectStore>,
    pub req_client: Arc<Client>,
    pub active_combines: Arc<Mutex<HashSet<RtToDa>>>,
    pub combines_add_when_done: Arc<Mutex<HashSet<RtToDa>>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[allow(non_snake_case)]
struct AdjForDST {
    AdjustForDST: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[allow(non_snake_case)]
struct ScheduleStatus {
    Last: String,
    LastUpdated: String,
    Next: String,
}
#[derive(Deserialize, Serialize, Debug, Clone)]
#[allow(non_snake_case)]
pub struct TimerMsg {
    Schedule: AdjForDST,
    ScheduleStatus: Option<ScheduleStatus>,
    IsPastDue: bool,
}

impl TryInto<InMsgJson> for Value {
    type Error = Errors;
    fn try_into(self) -> Result<InMsgJson, Errors> {
        match self {
            Value::String(valstr) => {
                let first = &valstr.chars().next();
                let last = &valstr.chars().last();
                let trimmed = match (first, last) {
                    (Some('\"'), Some('\"')) => &valstr[1..valstr.len() - 1],
                    _ => valstr.as_str(),
                };
                let replaced = trimmed.replace("\\\"", "\"");
                let in_msg: InMsgJson = match serde_json::from_str(replaced.as_str()) {
                    Ok(in_msg) => in_msg,
                    _ => return Err(Errors::QTinMsg),
                };
                Ok(in_msg)
            }
            _ => Err(Errors::FailedDeserialization),
        }
    }
}

impl TryInto<RtToDa> for Value {
    type Error = Errors;
    fn try_into(self) -> Result<RtToDa, Errors> {
        match self {
            Value::String(valstr) => {
                let first = &valstr.chars().next();
                let last = &valstr.chars().last();
                let trimmed = match (first, last) {
                    (Some('\"'), Some('\"')) => &valstr[1..valstr.len() - 1],
                    _ => valstr.as_str(),
                };
                let replaced = trimmed.replace("\\\"", "\"");
                let rt_to_da: RtToDa = match serde_json::from_str(replaced.as_str()) {
                    Ok(rt_to_da) => rt_to_da,
                    _ => return Err(Errors::QTinMsg),
                };
                Ok(rt_to_da)
            }
            _ => Err(Errors::FailedDeserialization),
        }
    }
}

impl TryInto<InMsg> for Value {
    type Error = Errors;
    fn try_into(self) -> Result<InMsg, Errors> {
        let in_msg_json: InMsgJson = self.try_into()?;
        let dt = {
            let begin_time_iso_str = in_msg_json.begin_time_iso_str.clone();
            let begin_naive_dt =
                DateTime::parse_from_str(&begin_time_iso_str, "%Y-%m-%dT%H:%M:%S%z").unwrap();
            begin_naive_dt.with_timezone(&Utc)
        };
        let pjm_end_point: PJMEndPoint = match in_msg_json.pjm_end_point.as_str().try_into() {
            Ok(pjm_end_point) => pjm_end_point,
            _ => return Err(Errors::NoPJMEndPoint),
        };
        Ok(InMsg {
            begin_time: dt,
            pjm_end_point,
            queue_next: in_msg_json.queue_next,
        })
    }
}

impl From<&InMsg> for InMsgJson {
    fn from(val: &InMsg) -> Self {
        let beg_str = val.begin_time.format("%Y-%m-%dT%H:%M:%S%z").to_string();
        let pjm_end_point = val.pjm_end_point.url_suffix.to_string();
        InMsgJson {
            begin_time_iso_str: beg_str,
            pjm_end_point,
            queue_next: val.queue_next,
        }
    }
}
