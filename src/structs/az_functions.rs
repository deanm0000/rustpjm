use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// Azure defined structure
// In
#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, Debug)]
pub struct Sys {
    pub MethodName: String,
    pub UtcNow: String,
    pub RandGuid: String,
}
#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, Debug)]
pub struct MetaData {
    pub DequeueCount: Option<String>,
    pub ExpirationTime: Option<String>,
    pub Id: Option<String>,
    pub InsertionTime: Option<String>,
    pub NextVisibleTime: Option<String>,
    pub PopReceipt: Option<String>,
    pub sys: Sys,
}
#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
pub struct FuncRequest {
    pub Data: HashMap<String, Value>,
    #[allow(dead_code)]
    pub Metadata: MetaData,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, Debug)]
pub struct Timer {
    pub Schedule: Schedule,
    pub ScheduleStatus: Option<String>,
    pub IsPastDue: bool,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, Debug)]
pub struct Schedule {
    pub AdjustForDST: bool,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, Debug)]
pub struct OutResponse {
    pub Outputs: Option<String>,
    pub Logs: Option<String>,
    pub ReturnValue: Option<String>,
}
