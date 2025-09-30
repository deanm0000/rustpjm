use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::error::Error;
use std::fmt;

use crate::structs::az_functions::OutResponse;
#[derive(Debug, Clone)]
pub enum Errors {
    Reqwest(String),
    ObjStore(String),
    Axum(String),
    MissingEnvVar(String),
    NoPJMEndPoint,
    HashMapkey,
    PJMTooMany,
    PJMOther,
    PJMNoTotalRowsHeader,
    PJM0Rows,
    QTJson,
    QTinMsg,
    QTToString(String),
    QTMeta,
    DFMakedf,
    BytesErr,
    ResponseErr,
    PlErr(String),
    FailedDeserialization,
    DateTimeParsing,
    Panic(String),
    MakeHeader(String),
    DtParse(String),
    DBCantConnect(String),
    DBexecute(String),
}
impl Error for Errors {}
impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl IntoResponse for Errors {
    fn into_response(self) -> axum::response::Response {
        let final_resp = OutResponse {
            Outputs: None,
            Logs: Some(self.to_string()),
            ReturnValue: None,
        };

        (StatusCode::FAILED_DEPENDENCY, Json(final_resp)).into_response()
    }
}
