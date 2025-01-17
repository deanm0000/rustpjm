use polars::prelude::*;
use reqwest::Response;
use std::error::Error;
use std::fmt;
#[derive(Debug, Clone)]
pub enum Errors {
    MissingEnvVar,
    NoPJMEndPoint,
    HashMapkey,
    PJMTooMany,
    PJMOther,
    PJMNoTotalRowsHeader,
    PJM0Rows,
    QTJson,
    QTinMsg,
    QTToString,
    DFMakedf,
    BytesErr,
    ResponseErr,
    PlErr,
    FailedDeserialization,
    DateTimeParsing,
    Panic,
}

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unknown error")
    }
}

impl Error for Errors {}

macro_rules! impl_into_result {
    ($traitname:ident, $old_error_type:ty, $new_error_type:expr, $result_type:ty) => {
        pub trait $traitname {
            fn cust_unwrap(self) -> Result<$result_type, Errors>;
        }

        impl $traitname for Result<$result_type, $old_error_type> {
            fn cust_unwrap(self) -> Result<$result_type, Errors> {
                match self {
                    Ok(value) => Ok(value),
                    Err(e) => {
                        eprintln!("{}", e.to_string());
                        return Err($new_error_type);
                    }
                }
            }
        }
    };
}

impl_into_result!(DFres, PolarsError, Errors::PlErr, DataFrame);
impl_into_result!(LFres, PolarsError, Errors::PlErr, LazyFrame);
impl_into_result!(ReqResp, reqwest::Error, Errors::ResponseErr, Response);
