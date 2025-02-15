use crate::errors::*;
use crate::pjmendpoints::PJMEndPoint;
use chrono::{DateTime, Duration, Utc};
use chrono_tz::America::New_York;
use num_integer::Integer;
use polars::prelude::*;
use reqwest::{Client, Response, StatusCode};
use std::{collections::HashMap, io::Cursor};

pub const MILLION: i64 = 1_000_000;
pub const BASE_URL: &str = "https://api.pjm.com/api/v1/";
pub const CONCAT_ARGS: UnionArgs = UnionArgs {
    parallel: false,
    rechunk: true,
    to_supertypes: true,
    diagonal: false,
    from_partitioned_ds: false,
    maintain_order: false,
};
pub fn check_len(lfs_len: &usize, results_len: &usize) -> Result<(), Errors> {
    if lfs_len != results_len {
        eprintln!("only {} lfs but there are {} results", lfs_len, results_len);
        Err(Errors::DFMakedf)
    } else {
        Ok(())
    }
}

// pub async fn check_file_exists(objstore: Arc<dyn ObjectStore>, path: &str) -> bool {
//     let head = objstore.head(&Path::from(path)).await;
//     head.is_ok()
// }

// pub fn get_save_targets(
//     lf: &LazyFrame,
//     duration: Duration,
// ) -> Result<(Vec<NaiveDate>, Expr, bool), Errors> {
//     let (unique_expr, expr, is_month) = if duration < Duration::days(1) {
//         (col("pricedate").unique(), col("pricedate"), true)
//     } else {
//         (
//             col("pricedate").dt().truncate(lit("1mo")).unique(),
//             col("pricedate").dt().truncate(lit("1mo")),
//             false,
//         )
//     };

//     let pl = lf
//         .clone()
//         .select(vec![unique_expr])
//         .collect()
//         .cust_unwrap()?;

//     let pl = match pl.column("pricedate") {
//         Ok(column) => column,
//         Err(_) => return Err(Errors::PlErr),
//     };

//     let pl = match pl.date() {
//         Ok(logical) => logical,
//         Err(_) => return Err(Errors::PlErr),
//     };
//     let mut pricedates: Vec<NaiveDate> = vec![];
// pl.into_iter().for_each(|pricedate| {
//     if let Some(a) = pricedate {
//           pricedates.push(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(a as i64))
//          }});

//     Ok((pricedates, expr, is_month))
// }

pub fn make_df(
    cur: Cursor<axum::body::Bytes>,
    pjm_end_point: &PJMEndPoint,
) -> Result<DataFrame, Errors> {
    let df = JsonReader::new(cur)
        .finish()
        .cust_unwrap()?
        .lazy()
        .select(&pjm_end_point.exprs)
        .sort(vec!["utcbegin"], SortMultipleOptions::new())
        .collect()
        .cust_unwrap()?;
    Ok(df)
}
pub fn bool_str(a_bool: bool) -> &'static str {
    match a_bool {
        true => "True",
        false => "False",
    }
}

fn add_ept_day(begin_dt: DateTime<Utc>) -> DateTime<Utc> {
    let ny_dt = begin_dt.with_timezone(&New_York);
    let ny_dt_plus_day = ny_dt + Duration::days(1);
    ny_dt_plus_day.with_timezone(&Utc) - Duration::hours(1)
}
pub fn make_parameters(
    start_row: u32,
    row_is_current: bool,
    begin_dt: DateTime<Utc>,
    pjm_end_point: &PJMEndPoint,
) -> HashMap<String, String> {
    // pjm has a bug where querying the da starting at 05:00 of utc time will ignore the time component and start at 00:00
    let fields = pjm_end_point.columns;
    let begin_str = begin_dt.format("%m-%d-%Y %H:%M").to_string();

    let end_time = match pjm_end_point.default_duration {
        Some(td) => begin_dt + td,
        None => add_ept_day(begin_dt),
    };

    let end_str = end_time.format("%m-%d-%Y %H:%M").to_string();
    let datetime_entry = format!("{} to {}", begin_str, end_str);

    let row_count = 50_000;
    let mut params = HashMap::new();
    params.insert("rowCount".to_string(), row_count.to_string());
    params.insert("StartRow".to_string(), start_row.to_string());
    params.insert("download".to_string(), "True".to_string());
    if row_is_current {
        params.insert(
            "row_is_current".to_string(),
            bool_str(row_is_current).to_string(),
        );
    };
    params.insert("datetime_beginning_utc".to_string(), datetime_entry);
    params.insert("fields".to_string(), fields.to_string());
    params
}

pub async fn pjm_pre_fetch(
    req_client: Arc<Client>,
    url: String,
    begin_dt: DateTime<Utc>,
    pjm_end_point: PJMEndPoint,
) -> Result<(Response, u32), Errors> {
    let row_is_current = !url.contains("unverified");
    let params = make_parameters(1, row_is_current, begin_dt, &pjm_end_point);
    let response = req_client
        .get(url)
        .query(&params)
        .send()
        .await
        .cust_unwrap()?;

    if !response.status().is_success() {
        eprintln!("resp bad {}", response.status());
        let status_code = match response.status() {
            StatusCode::TOO_MANY_REQUESTS => Errors::PJMTooMany,
            _ => Errors::PJMOther,
        };
        let headers = response.headers();
        eprintln!("{:?}", headers);
        if let Ok(txt) = response.text().await {
            eprintln!("{}", txt)
        };
        return Err(status_code);
    };
    eprintln!(
        "prefetch {} {:?} good status",
        pjm_end_point.url_suffix, begin_dt
    );
    let resp_headers = response.headers();

    let total_rows = match resp_headers.get("x-totalrows") {
        Some(total_rows) => total_rows,
        None => return Err(Errors::PJMNoTotalRowsHeader),
    };

    let total_rows = match total_rows.to_str() {
        Ok(tot_rows) => match tot_rows.parse::<u32>() {
            Ok(tot_rows) => tot_rows,
            Err(_) => return Err(Errors::PJMNoTotalRowsHeader),
        },
        Err(_) => return Err(Errors::PJMNoTotalRowsHeader),
    };
    Ok((response, total_rows))
}

pub async fn pjm_first_fetch(
    init_resp: Response,
    pjm_end_point: PJMEndPoint,
) -> Result<DataFrame, Errors> {
    let bytes = handle_bytes(init_resp.bytes().await)?;
    let cursor: Cursor<axum::body::Bytes> = Cursor::new(bytes);
    let df = make_df(cursor, &pjm_end_point)?;
    Ok(df)
}

pub fn get_next_time(lf: &LazyFrame, next_time: Duration) -> Result<DateTime<Utc>, Errors> {
    let df = lf
        .clone()
        .select(vec![col("utcbegin").max() + lit(next_time)])
        .collect()
        .cust_unwrap()?;

    let column = match df.column("utcbegin") {
        Ok(column) => column,
        Err(e) => {
            eprintln!("{}", e);
            return Err(Errors::PlErr);
        }
    };
    let dt = match column.datetime() {
        Ok(logical) => logical,
        Err(e) => {
            eprintln!("{}", e);
            return Err(Errors::PlErr);
        }
    };
    let next_time_ts = match dt.get(0) {
        Some(ts) => ts,
        None => {
            eprintln!("no next time ts");
            return Err(Errors::PlErr);
        }
    };

    let (sec, rem) = next_time_ts.div_rem(&MILLION);
    Ok(DateTime::from_timestamp(sec, (rem * 1000) as u32).expect("couldn't get chrono from int64"))
}

pub async fn pjm_fetch(
    req_client: Arc<Client>,
    url: String,
    begin_dt: DateTime<Utc>,
    start_row: u32,
    pjm_end_point: PJMEndPoint,
) -> Result<DataFrame, Errors> {
    let row_is_current = !url.contains("unverified");
    let params = make_parameters(start_row, row_is_current, begin_dt, &pjm_end_point);
    let response = req_client
        .get(&url)
        .query(&params)
        .send()
        .await
        .cust_unwrap()?;

    if !response.status().is_success() {
        eprintln!(
            "fetch {} {:?} {} bad status {}",
            pjm_end_point.url_suffix,
            begin_dt,
            start_row,
            response.status()
        );
        let status_code = match response.status() {
            StatusCode::TOO_MANY_REQUESTS => Errors::PJMTooMany,
            _ => Errors::PJMOther,
        };
        let headers = response.headers();
        eprintln!("{:?}", headers);
        if let Ok(txt) = response.text().await {
            eprintln!("{}", txt)
        };
        return Err(status_code);
    };
    eprintln!(
        "fetch {} {:?} {} good status",
        pjm_end_point.url_suffix, begin_dt, start_row
    );

    let bytes = handle_bytes(response.bytes().await)?;

    let cursor: Cursor<axum::body::Bytes> = Cursor::new(bytes);
    let df = make_df(cursor, &pjm_end_point)?;
    Ok(df)
}

pub fn handle_bytes(
    bytes: Result<axum::body::Bytes, reqwest::Error>,
) -> Result<axum::body::Bytes, Errors> {
    match bytes {
        Ok(bytes) => Ok(bytes),
        Err(e) => {
            let a = e.to_string();
            eprintln!("error in handle bytes {}", a);
            Err(Errors::BytesErr)
        }
    }
}

// const FIVEMIN: Duration = Duration::minutes(5);
// const ONEDAY: Duration = Duration::days(1);
// pub fn check_no_gaps(df: &DataFrame, next_time: Duration) -> Result<bool, Errors> {
//     let interval = match next_time {
//         FIVEMIN => polars_time::Duration::parse("5m"),
//         ONEDAY => polars_time::Duration::parse("1d"),
//         _ => return Err(Errors::PlErr),
//     };
//     let should_have = df
//         .clone()
//         .lazy()
//         .filter(col("node_id").eq(col("node_id").min()))
//         .select(vec![
//             datetime_range(
//                 col("utcbegin").min(),
//                 col("utcbegin").max(),
//                 interval,
//                 ClosedWindow::Both,
//                 None,
//                 None,
//             )
//             .alias("utcbegin"),
//             lit(true).alias("have"),
//         ]);

//     let join_check = should_have
//         .left_join(df.clone().lazy(), col("utcbegin"), col("utcbegin"))
//         .filter(col("have").is_null())
//         .collect()
//         .cust_unwrap()?;

//     match join_check.shape().0 {
//         0 => Ok(true),
//         _ => {
//             // TODO write function to queue up gaps and make state aware of existing task so they don't all clobber each other
//             // or make parallel allow separate files and use another process to scoop up multiple files.
//             // join_check is filtered so its utcbegin column are the missing intervals so it can be the input
//             Ok(false)
//         }
//     }
// }

// pub fn check_complete(df: &DataFrame, next_time: Duration) -> Result<(), Errors> {
//     let df = df
//         .clone()
//         .lazy()
//         .select(vec![
//             (col("utcbegin").min() - lit(next_time)).alias("prev_time"),
//             ((col("utcbegin")
//                 .min()
//                 .dt()
//                 .convert_time_zone("America/New_York".into())
//                 - lit(next_time))
//             .dt()
//             .date())
//             .neq(col("pricedate"))
//             .alias("good_begin"),
//             ((col("utcbegin")
//                 .max()
//                 .dt()
//                 .convert_time_zone("America/New_York".into())
//                 + lit(next_time))
//             .dt()
//             .date())
//             .neq(col("pricedate"))
//             .alias("good_end"),
//         ])
//         .collect()
//         .cust_unwrap()?;

//     let good_begin = match df.column("good_begin") {
//         Ok(column) => column,
//         Err(e) => {
//             eprintln!("{}", e);
//             return Err(Errors::PlErr);
//         }
//     };

//     let good_begin = match good_begin.bool() {
//         Ok(ca) => ca,
//         Err(e) => {
//             eprintln!("{}", e);
//             return Err(Errors::PlErr);
//         }
//     };

//     let good_begin = match good_begin.get(0) {
//         Some(raw) => raw,
//         None => {
//             eprintln!("no raw");
//             return Err(Errors::PlErr);
//         }
//     };

//     let good_end = match df.column("good_end") {
//         Ok(column) => column,
//         Err(e) => {
//             eprintln!("{}", e);
//             return Err(Errors::PlErr);
//         }
//     };

//     let good_end = match good_end.bool() {
//         Ok(ca) => ca,
//         Err(e) => {
//             eprintln!("{}", e);
//             return Err(Errors::PlErr);
//         }
//     };

//     let good_end = match good_end.get(0) {
//         Some(raw) => raw,
//         None => {
//             eprintln!("no raw");
//             return Err(Errors::PlErr);
//         }
//     };

//     match (good_begin, good_end) {
//         (true, true) => {
//             match check_no_gaps(&df, next_time)? {
//                 true => {
//                     // queue file aggregator
//                 }
//                 false => {
//                     // do nothing, when the missing gaps are called it'll eventually find gap free and hit true
//                 }
//             }
//         }
//         (false, _) => {
//             //gap filler
//         }
//         (_, false) => {
//             //do nothing
//         }
//     }
//     Ok(())
// }
