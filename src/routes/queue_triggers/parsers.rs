use super::parser_utils::*;
use crate::{errors::*, routes::queue_triggers::dbfuncs::df_to_db};
use crate::structs::cust::*;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use polars::prelude::*;
use polars_io::cloud::CloudWriter;
use std::cmp;
use tokio::task::{self, JoinHandle};
use uuid::Uuid;
use polars_plan::dsl::*;

pub async fn pjm(in_msg: &InMsg, state: &Arc<AppState>) -> Result<DateTime<Utc>, Errors> {
    let pjm_end_point = &in_msg.pjm_end_point;
    let begin_dt = in_msg.begin_time;
    eprintln!("{:?} {}", &begin_dt, &pjm_end_point.url_suffix);
    let url = format!("{}{}", BASE_URL, &pjm_end_point.url_suffix);
    let (init_resp, total_rows) = pjm_pre_fetch(
        Arc::clone(&state.req_client),
        url.clone(),
        begin_dt,
        pjm_end_point.clone(),
    )
    .await?;
    if total_rows == 0 {
        return Err(Errors::PJM0Rows);
    }
    //TODO: Don't get more than (maybe 10) pages and instead make new queues at new time intervals
    let pages = total_rows.div_ceil(50_000);
    let mut new_starts: Vec<u32> = match pages {
        0..=1 => vec![],
        _ => vec![0; cmp::max(0, pages as usize - 1)],
    };

    for i in 1..pages {
        new_starts[i as usize - 1] = i * 50_000 + 1;
    }

    let mut futures = vec![task::spawn(pjm_first_fetch(
        init_resp,
        pjm_end_point.clone(),
    ))];
    let mut new_futures: Vec<JoinHandle<Result<DataFrame, Errors>>> = new_starts
        .iter()
        .map(|x| {
            task::spawn(pjm_fetch(
                Arc::clone(&state.req_client),
                url.clone(),
                begin_dt,
                *x,
                pjm_end_point.clone(),
            ))
        })
        .collect();
    futures.append(&mut new_futures);
    let results: Vec<Result<Result<DataFrame, Errors>, task::JoinError>> = join_all(futures).await;
    let results_len = &results.len();

    let lfs: Vec<LazyFrame> = results
        .into_iter()
        .filter_map(|res| match res {
            Ok(Ok(df)) => Some(df.lazy()),
            _ => None,
        })
        .collect();
    check_len(&lfs.len(), results_len)?;

    let lf = concat(lfs, CONCAT_ARGS)
    .cust_unwrap()?
    .filter(col("pricedate").eq(col("pricedate").min()));
    // The filter is to keep each file representing no more than 1 day for the rt to da process, it'd be more efficient
    // to change the parameters such that it doesn't download multiple days but that should be a rare occurence
    let next_time: DateTime<Utc> = get_next_time(&lf, pjm_end_point.next_time)?;
    let save_path = format!(
        "apidata/{}/_input/{:?}_{}.parquet",
        pjm_end_point.url_suffix,
        begin_dt,
        Uuid::new_v4()
    );
    let objstore = Arc::clone(&state.object_store);
    let mut df = tokio::task::spawn_blocking(|| lf.collect().expect("final df collect"))
        .await
        .expect("err final df");
    let now = Utc::now();
    df_to_db(&df).await;
    let after = Utc::now();
    let secs = after-now;
    eprintln!("{:?} {} uploading to db took {} secs",&begin_dt, &pjm_end_point.url_suffix,secs.num_seconds());
    let now = Utc::now();
    let mut cloud_writer =
        CloudWriter::new_with_object_store(objstore, save_path.into()).expect("cloud writer");
    ParquetWriter::new(&mut cloud_writer)
        .finish(&mut df)
        .expect("Couldn't write file");
    let after = Utc::now();
    let secs = after-now;
    eprintln!("{:?} {} saving file took {} secs",&begin_dt, &pjm_end_point.url_suffix,secs.num_seconds());
    Ok(next_time)
}

