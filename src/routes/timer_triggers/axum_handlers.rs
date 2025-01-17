// use crate::errors::Errors;
// use crate::pjmendpoints::PJMEndPoint;
// use crate::AppState;
// use axum::http::StatusCode;
// use chrono::{Duration, NaiveDate};
// use cloud::CloudWriter;
// use futures::StreamExt;
// use object_store::path::Path;
// use object_store::ObjectStore;
// use polars::prelude::ScanArgsParquet;
// use polars::prelude::*;
// use sqlx::Execute;
// use sqlx::QueryBuilder;
// use sqlx::{Pool, Postgres};
// use std::collections::HashSet;
// use std::env;
// use std::sync::Arc;
// const FILE_PATHS: &str = "file_paths";
// #[derive(sqlx::Type, Debug, Clone)]
// #[sqlx(type_name = "pjm_nodes", rename_all = "lowercase")]
// enum PjmNodes {
//     Fern,
//     Camden,
//     Dom,
// }

// impl From<String> for PjmNodes {
//     fn from(val: String) -> Self {
//         let mut v = val.as_str();
//         if v.starts_with("\"") {
//             v = &v[1..];
//         }
//         if v.ends_with("\"") {
//             v = &v[..v.len() - 1];
//         }
//         match v {
//             "fern" => PjmNodes::Fern,
//             "camden" => PjmNodes::Camden,
//             "dom" => PjmNodes::Dom,
//             _ => {
//                 eprintln!("got {}", v);
//                 panic!("bad value")
//             }
//         }
//     }
// }

// #[derive(Debug, Clone)]
// struct RtPrices {
//     node: PjmNodes,
//     utcbegin: String,
//     pricedate: String,
//     hour: i32,
//     rtlmp: f64,
//     rtmcc: f64,
//     rtmcl: f64,
// }

// #[derive(Debug, Clone)]
// struct DaPrices {
//     node: PjmNodes,
//     utcbegin: String,
//     pricedate: String,
//     hour: i32,
//     dalmp: f64,
//     damcc: f64,
//     damcl: f64,
// }

// pub async fn timer_trigger(
//     timer_key: &String,
//     state: &Arc<AppState>,
// ) -> Result<StatusCode, Errors> {
//     eprintln!("started func");
//     let timer_key = if timer_key == &"rtfiveminhrllmps".to_string() {
//         "rt_fivemin_hrl_lmps".to_string()
//     } else if timer_key == &"rtunverifiedfiveminlmps".to_string() {
//         "rt_unverified_fivemin_lmps".to_string()
//     } else if timer_key == &"dahrllmps".to_string() {
//         "da_hrl_lmps".to_string()
//     } else {
//         return Err(Errors::FailedDeserialization);
//     };
//     eprintln!("made new timer_key {}", &timer_key);

//     let is_working = {
//         let mut active_combines = state.active_combines.lock().await;
//         let does_contain = active_combines.contains(timer_key.as_str());
//         if !does_contain {
//             active_combines.insert(timer_key.clone());
//         }
//         does_contain
//     };
//     eprintln!("checked is_working");
//     if is_working {
//         return Ok(StatusCode::OK);
//     };

//     let pjm_end_point: PJMEndPoint = timer_key.as_str().try_into()?;
//     let input_dir = format!("abfs://pjm/apidata/{}/_input/*.parquet", timer_key);
//     let mut args = ScanArgsParquet::default();
//     args.include_file_paths = Some(FILE_PATHS.into());
//     args.parallel = ParallelStrategy::None;
//     let existing_lf = LazyFrame::scan_parquet(input_dir, args).unwrap();
//     eprintln!("got existing_lf");
//     let existing_lf_copy = existing_lf.clone();
//     let schema = tokio::task::spawn_blocking(move || {
//         existing_lf_copy
//             .drop(vec![FILE_PATHS])
//             .collect_schema()
//             .unwrap()
//     })
//     .await
//     .unwrap();
//     eprintln!("got schema");
//     let existing_lf_copy = existing_lf.clone();
//     let pricedates_df = tokio::task::spawn_blocking(move || {
//         existing_lf_copy
//             .select(vec![col("pricedate").unique()])
//             .collect()
//             .unwrap()
//     })
//     .await
//     .unwrap();
//     eprintln!("got pricedates_df");
//     let pricedates_col = pricedates_df.column("pricedate").unwrap();

//     let mut pricedates: Vec<NaiveDate> = vec![];

//     pricedates_col
//         .date()
//         .unwrap()
//         .into_iter()
//         .for_each(|pricedate| match pricedate {
//             Some(pd) => pricedates
//                 .push(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(pd as i64)),
//             None => {}
//         });
//     eprintln!("got pricedates vec {}", &pricedates.len());
//     let mut union_args = UnionArgs::default();
//     union_args.parallel = false;
//     union_args.rechunk = true;
//     for pricedate in pricedates {
//         let existing_lf_copy = existing_lf.clone();
//         let new_day_df = tokio::task::spawn_blocking(move || {
//             existing_lf_copy
//                 .filter(col("pricedate").eq(lit(pricedate)))
//                 .collect()
//                 .unwrap()
//         })
//         .await
//         .unwrap();
//         eprintln!("got new_day_df");
//         let input_files = new_day_df
//             .clone()
//             .lazy()
//             .select(vec![col(FILE_PATHS)
//                 .str()
//                 .replace(lit("abfs://pjm/"), lit(""), true)
//                 .unique()])
//             .collect()
//             .unwrap();

//         let input_files = input_files.column(FILE_PATHS).unwrap().str().unwrap();
//         let a = input_files.first().unwrap();
//         let files_len = input_files.len();
//         eprintln!("files: qty: {}, first {}", files_len, a);
//         let new_day_dfs = new_day_df
//             .clone()
//             .lazy()
//             .drop(vec![FILE_PATHS])
//             .collect()
//             .unwrap()
//             .partition_by(vec!["node_id"], true)
//             .unwrap();
//         eprintln!("got new_day_dfs {}", new_day_dfs.len());
//         //TODO new_day_dfs to postgres

//         let pricedate_str = pricedate.format("%Y-%m-%d").to_string();
//         let existing_path = format!(
//             "abfs://pjm/apidata/{}/Day{}.parquet",
//             timer_key, pricedate_str
//         );
//         eprintln!("existing_path {}", &existing_path);
//         let mut existing_nodes = get_nodes(existing_path.as_str()).await;
//         eprintln!("got existing_nodes {}", &existing_nodes.len());
//         let write_to_str = format!(
//             "apidata/{}/_processing/Day{}.parquet",
//             timer_key, pricedate_str
//         );

//         eprintln!("write_to_str {}", write_to_str);
//         let write_to_path = Path::from(write_to_str.clone());
//         let object_store = Arc::clone(&state.object_store);
//         let cloud_writer =
//             CloudWriter::new_with_object_store(object_store, write_to_path).expect("cloud writer");
//         eprintln!("made cloud writer");
//         let pq_writer =
//             ParquetWriter::new(cloud_writer).with_compression(ParquetCompression::Zstd(None));

//         eprintln!("made pq writer with {:?}", &schema);
//         let mut batched_writer = pq_writer.batched(&schema).unwrap();

//         let existing_lf = match existing_nodes.len() {
//             0 => DataFrame::empty_with_schema(&schema).lazy(),
//             _ => {
//                 let mut args = ScanArgsParquet::default();
//                 args.parallel = ParallelStrategy::None;
//                 LazyFrame::scan_parquet(existing_path, args).unwrap()
//             }
//         };
//         eprintln!("made existing_lf");
//         let mut i = 0;
//         for df in new_day_dfs {
//             let node_id = df.column("node_id").unwrap().u64().unwrap().get(0).unwrap();
//             let filt = existing_lf
//                 .clone()
//                 .filter(col("node_id").eq(lit(node_id)))
//                 .collect()
//                 .unwrap();
//             let lfs = vec![filt.lazy(), df.lazy()];
//             let new_rg = concat(lfs, union_args)
//                 .unwrap()
//                 .unique(
//                     Some(vec!["utcbegin".to_string(), "node_id".to_string()]),
//                     UniqueKeepStrategy::Any,
//                 )
//                 .sort(
//                     vec!["utcbegin"],
//                     SortMultipleOptions::new().with_order_descending(false),
//                 )
//                 .collect()
//                 .unwrap();
//             if i == 0 {
//                 eprintln!("{}", new_rg);
//             };
//             batched_writer.write_batch(&new_rg).unwrap();
//             existing_nodes.remove(&node_id);
//             i += 1;
//         }
//         eprintln!(
//             "iterated the new_day_dfs with {} existing nodes leftover",
//             &existing_nodes.len()
//         );
//         for node_id in existing_nodes {
//             let new_rg = existing_lf
//                 .clone()
//                 .filter(col("node_id").eq(lit(node_id)))
//                 .unique(
//                     Some(vec!["utcbegin".to_string(), "node_id".to_string()]),
//                     UniqueKeepStrategy::Any,
//                 )
//                 .sort(
//                     vec!["utcbegin"],
//                     SortMultipleOptions::new().with_order_descending(false),
//                 )
//                 .collect()
//                 .unwrap();
//             batched_writer.write_batch(&new_rg).unwrap();
//         }
//         batched_writer.finish().expect("finish");
//         eprintln!("finished batch");
//         // let object_store = Arc::clone(&state.object_store);
//         // let copy_res = object_store
//         //     .copy(
//         //         &write_to_path,
//         //         &Path::from(write_to_str.replace("/_processing", "")),
//         //     )
//         //     .await;
//         // match copy_res {
//         //     Ok(_) => {
//         //         eprintln!("copied from processing to root");
//         //         let del_res = object_store.delete(&write_to_path).await;
//         //         match del_res {
//         //             Ok(_) => {
//         //                 eprintln!("deleted from processing");
//         //             }
//         //             Err(_) => {
//         //                 eprintln!("failed to delete processing")
//         //             }
//         //         };
//         //     }
//         //     Err(_) => {
//         //         eprintln!("failed to copy processing to root");
//         //     }
//         // }

//         //TODO: delete these async instead of one at a time.
//         // for del_file in input_files {
//         //     match del_file {
//         //         Some(del_file) => {
//         //             let _ = object_store.delete(&Path::from(del_file)).await;
//         //         }
//         //         None => {}
//         //     }
//         // }
//     }
//     {
//         let mut active_combines = state.active_combines.lock().await;
//         active_combines.remove(&timer_key.clone());
//     };
//     Ok(StatusCode::OK)
// }

// async fn get_nodes(file_path: &str) -> HashSet<u64> {
//     let mut nodes: HashSet<u64> = HashSet::new();
//     let async_reader = ParquetAsyncReader::from_uri(file_path, None, None).await;
//     if async_reader.is_err() {
//         return nodes;
//     };
//     let mut async_reader = async_reader.unwrap();
//     let metadata = async_reader.get_metadata().await;
//     if metadata.is_err() {
//         return nodes;
//     };
//     let metadata = metadata.unwrap();
//     let meta_cloned = Arc::clone(&metadata);
//     let row_groups = &meta_cloned.row_groups;

//     row_groups.into_iter().for_each(|rg| {
//         let columns = rg.columns_under_root_iter("node_id").unwrap();
//         columns.into_iter().for_each(|col| {
//             let c = col.metadata();
//             let stats = c.statistics.clone().unwrap();
//             let min = stats.min_value.clone().unwrap();
//             let max = stats.max_value.clone().unwrap();
//             if min != max {
//                 panic!("min not max")
//             }
//             let array: [u8; 8] = min.try_into().expect("Vec<u8> is not 8 bytes long.");
//             let node = u64::from_le_bytes(array);
//             nodes.insert(node);
//         });
//     });
//     nodes
// }
// async fn get_files(objstore: Arc<dyn ObjectStore>, dir: &str) -> Vec<String> {
//     let prefix = Path::from(dir);
//     let mut list_stream = objstore.list(Some(&prefix));
//     let mut files: Vec<String> = vec![];
//     // Print a line about each object
//     while let Some(meta) = list_stream.next().await.transpose().unwrap() {
//         let file = format!("{}", meta.location);
//         files.push(file);
//         // println!("Name: {}, size: {}", meta.location, meta.size);
//     }
//     files
// }
