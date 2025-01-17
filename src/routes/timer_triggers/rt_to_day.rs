use crate::errors::Errors;
use crate::AppState;
use crate::RtToDa;
use axum::http::StatusCode;
use chrono::NaiveDate;
use cloud::CloudWriter;
use futures::future::join_all;
use object_store::path::Path;
use object_store::ObjectStore;
use polars::prelude::ScanArgsParquet;
use polars::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use std::panic::{self, AssertUnwindSafe};


pub async fn parse_rt_to_da_starter(
    rt_to_da: RtToDa,
    state: Arc<AppState>,
) -> Result<StatusCode, Errors> {
    tokio::spawn(parse_rt_to_da_wrapper(rt_to_da, state));
    Ok(StatusCode::OK)

}

async fn parse_rt_to_da_wrapper(
    rt_to_da: RtToDa,
    state: Arc<AppState>,
) {
    let endpoint = rt_to_da.endpoint.clone();
    // let pjm_end_point: PJMEndPoint = rt_to_da.endpoint.as_str().try_into()?;

    let is_working = {
        let mut active_combines = state.active_combines.lock().await;
        if active_combines.contains(&rt_to_da) {
            let mut combines_add_when_done = state.combines_add_when_done.lock().await;
            combines_add_when_done.insert(rt_to_da.clone());
            true
        } else {
            active_combines.insert(rt_to_da.clone());
            false
        }
    };

    if is_working {
        return
    };
    let _ = panic::catch_unwind(AssertUnwindSafe(|| async {
    parse_rt_to_da(rt_to_da, &state).await
}));
    {
        let mut combines_add_when_done = state.combines_add_when_done.lock().await;
        let mut item_to_queue: Option<RtToDa> = None;
        for item in combines_add_when_done.iter() {
            if item.endpoint == endpoint {
                item_to_queue = Some(item.clone());
                break;
            };
        }
        if let Some(item) = item_to_queue {
            combines_add_when_done.remove(&item);
        }
    };

}

async fn parse_rt_to_da(rt_to_da: RtToDa, state: &Arc<AppState>) {
    eprintln!("in parse rt_to_da func");
    let endpoint = &rt_to_da.endpoint;
    let dir = format!("abfs://pjm/apidata/{}/_input/*.parquet", endpoint);
    let pricedate_str = rt_to_da.date.as_str();
    let pricedate = NaiveDate::parse_from_str(pricedate_str, "%Y-%m-%d").unwrap();

    eprintln!("scanning files in {}", &dir);
    let args = ScanArgsParquet { include_file_paths: Some("file_path".into()), ..Default::default() };
    let new_df = tokio::task::spawn_blocking(move || {
        LazyFrame::scan_parquet(dir, args)
            .unwrap()
            .filter(col("pricedate").eq(lit(pricedate)))
            .collect()
            .unwrap()
    })
    .await
    .unwrap();
    if new_df.shape().0==0 {return}
    eprintln!("making files column");
    let files = new_df.column("file_path").unwrap();
    let files = files.str().unwrap().unique().unwrap();
    let new_df = new_df.drop("file_path").unwrap();
    eprintln!("have files ca with {} unique files", files.len());
    let new_dfs = new_df.clone().partition_by(vec!["node_id"], true).unwrap();
    eprintln!("have {} df partitions", &new_dfs.len());
    let schema = new_df.schema();
    eprintln!("pricedate is {}", &pricedate_str);
    let existing_file = format!("abfs://pjm/apidata/{}/Day{}.parquet", endpoint, pricedate_str);
    let write_to_str = format!("apidata/{}/_processing/Day{}.parquet", endpoint.as_str(), pricedate_str);
    let mut existing_nodes = get_nodes(existing_file.as_str()).await;
    let existing_lf = match existing_nodes.len() {
        0=> DataFrame::empty_with_schema(&schema).lazy(),
        _ => {
            let lf =tokio::task::spawn_blocking(move || {
                LazyFrame::scan_parquet(existing_file.clone(), ScanArgsParquet::default()).unwrap()
            })
            .await
            .unwrap();
            eprintln!("have {} existing nodes and existing lf", &existing_nodes.len());
            lf
        }
    };
    
    let write_to_path = Path::from(write_to_str.clone());
    let object_store = Arc::clone(&state.object_store);
    let cloud_writer = CloudWriter::new_with_object_store(object_store, write_to_path.clone())
        .expect("cloud writer");

    let pq_writer =
        ParquetWriter::new(cloud_writer).with_compression(ParquetCompression::Zstd(None));
    let mut batched_writer = pq_writer.batched(&schema).unwrap();

    let union_args = UnionArgs {parallel : false,rechunk: true, ..Default::default()};
    eprintln!("starting to write new file to {}", &write_to_str);
    for (i,df) in new_dfs.into_iter().enumerate() {
        let node_id = df.column("node_id").unwrap().u64().unwrap().get(0).unwrap();
        if i%100==0 || i<=10 {
            eprintln!("working on node_id {} with {} rows; i={}", node_id, df.shape().0, i);
            eprintln!("{}", df);
        }

        let existing_lf_clone=existing_lf.clone();
        let filt = tokio::task::spawn_blocking(move || {
            existing_lf_clone
            .filter(col("node_id").eq(lit(node_id)))
            .collect()
            .unwrap()
        })
        .await
        .unwrap();
        if i%100==0 || i<=10 {
            eprintln!("have {} existing rows", filt.shape().0);
        }
        let lfs = vec![filt.lazy(), df.lazy()]; // keep this order for the concat keep last strategy
      
        let new_rg = concat(lfs, union_args)
            .unwrap()
            .unique(
                Some(vec!["utcbegin".to_string(), "node_id".to_string()]),
                UniqueKeepStrategy::Last, //Keep last so that new data updates old data.
            )
            .sort(
                vec!["utcbegin"],
                SortMultipleOptions::new().with_order_descending(false),
            )
            .collect()
            .unwrap();
        if i%100==0 || i<=10 {
            eprintln!("about to write batch of {} rows", new_rg.shape().0);
        }
        batched_writer.write_batch(&new_rg).unwrap();
        existing_nodes.remove(&node_id);
    }
    eprintln!("done with partitioned dfs, there are {} leftover nodes", existing_nodes.len());
    for node_id in existing_nodes {
        let existing_lf_clone=existing_lf.clone();

        let new_rg = tokio::task::spawn_blocking(move || {
            existing_lf_clone
            .clone()
            .filter(col("node_id").eq(lit(node_id)))
            .unique(
                Some(vec!["utcbegin".to_string(), "node_id".to_string()]),
                UniqueKeepStrategy::Any,
            )
            .sort(
                vec!["utcbegin"],
                SortMultipleOptions::new().with_order_descending(false),
            )
            .collect()
            .unwrap()
        })
        .await
        .unwrap();
        batched_writer.write_batch(&new_rg).unwrap();
    }
    batched_writer.finish().expect("finish");
    eprintln!("done writing parquet file");
    let object_store = Arc::clone(&state.object_store);

    copy_retry(
        object_store,
        &write_to_path,
        &Path::from(write_to_str.replace("/_processing", "")),
        10,
    )
    .await.unwrap();
    eprintln!("copied file from processing");
    let object_store = Arc::clone(&state.object_store);
    del_retry(object_store, write_to_path, 10).await.unwrap();
    eprintln!("deleted file from processing");
    // let del_paths:Vec<Path> = rt_to_da
    let futures: Vec<JoinHandle<Result<(), Errors>>> = files
        .clone()
        .into_iter()
        .map(|file| {
            let file_path = Path::from(file.unwrap());
            task::spawn(del_retry(Arc::clone(&state.object_store), file_path, 10))
        })
        .collect();

    let _results: Vec<Result<Result<(), Errors>, task::JoinError>> = join_all(futures).await;
    eprintln!("deleted {} incoming files", files.len());
}
async fn copy_retry(
    object_store: Arc<dyn ObjectStore>,
    source_path: &Path,
    target_path: &Path,
    times_to_retry: usize,
) -> Result<(), Errors> {
    let mut copy_fails = 0;
    loop {
        if copy_fails >= times_to_retry {
            return Err(Errors::ResponseErr);
        }
        let copy_res = object_store
            .copy(
                source_path,
                target_path, // ,
            )
            .await;
        match copy_res {
            Ok(_)=> {
                eprintln!("copy worked");
                return Ok(())},
            Err(e)=>{
                eprintln!("{:?}", e);
            }
        };
        eprintln!("copy didn't work");
        sleep(std::time::Duration::from_secs(1)).await;
        copy_fails += 1;
    }
}
async fn del_retry(
    object_store: Arc<dyn ObjectStore>,
    file_path: Path,
    times_to_retry: usize,
) -> Result<(), Errors> {
    let mut del_fails = 0usize;
    loop {
        if del_fails >= times_to_retry {
            return Err(Errors::ResponseErr);
        }
        let del_res = object_store.delete(&file_path).await;
        if del_res.is_ok() {
            eprintln!("del worked");
            return Ok(());
        }
        eprintln!("del didn't work");
        sleep(std::time::Duration::from_secs(1)).await;
        del_fails += 1;
    }
}
async fn get_nodes(file_path: &str) -> HashSet<u64> {
    let mut nodes: HashSet<u64> = HashSet::new();
    let async_reader = ParquetAsyncReader::from_uri(file_path, None, None).await;
    if async_reader.is_err() {
        return nodes;
    };
    let mut async_reader = async_reader.unwrap();
    let metadata = async_reader.get_metadata().await;
    if metadata.is_err() {
        return nodes;
    };
    let metadata = metadata.unwrap();
    let meta_cloned = Arc::clone(metadata);
    let row_groups = &meta_cloned.row_groups;

    row_groups.iter().for_each(|rg| {
        let columns = rg.columns_under_root_iter("node_id").unwrap();
        columns.into_iter().for_each(|col| {
            let c = col.metadata();
            let stats = c.statistics.clone().unwrap();
            let min = stats.min_value.clone().unwrap();
            let max = stats.max_value.clone().unwrap();
            if min != max {
                panic!("min not max")
            }
            let array: [u8; 8] = min.try_into().expect("Vec<u8> is not 8 bytes long.");
            let node = u64::from_le_bytes(array);
            nodes.insert(node);
        });
    });
    nodes
}
