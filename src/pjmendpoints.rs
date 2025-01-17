use crate::errors::Errors;
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc, Weekday};
use chrono_tz::America::New_York;
use polars::prelude::*;
static STRP_FMT: PlSmallStr = PlSmallStr::from_static("%Y-%m-%dT%H:%M:%S");
static UTC: PlSmallStr = PlSmallStr::from_static("UTC");
static NY: PlSmallStr = PlSmallStr::from_static("America/New_York");

#[derive(Clone)]
pub struct PJMEndPoint {
    pub url_suffix: &'static str,
    pub columns: &'static str,
    pub exprs: Vec<Expr>,
    pub next_time: Duration,
    expected: Arc<dyn Fn(DateTime<Utc>) -> u32 + Send + Sync>,
    pub default_duration: Option<Duration>,
    pub unique_by: Vec<&'static str>,
    pub sort_by: Vec<&'static str>,
    pub partition_by: &'static str,
}

impl PJMEndPoint {
    pub fn expected(&self, in_dt: DateTime<Utc>) -> u32 {
        let func = Arc::clone(&self.expected);
        func(in_dt)
    }
    pub fn is_sub_daily(&self) -> bool {
        self.next_time < Duration::days(1)
    }
    pub fn is_first_last(&self, begin_dt: DateTime<Utc>, next_time: DateTime<Utc>) -> (bool, bool) {
        let in_ny = begin_dt.with_timezone(&New_York);
        let trunc_day = in_ny.date_naive();
        let ny_day = New_York
            .from_local_datetime(&trunc_day.and_hms_opt(0, 0, 0).unwrap())
            .single()
            .expect("can't convert dt in first/last");
        let is_first = in_ny == ny_day;

        let next_trunc_day = next_time.date_naive();

        let is_last = trunc_day < next_trunc_day;
        (is_first, is_last)
    }
}

fn mystrp() -> StrptimeOptions {
    StrptimeOptions {
        format: Some(STRP_FMT.clone()),
        strict: true,
        exact: true,
        cache: true,
    }
}

fn utc_begin() -> Expr {
    col("datetime_beginning_utc")
        .str()
        .to_datetime(None, None, mystrp(), lit("raise"))
        .dt()
        .replace_time_zone(Some(UTC.clone()), lit("raise"), NonExistent::Raise)
}
fn ny_time() -> Expr {
    utc_begin().dt().convert_time_zone(NY.clone())
}

fn utcbegin() -> Expr {
    utc_begin().alias("utcbegin")
}

fn pricedate() -> Expr {
    ny_time().dt().date().alias("pricedate")
}

fn hour() -> Expr {
    (ny_time().dt().hour() + lit(1))
        .cast(DataType::UInt8)
        .alias("hour")
}

fn node_id() -> Expr {
    col("pnode_id").cast(DataType::UInt64).alias("node_id")
}

fn lmp_col(lmp: &'static str) -> Expr {
    let new_name = match lmp {
        "total_lmp_da" => "dalmp",
        "congestion_price_da" => "damcc",
        "marginal_loss_price_da" => "damcl",
        "total_lmp_rt" => "rtlmp",
        "congestion_price_rt" => "rtmcc",
        "marginal_loss_price_rt" => "rtmcl",
        _ => lmp,
    };
    col(lmp).cast(DataType::Float64).alias(new_name)
}
fn da_hrl_lmps() -> PJMEndPoint {
    fn expected(in_dt: DateTime<Utc>) -> u32 {
        let expected_dt = in_dt + Duration::hours(-14);
        let now = Utc::now();
        let diff = expected_dt - now;
        let secs = diff.num_seconds();
        if secs > 0 {
            secs as u32
        } else {
            60u32
        }
    }
    PJMEndPoint {
    url_suffix:"da_hrl_lmps", 
    columns: "datetime_beginning_utc,pnode_id,total_lmp_da,congestion_price_da,marginal_loss_price_da", 
    exprs: vec![utcbegin(), pricedate(), hour(),
    node_id(), lmp_col("total_lmp_da"), lmp_col("congestion_price_da"), lmp_col("marginal_loss_price_da")],
    next_time:Duration::hours(1),
    expected:Arc::new(expected),
    default_duration: None,
    unique_by: vec!["node_id", "utcbegin"],
    sort_by: vec!["utcbegin"],
    partition_by: "node_id"
}
}

fn rt_fivemin_hrl_lmps() -> PJMEndPoint {
    fn expected(in_dt: DateTime<Utc>) -> u32 {
        let in_ny = in_dt.with_timezone(&New_York);
        let next_day = in_ny.date_naive() + Duration::days(1);
        let weekday = next_day.weekday();
        let next_day = match weekday {
            Weekday::Sat => next_day + Duration::days(2),
            Weekday::Sun => next_day + Duration::days(1),
            _ => next_day,
        };
        let expected_dt = New_York
            .from_local_datetime(&next_day.and_hms_opt(10, 0, 0).unwrap())
            .single()
            .expect("can't convert dt")
            .with_timezone(&Utc);
        let now = Utc::now();
        let diff = expected_dt - now;
        let secs = diff.num_seconds();
        if secs > 0 {
            secs as u32
        } else {
            0u32
        }
    }
    PJMEndPoint {
    url_suffix:"rt_fivemin_hrl_lmps",
    columns: "datetime_beginning_utc,pnode_id,total_lmp_rt,congestion_price_rt,marginal_loss_price_rt",
    exprs: vec![utcbegin(),pricedate(), hour(),
    node_id(), lmp_col("total_lmp_rt"), lmp_col("congestion_price_rt"), lmp_col("marginal_loss_price_rt")],
    next_time:Duration::minutes(5),
    expected:Arc::new(expected),
    default_duration: Some(Duration::minutes(10)),
    unique_by: vec!["node_id", "utcbegin"],
    sort_by: vec!["utcbegin"],
    partition_by: "node_id"
}
}

fn rt_unverified_fivemin_lmps() -> PJMEndPoint {
    fn expected(in_dt: DateTime<Utc>) -> u32 {
        let expected_dt = in_dt + Duration::minutes(5);
        let now = Utc::now();
        let diff = expected_dt - now;
        let secs = diff.num_seconds();

        if secs > 0 {
            secs as u32
        } else {
            0u32
        }
    }
    PJMEndPoint {
    url_suffix:"rt_unverified_fivemin_lmps",
    columns: "datetime_beginning_utc,pnode_id,total_lmp_rt,congestion_price_rt,marginal_loss_price_rt",
    exprs: vec![utcbegin(),pricedate(), hour(),
    node_id(), lmp_col("total_lmp_rt"), lmp_col("congestion_price_rt"), lmp_col("marginal_loss_price_rt")],
    next_time:Duration::minutes(5),
    expected:Arc::new(expected),
    default_duration: Some(Duration::minutes(10)),
    unique_by: vec!["node_id", "utcbegin"],
    sort_by: vec!["utcbegin"],
    partition_by: "node_id"
}
}

impl TryFrom<&str> for PJMEndPoint {
    type Error = Errors;
    fn try_from(url_suffix: &str) -> Result<Self, Self::Error> {
        match url_suffix {
            "da_hrl_lmps" => Ok(da_hrl_lmps()),
            "rt_fivemin_hrl_lmps" => Ok(rt_fivemin_hrl_lmps()),
            "rt_unverified_fivemin_lmps" => Ok(rt_unverified_fivemin_lmps()),
            _ => Err(Errors::NoPJMEndPoint),
        }
    }
}
