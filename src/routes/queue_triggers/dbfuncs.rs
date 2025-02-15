use polars::prelude::*;
// use sqlx::Execute;
use sqlx::QueryBuilder;
use sqlx::{Pool, Postgres};

#[derive(sqlx::Type, Debug, Clone)]
#[sqlx(type_name = "pjm_nodes", rename_all = "lowercase")]
enum PjmNodes {
    Fern,
    Camden,
    Dom,
}

impl From<String> for PjmNodes {
    fn from(val: String) -> Self {
        let mut v = val.as_str();
        if v.starts_with("\"") {
            v = &v[1..];
        }
        if v.ends_with("\"") {
            v = &v[..v.len() - 1];
        }
        match v {
            "fern" => PjmNodes::Fern,
            "camden" => PjmNodes::Camden,
            "dom" => PjmNodes::Dom,
            _ => {
                eprintln!("got {}", v);
                panic!("bad value")
            }
        }
    }
}

pub async fn df_to_db(df: &DataFrame) {
    let username = std::env::var("PGUSER").unwrap();
    let password = std::env::var("PGPW").unwrap();

    let database_url = format!(
        "postgresql://{}:{}@projectsdb.usspapps.com:55669/projects?sslmode=require",
        username, password
    );
    let pool = Pool::<Postgres>::connect(database_url.as_str())
        .await
        .unwrap();

    let s = Series::new("node_id".into(), [2156110049u64, 45565887u64, 35010337u64]);
    let node_indx = DataFrame::new(vec![
        s.clone().into(),
        Series::new("node".into(), ["fern", "camden", "dom"]).into(),
    ])
    .unwrap()
    .lazy();

    let column_names: Vec<&str> = df
        .get_column_names()
        .into_iter()
        .map(|col_name| col_name.as_str())
        .collect();
    let is_da = column_names.contains(&"dalmp");
    let table_name = match is_da {
        true => "asset_mgmt.pjm_daprices",
        false => "asset_mgmt.pjm_rtprices",
    };
    let cols = match is_da {
        true => vec![
            col("node"),
            col("utcbegin"),
            col("pricedate"),
            col("hour"),
            col("dalmp"),
            col("damcc"),
            col("damcl"),
        ],
        false => vec![
            col("node"),
            col("utcbegin"),
            col("pricedate"),
            col("hour"),
            col("rtlmp"),
            col("rtmcc"),
            col("rtmcl"),
        ],
    };

    let to_insert_df = df
        .clone()
        .lazy()
        .filter(col("node_id").is_in(lit(s)))
        .left_join(node_indx, col("node_id"), col("node_id"))
        .select(cols)
        .collect()
        .unwrap();

    let column_names: Vec<&str> = to_insert_df
        .get_column_names()
        .into_iter()
        .map(|col_name| col_name.as_str())
        .collect();

    let insert_table = format!("INSERT INTO {} ({}) ", table_name, column_names.join(", "));

    let ins_tups: Vec<(PjmNodes, String, String, i32, f64, f64, f64)> = (0..to_insert_df.height())
        .into_iter()
        .map(|row| {
            let row_vals = to_insert_df.get_row(row).unwrap().0;
            (
                row_vals[0].to_string().into(),
                row_vals[1].to_string(),
                row_vals[2].to_string(),
                row_vals[3].try_extract::<u8>().unwrap() as i32,
                row_vals[4].try_extract::<f64>().unwrap(),
                row_vals[5].try_extract::<f64>().unwrap(),
                row_vals[6].try_extract::<f64>().unwrap(),
            )
        })
        .collect();
    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(&insert_table);
    query_builder.push_values(ins_tups, |mut b, rs| {
        b.push_bind(rs.0)
            .push("(")
            .push_bind_unseparated(rs.1)
            .push_unseparated("::timestamptz)")
            .push("(")
            .push_bind_unseparated(rs.2)
            .push_unseparated("::date)")
            .push_bind(rs.3)
            .push_bind(rs.4)
            .push_bind(rs.5)
            .push_bind(rs.6);
    });
    query_builder.push(" ON CONFLICT (node, utcbegin) DO UPDATE SET ");

    let update_columns: Vec<String> = column_names
        .iter()
        .filter_map(|item| {
            if item == &"node" || item == &"utcbegin" {
                return None;
            }
            Some(format!("{} = EXCLUDED.{}", item, item))
        })
        .collect();
    let update_clause = update_columns.join(", ");
    query_builder.push(&update_clause);

    let query: sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments> =
        query_builder.build();
    let _ = query.execute(&pool).await.unwrap();
}
