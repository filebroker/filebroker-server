use diesel::RunQueryDsl;
use serde::Deserialize;
use warp::{Rejection, Reply};

use crate::{acquire_db_connection, error::Error, model::Post};

pub mod compiler;

#[derive(Deserialize)]
pub struct QueryParametersFilter {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub query: Option<String>,
}

pub struct QueryParameters {
    pub limit: Option<String>,
    pub page: Option<u32>,
    pub ordering: Vec<Ordering>,
}

pub struct Ordering {
    pub expression: String,
    pub direction: Direction,
}

pub enum Direction {
    Ascending,
    Descending,
}

pub async fn search_handler(
    query_parameters_filter: QueryParametersFilter,
) -> Result<impl Reply, Rejection> {
    let query_parameters = QueryParameters {
        limit: query_parameters_filter.limit.map(|l| l.to_string()),
        page: query_parameters_filter.page,
        ordering: Vec::new(),
    };

    let sql_query = compiler::compile_sql(query_parameters_filter.query, query_parameters)?;
    let connection = acquire_db_connection()?;
    let posts = diesel::sql_query(&sql_query)
        .load::<Post>(&connection)
        .map_err(|e| Error::QueryError(e.to_string()))?;

    Ok(warp::reply::json(&posts))
}

pub mod functions {
    use diesel::sql_types::Text;
    sql_function!(fn lower(x: Text) -> Text);
}
