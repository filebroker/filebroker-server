use std::collections::HashMap;

use chrono::{DateTime, Utc};
use diesel::{OptionalExtension, RunQueryDsl};
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{Rejection, Reply};

use crate::{
    acquire_db_connection,
    error::Error,
    model::{PostQueryObject, PostWindowQueryObject, S3Object, User},
    perms,
};

pub mod compiler;

const DEFAULT_LIMIT_STR: &str = "50";
const MAX_LIMIT: u16 = 100;
const MAX_LIMIT_STR: &str = "100";

#[derive(Deserialize, Validate)]
pub struct QueryParametersFilter {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    #[validate(length(min = 0, max = 1024))]
    pub query: Option<String>,
}

pub struct QueryParameters {
    pub limit: Option<String>,
    pub page: Option<u32>,
    pub ordering: Vec<Ordering>,
    pub variables: HashMap<String, String>,
}

pub struct Ordering {
    pub expression: String,
    pub direction: Direction,
}

pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub full_count: Option<i64>,
    pub pages: Option<i64>,
    pub posts: Vec<PostQueryObject>,
}

pub async fn search_handler(
    user: Option<User>,
    query_parameters_filter: QueryParametersFilter,
) -> Result<impl Reply, Rejection> {
    query_parameters_filter.validate().map_err(|e| {
        Error::InvalidRequestInputError(format!(
            "Validation failed for QueryParametersFilter: {}",
            e
        ))
    })?;

    let query_parameters = prepare_query_parameters(&query_parameters_filter, &user);

    let sql_query = compiler::compile_sql(query_parameters_filter.query, query_parameters, &user)?;
    let mut connection = acquire_db_connection()?;
    let posts = diesel::sql_query(&sql_query)
        .load::<PostQueryObject>(&mut connection)
        .map_err(|e| Error::QueryError(e.to_string()))?;

    let full_count = if posts.is_empty() {
        Some(0)
    } else {
        posts[0].full_count
    };

    let pages = if posts.is_empty() {
        Some(0)
    } else if let Some(full_count) = full_count {
        let limit = posts[0].evaluated_limit;
        if limit > MAX_LIMIT as i32 {
            return Err(warp::reject::custom(Error::IllegalQueryInputError(
                format!("Limit '{}' exceeds maximum limit of {}.", limit, MAX_LIMIT),
            )));
        }
        Some(((full_count as f64) / (limit as f64)).ceil() as i64)
    } else {
        None
    };

    Ok(warp::reply::json(&SearchResult {
        full_count,
        pages,
        posts,
    }))
}

#[derive(Serialize)]
pub struct PostDetailed {
    pub pk: i32,
    pub data_url: Option<String>,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub fk_create_user: i32,
    pub score: i32,
    pub s3_object: Option<S3Object>,
    pub thumbnail_url: Option<String>,
    pub prev_post_pk: Option<i32>,
    pub next_post_pk: Option<i32>,
}

pub async fn get_post_handler(
    user: Option<User>,
    post_pk: i32,
    query_parameters_filter: QueryParametersFilter,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection()?;

    let (post, s3_object) = perms::load_post_secured(post_pk, &mut connection, user.as_ref())?;

    let query_parameters = prepare_query_parameters(&query_parameters_filter, &user);
    let (prev_post_pk, next_post_pk) = match query_parameters_filter.query {
        Some(query) => {
            let sql_query =
                compiler::compile_window_query(post.pk, query, query_parameters, &user)?;
            let mut connection = acquire_db_connection()?;
            let result = diesel::sql_query(&sql_query)
                .get_result::<PostWindowQueryObject>(&mut connection)
                .optional()
                .map_err(|e| Error::QueryError(e.to_string()))?;

            if let Some(result) = result {
                (result.prev, result.next)
            } else {
                (None, None)
            }
        }
        None => (None, None),
    };

    Ok(warp::reply::json(&PostDetailed {
        pk: post.pk,
        data_url: post.data_url,
        source_url: post.source_url,
        title: post.title,
        creation_timestamp: post.creation_timestamp,
        fk_create_user: post.fk_create_user,
        score: post.score,
        s3_object,
        thumbnail_url: post.thumbnail_url,
        prev_post_pk,
        next_post_pk,
    }))
}

fn prepare_query_parameters(
    query_parameters_filter: &QueryParametersFilter,
    user: &Option<User>,
) -> QueryParameters {
    let mut variables = HashMap::new();
    variables.insert(
        String::from("current_utc_timestamp"),
        Utc::now().to_string(),
    );
    variables.insert(String::from("current_utc_date"), Utc::today().to_string());

    if let Some(ref user) = user {
        variables.insert(String::from("current_user_key"), user.pk.to_string());
    }

    QueryParameters {
        limit: query_parameters_filter.limit.map(|l| l.to_string()),
        page: query_parameters_filter.page,
        ordering: Vec::new(),
        variables,
    }
}

pub mod functions {
    use diesel::sql_types::Text;
    sql_function!(fn lower(x: Text) -> Text);
}
