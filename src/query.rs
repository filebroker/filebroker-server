use std::{cmp, collections::HashMap, str::FromStr};

use chrono::{DateTime, Utc};
use diesel::{OptionalExtension, QueryDsl, QueryableByName};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{Rejection, Reply};

use crate::model::{
    PostCollectionFull, PostCollectionItemQueryObject, PostCollectionQueryObject, S3ObjectMetadata,
};
use crate::perms::{PostCollectionItemJoined, PostCollectionJoined};
use crate::post::PostCollectionGroupAccessDetailed;
use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, TextExpressionMethods},
    error::Error,
    model::{PostQueryObject, PostWindowQueryObject, S3Object, Tag, User},
    perms::{self, PostJoined},
    post::{self, PostGroupAccessDetailed},
    schema::tag,
};

use self::{
    compiler::{
        ast::{
            AttributeNode, ExpressionNode, ExpressionStatement, FunctionCallNode, ModifierNode,
            Node, PostTagNode, SemanticAnalysisVisitor, StatementNode, VariableNode,
        },
        dict::Scope,
        lexer, Location, Log,
    },
    functions::{char_length, lower},
};

pub mod compiler;

const DEFAULT_LIMIT_STR: &str = "50";
const MAX_LIMIT: u32 = 100;
const MAX_FULL_LIMIT: u32 = 10000;
const MAX_FULL_LIMIT_STR: &str = "10000";

macro_rules! report_missing_pks {
    ($tab:ident, $pks:expr, $connection:expr) => {
        $tab::table
            .select($tab::pk)
            .filter($tab::pk.eq_any($pks))
            .load::<i64>($connection)
            .await
            .map(|found_pks| {
                let missing_pks = $pks
                    .iter()
                    .filter(|pk| !found_pks.contains(pk))
                    .collect::<Vec<_>>();
                if missing_pks.is_empty() {
                    Ok(())
                } else {
                    Err(crate::Error::InvalidEntityReferenceError(
                        itertools::Itertools::intersperse(
                            missing_pks.into_iter().map(i64::to_string),
                            String::from(", "),
                        )
                        .collect::<String>(),
                    ))
                }
            })
            .map_err(crate::Error::from)
    };
}

pub(crate) use report_missing_pks;

macro_rules! load_and_report_missing_pks {
    ($return_type:ident, $tab:ident, $pks:expr, $connection:expr) => {{
        let found = $tab::table
            .filter($tab::pk.eq_any($pks))
            .load::<$return_type>($connection)
            .await
            .map_err(crate::Error::from)?;

        let found_pks = found
            .iter()
            .map(|f| f.pk)
            .collect::<std::collections::HashSet<_>>();
        let missing_pks = $pks
            .iter()
            .filter(|pk| !found_pks.contains(pk))
            .collect::<Vec<_>>();
        if missing_pks.is_empty() {
            Ok(found)
        } else {
            Err(crate::Error::InvalidEntityReferenceError(
                itertools::Itertools::intersperse(
                    missing_pks.into_iter().map(i64::to_string),
                    String::from(", "),
                )
                .collect::<String>(),
            ))
        }
    }};
}

pub(crate) use load_and_report_missing_pks;

#[derive(Deserialize, Validate)]
pub struct QueryParametersFilter {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    #[validate(length(min = 0, max = 1024))]
    pub query: Option<String>,
    pub exclude_window: Option<bool>,
    pub shuffle: Option<bool>,
    pub writable_only: Option<bool>,
}

pub struct QueryParameters {
    pub limit: Option<String>,
    pub page: Option<u32>,
    pub ordering: Vec<Ordering>,
    pub variables: HashMap<String, String>,
    pub shuffle: bool,
    pub writable_only: bool,
    pub base_table_name: &'static str,
    pub tag_relation_table_name: &'static str,
    pub select_statements: Vec<String>,
    pub join_statements: Vec<String>,
    pub max_limit: u32,
    pub fallback_orderings: Vec<Ordering>,
    pub from_table_override: Option<&'static str>,
    pub predefined_where_conditions: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Ordering {
    pub expression: String,
    pub direction: Direction,
    pub nullable: bool,
    pub table: &'static str,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub full_count: Option<i64>,
    pub pages: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub posts: Option<Vec<PostQueryObject>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collections: Option<Vec<PostCollectionQueryObject>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection_items: Option<Vec<PostCollectionItemQueryObject>>,
}

pub async fn search_handler(
    user: Option<User>,
    scope: Option<String>,
    scope_param: Option<String>,
    query_parameters_filter: QueryParametersFilter,
) -> Result<impl Reply, Rejection> {
    query_parameters_filter.validate().map_err(|e| {
        Error::InvalidRequestInputError(format!(
            "Validation failed for QueryParametersFilter: {}",
            e
        ))
    })?;

    let mut scope = scope
        .map(|scope| {
            Scope::from_str(scope.as_str()).map_err(|_| {
                warp::reject::custom(Error::BadRequestError(format!(
                    "Invalid scope '{}'",
                    &scope
                )))
            })
        })
        .unwrap_or(Ok(Scope::Post))?;

    if let Some(scope_param) = scope_param {
        if scope == Scope::Collection {
            scope = match scope_param.parse::<i64>() {
                Ok(collection_pk) => Scope::CollectionItem { collection_pk },
                Err(_) => {
                    return Err(warp::reject::custom(Error::BadRequestError(format!(
                        "'{scope_param}' is not a valid i64"
                    ))))
                }
            };
        } else {
            return Err(warp::reject::custom(Error::BadRequestError(format!(
                "Scope {:?} does not accept a parameter",
                &scope
            ))));
        }
    }

    let query_parameters = prepare_query_parameters(&query_parameters_filter, &user, &scope)?;
    let max_limit = query_parameters.max_limit;

    let sql_query = compiler::compile_sql(
        query_parameters_filter.query,
        query_parameters,
        &scope,
        &user,
    )?;
    let mut connection = acquire_db_connection().await?;
    match scope {
        Scope::Global => {
            Err(Error::BadRequestError(format!("Invalid scope '{:?}'", &scope)).into())
        }
        Scope::Post => Ok(warp::reply::json(
            &get_search_result::<PostQueryObject>(sql_query, max_limit, &mut connection).await?,
        )),
        Scope::Collection => Ok(warp::reply::json(
            &get_search_result::<PostCollectionQueryObject>(sql_query, max_limit, &mut connection)
                .await?,
        )),
        Scope::CollectionItem { .. } => Ok(warp::reply::json(
            &get_search_result::<PostCollectionItemQueryObject>(
                sql_query,
                max_limit,
                &mut connection,
            )
            .await?,
        )),
    }
}

#[derive(Serialize)]
pub struct PostDetailed {
    pub pk: i64,
    pub data_url: Option<String>,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub create_user: User,
    pub score: i32,
    pub s3_object: S3Object,
    pub s3_object_metadata: S3ObjectMetadata,
    pub thumbnail_url: Option<String>,
    pub prev_post: Option<PostWindowObject>,
    pub next_post: Option<PostWindowObject>,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_edit: bool,
    pub description: Option<String>,
    pub is_editable: bool,
    pub is_deletable: bool,
    pub tags: Vec<Tag>,
    pub group_access: Vec<PostGroupAccessDetailed>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_collection_item: Option<PostCollectionItemInformation>,
}

#[derive(Serialize)]
pub struct PostWindowObject {
    pk: i64,
    page: u32,
}

#[derive(Serialize)]
pub struct PostCollectionItemInformation {
    pub post_collection: PostCollectionFull,
    pub added_by: User,
    pub creation_timestamp: DateTime<Utc>,
    pub pk: i64,
    pub ordinal: i32,
}

pub async fn get_post_handler(
    user: Option<User>,
    outer_pk: i64,
    inner_pk: Option<i64>,
    query_parameters_filter: QueryParametersFilter,
) -> Result<impl Reply, Rejection> {
    query_parameters_filter.validate().map_err(|e| {
        Error::InvalidRequestInputError(format!(
            "Validation failed for QueryParametersFilter: {}",
            e
        ))
    })?;

    let mut connection = acquire_db_connection().await?;

    let (scope, post, post_collection_item) = if let Some(inner_pk) = inner_pk {
        let PostCollectionItemJoined {
            post_collection_item,
            added_by,
            post,
            post_collection,
        } = perms::load_post_collection_item_secured(
            outer_pk,
            inner_pk,
            &mut connection,
            user.as_ref(),
        )
        .await?;
        (
            Scope::CollectionItem {
                collection_pk: outer_pk,
            },
            post,
            Some(PostCollectionItemInformation {
                post_collection: PostCollectionFull {
                    pk: post_collection.post_collection.pk,
                    title: post_collection.post_collection.title,
                    creation_timestamp: post_collection.post_collection.creation_timestamp,
                    create_user: post_collection.create_user.into(),
                    poster_object: post_collection.poster_object.map(|p| p.into()),
                    thumbnail_object_key: post_collection.post_collection.poster_object_key,
                    public: post_collection.post_collection.public,
                    public_edit: post_collection.post_collection.public_edit,
                    description: post_collection.post_collection.description,
                },
                added_by,
                creation_timestamp: post_collection_item.creation_timestamp,
                pk: post_collection_item.pk,
                ordinal: post_collection_item.ordinal,
            }),
        )
    } else {
        (
            Scope::Post,
            perms::load_post_secured(outer_pk, &mut connection, user.as_ref()).await?,
            None,
        )
    };

    let PostJoined {
        post,
        create_user,
        s3_object,
        s3_object_metadata,
    } = post;

    let page = query_parameters_filter.page;
    let exclude_window = query_parameters_filter.exclude_window;
    let query_parameters = prepare_query_parameters(&query_parameters_filter, &user, &scope)?;
    let (prev_post, next_post) = if !exclude_window.unwrap_or(false) {
        let sql_query = compiler::compile_window_query(
            post_collection_item
                .as_ref()
                .map(|item| item.pk)
                .unwrap_or(post.pk),
            query_parameters_filter.query,
            query_parameters,
            &scope,
            &user,
        )?;
        let result = diesel::sql_query(sql_query)
            .get_result::<PostWindowQueryObject>(&mut connection)
            .await
            .optional()
            .map_err(|e| Error::QueryError(e.to_string()))?;

        if let Some(result) = result {
            let limit = result.evaluated_limit;
            if limit > MAX_LIMIT as i32 {
                return Err(warp::reject::custom(Error::IllegalQueryInputError(
                    format!("Limit '{}' exceeds maximum limit of {}.", limit, MAX_LIMIT),
                )));
            }

            let page = page.unwrap_or(0);
            let row_number = result.row_number;
            let offset = limit as i64 * page as i64;
            let row_within_page = row_number - offset;

            (
                result.prev.map(|prev| PostWindowObject {
                    pk: prev,
                    page: if row_within_page == 1 {
                        page.saturating_sub(1)
                    } else {
                        page
                    },
                }),
                result.next.map(|next| PostWindowObject {
                    pk: next,
                    page: if row_within_page == limit as i64 {
                        page + 1
                    } else {
                        page
                    },
                }),
            )
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    let is_editable = post.is_editable(user.as_ref(), &mut connection).await?;
    let is_deletable = post.is_deletable(user.as_ref(), &mut connection).await?;
    let tags = post::get_post_tags(post.pk, &mut connection)
        .await
        .map_err(Error::from)?;
    let group_access = post::get_post_group_access(post.pk, user.as_ref(), &mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&PostDetailed {
        pk: post.pk,
        data_url: post.data_url,
        source_url: post.source_url,
        title: post.title,
        creation_timestamp: post.creation_timestamp,
        create_user,
        score: post.score,
        s3_object,
        s3_object_metadata,
        thumbnail_url: post.thumbnail_url,
        prev_post,
        next_post,
        public: post.public,
        public_edit: post.public_edit,
        description: post.description,
        is_editable,
        is_deletable,
        tags,
        group_access,
        post_collection_item,
    }))
}

#[derive(Serialize)]
pub struct PostCollectionDetailed {
    pub pk: i64,
    pub title: String,
    pub create_user: User,
    pub creation_timestamp: DateTime<Utc>,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_edit: bool,
    pub poster_object: Option<S3Object>,
    pub poster_object_key: Option<String>,
    pub description: Option<String>,
    pub is_editable: bool,
    pub is_deletable: bool,
    pub tags: Vec<Tag>,
    pub group_access: Vec<PostCollectionGroupAccessDetailed>,
}

pub async fn get_post_collection_handler(
    user: Option<User>,
    post_collection_pk: i64,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let PostCollectionJoined {
        post_collection,
        create_user,
        poster_object,
    } = perms::load_post_collection_secured(post_collection_pk, &mut connection, user.as_ref())
        .await?;

    let is_editable = post_collection
        .is_editable(user.as_ref(), &mut connection)
        .await?;
    let is_deletable = post_collection
        .is_deletable(user.as_ref(), &mut connection)
        .await?;
    let tags = post::get_post_collection_tags(post_collection_pk, &mut connection)
        .await
        .map_err(Error::from)?;
    let group_access =
        post::get_post_collection_group_access(post_collection_pk, user.as_ref(), &mut connection)
            .await
            .map_err(Error::from)?;

    Ok(warp::reply::json(&PostCollectionDetailed {
        pk: post_collection.pk,
        title: post_collection.title,
        create_user,
        creation_timestamp: post_collection.creation_timestamp,
        public: post_collection.public,
        public_edit: post_collection.public_edit,
        poster_object,
        poster_object_key: post_collection.poster_object_key,
        description: post_collection.description,
        is_editable,
        is_deletable,
        tags,
        group_access,
    }))
}

/// Find all posts for the given query string, limited to 10000 results maximum
pub async fn find_all_posts(
    query: String,
    user: &Option<User>,
) -> Result<Vec<PostQueryObject>, Error> {
    let scope = Scope::Post;
    let query_parameters_filter = QueryParametersFilter {
        limit: None,
        page: None,
        query: Some(query),
        exclude_window: None,
        shuffle: None,
        writable_only: None,
    };
    let mut query_parameters = prepare_query_parameters(&query_parameters_filter, user, &scope)?;
    query_parameters.limit = Some(MAX_FULL_LIMIT.to_string());
    query_parameters.max_limit = MAX_FULL_LIMIT;
    let sql_query = compiler::compile_sql(
        query_parameters_filter.query,
        query_parameters,
        &scope,
        user,
    )?;
    let mut connection = acquire_db_connection().await?;
    let search_result =
        get_search_result::<PostQueryObject>(sql_query, MAX_FULL_LIMIT, &mut connection).await?;
    if search_result
        .full_count
        .map(|count| count > MAX_FULL_LIMIT.into())
        .unwrap_or(true)
    {
        Err(Error::TooManyResultsError(
            search_result.full_count.unwrap_or(100000) as u32,
            MAX_FULL_LIMIT,
        ))
    } else {
        Ok(search_result.posts.unwrap_or_default())
    }
}

pub trait SearchQueryResultObject {
    fn get_search_result(objects: Vec<Self>, max_limit: u32) -> Result<SearchResult, Error>
    where
        Self: Sized,
    {
        let full_count = if objects.is_empty() {
            Some(0)
        } else {
            objects[0].get_full_count()
        };

        let pages: Option<i64> = if objects.is_empty() {
            Some(0)
        } else {
            let limit = objects[0].get_evaluated_limit();
            if limit > max_limit as i32 {
                return Err(Error::IllegalQueryInputError(format!(
                    "Limit '{}' exceeds maximum limit of {}.",
                    limit, max_limit
                )));
            }
            full_count.map(|full_count| ((full_count as f64) / (limit as f64)).ceil() as i64)
        };

        Ok(Self::construct_search_result(full_count, pages, objects))
    }

    fn construct_search_result(
        full_count: Option<i64>,
        pages: Option<i64>,
        objects: Vec<Self>,
    ) -> SearchResult
    where
        Self: Sized;

    fn get_full_count(&self) -> Option<i64>;

    fn get_evaluated_limit(&self) -> i32;
}

async fn get_search_result<
    T: SearchQueryResultObject + Send + QueryableByName<diesel::pg::Pg> + 'static,
>(
    sql_query: String,
    max_limit: u32,
    connection: &mut AsyncPgConnection,
) -> Result<SearchResult, Error> {
    let instant = std::time::Instant::now();
    let objects = diesel::sql_query(sql_query)
        .load::<T>(connection)
        .await
        .map_err(|e| Error::QueryError(e.to_string()))?;
    log::debug!("Query took {}ms", instant.elapsed().as_millis());

    T::get_search_result(objects, max_limit)
}

fn prepare_query_parameters(
    query_parameters_filter: &QueryParametersFilter,
    user: &Option<User>,
    scope: &Scope,
) -> Result<QueryParameters, Error> {
    let mut variables = HashMap::new();
    variables.insert(
        String::from("current_utc_timestamp"),
        Utc::now().to_string(),
    );
    variables.insert(
        String::from("current_utc_date"),
        Utc::now().date_naive().to_string(),
    );

    if let Some(ref user) = user {
        variables.insert(String::from("current_user_key"), user.pk.to_string());
    }

    match scope {
        Scope::Global => Err(Error::BadRequestError(format!(
            "Invalid scope '{:?}'",
            &scope
        ))),
        Scope::Post => Ok(QueryParameters {
            limit: query_parameters_filter.limit.map(|l| l.to_string()),
            page: query_parameters_filter.page,
            ordering: Vec::new(),
            variables,
            shuffle: query_parameters_filter.shuffle.unwrap_or(false),
            writable_only: query_parameters_filter.writable_only.unwrap_or(false),
            base_table_name: "post",
            tag_relation_table_name: "post_tag",
            select_statements: vec![
                String::from("post.pk AS post_pk"),
                String::from("post.data_url AS post_data_url"),
                String::from("post.source_url AS post_source_url"),
                String::from("post.title AS post_title"),
                String::from("post.creation_timestamp AS post_creation_timestamp"),
                String::from("post.score AS post_score"),
                String::from("post.thumbnail_url AS post_thumbnail_url"),
                String::from("post.public AS post_public"),
                String::from("post.public_edit AS post_public_edit"),
                String::from("post.description AS post_description"),
                String::from("s3_object.thumbnail_object_key AS post_thumbnail_object_key"),
                String::from("create_user.pk AS post_create_user_pk"),
                String::from("create_user.user_name AS post_create_user_user_name"),
                String::from("create_user.user_name AS post_create_user_user_name"),
                String::from("create_user.email AS post_create_user_email"),
                String::from("create_user.avatar_url AS post_create_user_avatar_url"),
                String::from("create_user.creation_timestamp AS post_create_user_creation_timestamp"),
                String::from("create_user.email_confirmed AS post_create_user_email_confirmed"),
                String::from("create_user.display_name AS post_create_user_display_name"),
                String::from("create_user.password_fail_count AS post_create_user_password_fail_count"),
                String::from("s3_object.object_key AS post_s3_object_object_key"),
                String::from("s3_object.sha256_hash AS post_s3_object_sha256_hash"),
                String::from("s3_object.size_bytes AS post_s3_object_size_bytes"),
                String::from("s3_object.mime_type AS post_s3_object_mime_type"),
                String::from("s3_object.fk_broker AS post_s3_object_fk_broker"),
                String::from("s3_object.fk_uploader AS post_s3_object_fk_uploader"),
                String::from("s3_object.thumbnail_object_key AS post_s3_object_thumbnail_object_key"),
                String::from("s3_object.creation_timestamp AS post_s3_object_creation_timestamp"),
                String::from("s3_object.filename AS post_s3_object_filename"),
                String::from("s3_object.hls_master_playlist AS post_s3_object_hls_master_playlist"),
                String::from("s3_object.hls_disabled AS post_s3_object_hls_disabled"),
                String::from("s3_object.hls_locked_at AS post_s3_object_hls_locked_at"),
                String::from("s3_object.thumbnail_locked_at AS post_s3_object_thumbnail_locked_at"),
                String::from("s3_object.hls_fail_count AS post_s3_object_hls_fail_count"),
                String::from("s3_object.thumbnail_fail_count AS post_s3_object_thumbnail_fail_count"),
                String::from("s3_object.thumbnail_disabled AS post_s3_object_thumbnail_disabled"),
                String::from("s3_object_metadata.object_key AS post_s3_object_metadata_object_key"),
                String::from("s3_object_metadata.file_type AS post_s3_object_metadata_file_type"),
                String::from("s3_object_metadata.file_type_extension AS post_s3_object_metadata_file_type_extension"),
                String::from("s3_object_metadata.mime_type AS post_s3_object_metadata_mime_type"),
                String::from("s3_object_metadata.title AS post_s3_object_metadata_title"),
                String::from("s3_object_metadata.artist AS post_s3_object_metadata_artist"),
                String::from("s3_object_metadata.album AS post_s3_object_metadata_album"),
                String::from("s3_object_metadata.album_artist AS post_s3_object_metadata_album_artist"),
                String::from("s3_object_metadata.composer AS post_s3_object_metadata_composer"),
                String::from("s3_object_metadata.genre AS post_s3_object_metadata_genre"),
                String::from("s3_object_metadata.date AS post_s3_object_metadata_date"),
                String::from("s3_object_metadata.track_number AS post_s3_object_metadata_track_number"),
                String::from("s3_object_metadata.track_count AS post_s3_object_metadata_track_count"),
                String::from("s3_object_metadata.disc_number AS post_s3_object_metadata_disc_number"),
                String::from("s3_object_metadata.disc_count AS post_s3_object_metadata_disc_count"),
                String::from("s3_object_metadata.duration::varchar AS post_s3_object_metadata_duration"),
                String::from("s3_object_metadata.width AS post_s3_object_metadata_width"),
                String::from("s3_object_metadata.height AS post_s3_object_metadata_height"),
                String::from("s3_object_metadata.size AS post_s3_object_metadata_size"),
                String::from("s3_object_metadata.bit_rate AS post_s3_object_metadata_bit_rate"),
                String::from("s3_object_metadata.format_name AS post_s3_object_metadata_format_name"),
                String::from("s3_object_metadata.format_long_name AS post_s3_object_metadata_format_long_name"),
                String::from("s3_object_metadata.video_stream_count AS post_s3_object_metadata_video_stream_count"),
                String::from("s3_object_metadata.video_codec_name AS post_s3_object_metadata_video_codec_name"),
                String::from("s3_object_metadata.video_codec_long_name AS post_s3_object_metadata_video_codec_long_name"),
                String::from("s3_object_metadata.video_frame_rate AS post_s3_object_metadata_video_frame_rate"),
                String::from("s3_object_metadata.video_bit_rate_max AS post_s3_object_metadata_video_bit_rate_max"),
                String::from("s3_object_metadata.audio_stream_count AS post_s3_object_metadata_audio_stream_count"),
                String::from("s3_object_metadata.audio_codec_name AS post_s3_object_metadata_audio_codec_name"),
                String::from("s3_object_metadata.audio_codec_long_name AS post_s3_object_metadata_audio_codec_long_name"),
                String::from("s3_object_metadata.audio_sample_rate AS post_s3_object_metadata_audio_sample_rate"),
                String::from("s3_object_metadata.audio_channels AS post_s3_object_metadata_audio_channels"),
                String::from("s3_object_metadata.audio_bit_rate_max AS post_s3_object_metadata_audio_bit_rate_max"),
                String::from("s3_object_metadata.raw AS post_s3_object_metadata_raw"),
            ],
            join_statements: vec![
                String::from("INNER JOIN s3_object ON s3_object.object_key = post.s3_object"),
                String::from("INNER JOIN s3_object_metadata ON s3_object_metadata.object_key = post.s3_object"),
                String::from(
                    "INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user",
                ),
            ],
            max_limit: MAX_LIMIT,
            fallback_orderings: vec![
                Ordering {
                    expression: String::from("post.pk"),
                    direction: Direction::Descending,
                    nullable: false,
                    table: "post",
                },
                Ordering {
                    expression: String::from("s3_object_metadata.object_key"),
                    direction: Direction::Ascending,
                    nullable: false,
                    table: "s3_object_metadata",
                },
            ],
            from_table_override: None,
            predefined_where_conditions: None,
        }),
        Scope::Collection => Ok(QueryParameters {
            limit: query_parameters_filter.limit.map(|l| l.to_string()),
            page: query_parameters_filter.page,
            ordering: Vec::new(),
            variables,
            shuffle: query_parameters_filter.shuffle.unwrap_or(false),
            writable_only: query_parameters_filter.writable_only.unwrap_or(false),
            base_table_name: "post_collection",
            tag_relation_table_name: "post_collection_tag",
            select_statements: vec![
                String::from("post_collection.pk AS post_collection_pk"),
                String::from("post_collection.title AS post_collection_title"),
                String::from("post_collection.creation_timestamp AS post_collection_creation_timestamp"),
                String::from("post_collection.public AS post_collection_public"),
                String::from("post_collection.public_edit AS post_collection_public_edit"),
                String::from("post_collection.description AS post_collection_description"),
                String::from("poster_object.thumbnail_object_key AS post_collection_thumbnail_object_key"),
                String::from("poster_object.object_key AS post_collection_poster_object_key"),
                String::from("poster_object.sha256_hash AS post_collection_poster_sha256_hash"),
                String::from("poster_object.size_bytes AS post_collection_poster_size_bytes"),
                String::from("poster_object.mime_type AS post_collection_poster_mime_type"),
                String::from("poster_object.fk_broker AS post_collection_poster_fk_broker"),
                String::from("poster_object.fk_uploader AS post_collection_poster_fk_uploader"),
                String::from("poster_object.thumbnail_object_key AS post_collection_poster_thumbnail_object_key"),
                String::from("poster_object.creation_timestamp AS post_collection_poster_creation_timestamp"),
                String::from("poster_object.filename AS post_collection_poster_filename"),
                String::from("poster_object.hls_master_playlist AS post_collection_poster_hls_master_playlist"),
                String::from("poster_object.hls_disabled AS post_collection_poster_hls_disabled"),
                String::from("poster_object.hls_locked_at AS post_collection_poster_hls_locked_at"),
                String::from("poster_object.thumbnail_locked_at AS post_collection_poster_thumbnail_locked_at"),
                String::from("poster_object.hls_fail_count AS post_collection_poster_hls_fail_count"),
                String::from("poster_object.thumbnail_fail_count AS post_collection_poster_thumbnail_fail_count"),
                String::from("poster_object.thumbnail_disabled AS post_collection_poster_thumbnail_disabled"),
                String::from("create_user.pk AS post_collection_create_user_pk"),
                String::from("create_user.user_name AS post_collection_create_user_user_name"),
                String::from("create_user.email AS post_collection_create_user_email"),
                String::from("create_user.avatar_url AS post_collection_create_user_avatar_url"),
                String::from("create_user.creation_timestamp AS post_collection_create_user_creation_timestamp"),
                String::from("create_user.email_confirmed AS post_collection_create_user_email_confirmed"),
                String::from("create_user.display_name AS post_collection_create_user_display_name"),
                String::from("create_user.password_fail_count AS post_collection_create_user_password_fail_count"),
            ],
            join_statements: vec![
                String::from("LEFT JOIN s3_object AS poster_object ON poster_object.object_key = post_collection.poster_object_key"),
                String::from("INNER JOIN registered_user create_user ON create_user.pk = post_collection.fk_create_user"),
            ],
            max_limit: MAX_LIMIT,
            fallback_orderings: vec![Ordering {
                expression: String::from("post_collection.pk"),
                direction: Direction::Descending,
                nullable: false,
                table: "post_collection",
            }],
            from_table_override: None,
            predefined_where_conditions: None,
        }),
        Scope::CollectionItem { collection_pk } => Ok(QueryParameters {
            limit: query_parameters_filter.limit.map(|l| l.to_string()),
            page: query_parameters_filter.page,
            ordering: Vec::new(),
            variables,
            shuffle: query_parameters_filter.shuffle.unwrap_or(false),
            writable_only: query_parameters_filter.writable_only.unwrap_or(false),
            base_table_name: "post",
            tag_relation_table_name: "post_tag",
            select_statements: vec![
                String::from("post_collection_item.pk AS post_collection_item_pk"),
                String::from("post_collection_item.ordinal AS post_collection_item_ordinal"),
                String::from("post_collection_item.creation_timestamp AS post_collection_item_creation_timestamp"),
                String::from("post_collection_item_added_user.pk AS post_collection_item_added_user_pk"),
                String::from("post_collection_item_added_user.user_name AS post_collection_item_added_user_user_name"),
                String::from("post_collection_item_added_user.email AS post_collection_item_added_user_email"),
                String::from("post_collection_item_added_user.avatar_url AS post_collection_item_added_user_avatar_url"),
                String::from("post_collection_item_added_user.creation_timestamp AS post_collection_item_added_user_creation_timestamp"),
                String::from("post_collection_item_added_user.email_confirmed AS post_collection_item_added_user_email_confirmed"),
                String::from("post_collection_item_added_user.display_name AS post_collection_item_added_user_display_name"),
                String::from("post_collection_item_added_user.password_fail_count AS post_collection_item_added_user_password_fail_count"),
                // post data
                String::from("post.pk AS post_pk"),
                String::from("post.data_url AS post_data_url"),
                String::from("post.source_url AS post_source_url"),
                String::from("post.title AS post_title"),
                String::from("post.creation_timestamp AS post_creation_timestamp"),
                String::from("post.score AS post_score"),
                String::from("post.thumbnail_url AS post_thumbnail_url"),
                String::from("post.public AS post_public"),
                String::from("post.public_edit AS post_public_edit"),
                String::from("post.description AS post_description"),
                String::from("post_s3_object.thumbnail_object_key AS post_thumbnail_object_key"),
                String::from("post_create_user.pk AS post_create_user_pk"),
                String::from("post_create_user.user_name AS post_create_user_user_name"),
                String::from("post_create_user.user_name AS post_create_user_user_name"),
                String::from("post_create_user.email AS post_create_user_email"),
                String::from("post_create_user.avatar_url AS post_create_user_avatar_url"),
                String::from("post_create_user.creation_timestamp AS post_create_user_creation_timestamp"),
                String::from("post_create_user.email_confirmed AS post_create_user_email_confirmed"),
                String::from("post_create_user.display_name AS post_create_user_display_name"),
                String::from("post_create_user.password_fail_count AS post_create_user_password_fail_count"),
                String::from("post_s3_object.object_key AS post_s3_object_object_key"),
                String::from("post_s3_object.sha256_hash AS post_s3_object_sha256_hash"),
                String::from("post_s3_object.size_bytes AS post_s3_object_size_bytes"),
                String::from("post_s3_object.mime_type AS post_s3_object_mime_type"),
                String::from("post_s3_object.fk_broker AS post_s3_object_fk_broker"),
                String::from("post_s3_object.fk_uploader AS post_s3_object_fk_uploader"),
                String::from("post_s3_object.thumbnail_object_key AS post_s3_object_thumbnail_object_key"),
                String::from("post_s3_object.creation_timestamp AS post_s3_object_creation_timestamp"),
                String::from("post_s3_object.filename AS post_s3_object_filename"),
                String::from("post_s3_object.hls_master_playlist AS post_s3_object_hls_master_playlist"),
                String::from("post_s3_object.hls_disabled AS post_s3_object_hls_disabled"),
                String::from("post_s3_object.hls_locked_at AS post_s3_object_hls_locked_at"),
                String::from("post_s3_object.thumbnail_locked_at AS post_s3_object_thumbnail_locked_at"),
                String::from("post_s3_object.hls_fail_count AS post_s3_object_hls_fail_count"),
                String::from("post_s3_object.thumbnail_fail_count AS post_s3_object_thumbnail_fail_count"),
                String::from("post_s3_object.thumbnail_disabled AS post_s3_object_thumbnail_disabled"),
                String::from("s3_object_metadata.object_key AS post_s3_object_metadata_object_key"),
                String::from("s3_object_metadata.file_type AS post_s3_object_metadata_file_type"),
                String::from("s3_object_metadata.file_type_extension AS post_s3_object_metadata_file_type_extension"),
                String::from("s3_object_metadata.mime_type AS post_s3_object_metadata_mime_type"),
                String::from("s3_object_metadata.title AS post_s3_object_metadata_title"),
                String::from("s3_object_metadata.artist AS post_s3_object_metadata_artist"),
                String::from("s3_object_metadata.album AS post_s3_object_metadata_album"),
                String::from("s3_object_metadata.album_artist AS post_s3_object_metadata_album_artist"),
                String::from("s3_object_metadata.composer AS post_s3_object_metadata_composer"),
                String::from("s3_object_metadata.genre AS post_s3_object_metadata_genre"),
                String::from("s3_object_metadata.date AS post_s3_object_metadata_date"),
                String::from("s3_object_metadata.track_number AS post_s3_object_metadata_track_number"),
                String::from("s3_object_metadata.track_count AS post_s3_object_metadata_track_count"),
                String::from("s3_object_metadata.disc_number AS post_s3_object_metadata_disc_number"),
                String::from("s3_object_metadata.disc_count AS post_s3_object_metadata_disc_count"),
                String::from("s3_object_metadata.duration::varchar AS post_s3_object_metadata_duration"),
                String::from("s3_object_metadata.width AS post_s3_object_metadata_width"),
                String::from("s3_object_metadata.height AS post_s3_object_metadata_height"),
                String::from("s3_object_metadata.size AS post_s3_object_metadata_size"),
                String::from("s3_object_metadata.bit_rate AS post_s3_object_metadata_bit_rate"),
                String::from("s3_object_metadata.format_name AS post_s3_object_metadata_format_name"),
                String::from("s3_object_metadata.format_long_name AS post_s3_object_metadata_format_long_name"),
                String::from("s3_object_metadata.video_stream_count AS post_s3_object_metadata_video_stream_count"),
                String::from("s3_object_metadata.video_codec_name AS post_s3_object_metadata_video_codec_name"),
                String::from("s3_object_metadata.video_codec_long_name AS post_s3_object_metadata_video_codec_long_name"),
                String::from("s3_object_metadata.video_frame_rate AS post_s3_object_metadata_video_frame_rate"),
                String::from("s3_object_metadata.video_bit_rate_max AS post_s3_object_metadata_video_bit_rate_max"),
                String::from("s3_object_metadata.audio_stream_count AS post_s3_object_metadata_audio_stream_count"),
                String::from("s3_object_metadata.audio_codec_name AS post_s3_object_metadata_audio_codec_name"),
                String::from("s3_object_metadata.audio_codec_long_name AS post_s3_object_metadata_audio_codec_long_name"),
                String::from("s3_object_metadata.audio_sample_rate AS post_s3_object_metadata_audio_sample_rate"),
                String::from("s3_object_metadata.audio_channels AS post_s3_object_metadata_audio_channels"),
                String::from("s3_object_metadata.audio_bit_rate_max AS post_s3_object_metadata_audio_bit_rate_max"),
                String::from("s3_object_metadata.raw AS post_s3_object_metadata_raw"),
                // collection data
                String::from("post_collection.pk AS post_collection_pk"),
                String::from("post_collection.title AS post_collection_title"),
                String::from("post_collection.creation_timestamp AS post_collection_creation_timestamp"),
                String::from("post_collection.public AS post_collection_public"),
                String::from("post_collection.public_edit AS post_collection_public_edit"),
                String::from("post_collection.description AS post_collection_description"),
                String::from("poster_object.thumbnail_object_key AS post_collection_thumbnail_object_key"),
                String::from("poster_object.object_key AS post_collection_poster_object_key"),
                String::from("poster_object.sha256_hash AS post_collection_poster_sha256_hash"),
                String::from("poster_object.size_bytes AS post_collection_poster_size_bytes"),
                String::from("poster_object.mime_type AS post_collection_poster_mime_type"),
                String::from("poster_object.fk_broker AS post_collection_poster_fk_broker"),
                String::from("poster_object.fk_uploader AS post_collection_poster_fk_uploader"),
                String::from("poster_object.thumbnail_object_key AS post_collection_poster_thumbnail_object_key"),
                String::from("poster_object.creation_timestamp AS post_collection_poster_creation_timestamp"),
                String::from("poster_object.filename AS post_collection_poster_filename"),
                String::from("poster_object.hls_master_playlist AS post_collection_poster_hls_master_playlist"),
                String::from("poster_object.hls_disabled AS post_collection_poster_hls_disabled"),
                String::from("poster_object.hls_locked_at AS post_collection_poster_hls_locked_at"),
                String::from("poster_object.thumbnail_locked_at AS post_collection_poster_thumbnail_locked_at"),
                String::from("poster_object.hls_fail_count AS post_collection_poster_hls_fail_count"),
                String::from("poster_object.thumbnail_fail_count AS post_collection_poster_thumbnail_fail_count"),
                String::from("poster_object.thumbnail_disabled AS post_collection_poster_thumbnail_disabled"),
                String::from("post_collection_create_user.pk AS post_collection_create_user_pk"),
                String::from("post_collection_create_user.user_name AS post_collection_create_user_user_name"),
                String::from("post_collection_create_user.email AS post_collection_create_user_email"),
                String::from("post_collection_create_user.avatar_url AS post_collection_create_user_avatar_url"),
                String::from("post_collection_create_user.creation_timestamp AS post_collection_create_user_creation_timestamp"),
                String::from("post_collection_create_user.email_confirmed AS post_collection_create_user_email_confirmed"),
                String::from("post_collection_create_user.display_name AS post_collection_create_user_display_name"),
                String::from("post_collection_create_user.password_fail_count AS post_collection_create_user_password_fail_count"),
            ],
            join_statements: vec![
                String::from("INNER JOIN post ON post_collection_item.fk_post = post.pk"),
                String::from("INNER JOIN post_collection ON post_collection_item.fk_post_collection = post_collection.pk"),
                String::from("INNER JOIN registered_user post_collection_item_added_user ON post_collection_item.fk_added_by = post_collection_item_added_user.pk"),
                String::from("INNER JOIN s3_object post_s3_object ON post_s3_object.object_key = post.s3_object"),
                String::from("INNER JOIN s3_object_metadata ON s3_object_metadata.object_key = post.s3_object"),
                String::from("INNER JOIN registered_user post_create_user ON post_create_user.pk = post.fk_create_user"),
                String::from("LEFT JOIN s3_object AS poster_object ON poster_object.object_key = post_collection.poster_object_key"),
                String::from("INNER JOIN registered_user post_collection_create_user ON post_collection_create_user.pk = post_collection.fk_create_user"),
            ],
            max_limit: MAX_LIMIT,
            fallback_orderings: vec![Ordering {
                expression: String::from("post_collection_item.ordinal"),
                direction: Direction::Ascending,
                nullable: false,
                table: "post_collection_item",
            }],
            from_table_override: Some("post_collection_item"),
            predefined_where_conditions: Some(vec![format!("post_collection_item.fk_post_collection = {collection_pk}")]),
        }),
    }
}

#[derive(Deserialize, Validate)]
pub struct AnalyzeQueryRequest {
    pub cursor_pos: Option<usize>,
    #[validate(length(min = 0, max = 1024))]
    pub query: String,
    pub scope: Option<String>,
}

#[derive(Serialize)]
pub struct AnalyzeQueryResponse {
    pub error: Option<QueryCompilationError>,
    pub suggestions: Vec<QueryAutocompleteSuggestion>,
}

#[derive(Serialize)]
pub struct QueryAutocompleteSuggestion {
    pub text: String,
    pub display: String,
    pub target_location: Location,
    pub suggestion_type: QueryAutocompleteSuggestionType,
}

#[derive(Serialize)]
pub struct QueryAutocompleteSuggestionType {
    pub name: &'static str,
    pub prefix: Option<&'static str>,
}

#[derive(Serialize)]
pub struct QueryCompilationError {
    pub phase: String,
    pub errors: Vec<compiler::Error>,
}

pub async fn analyze_query_handler(request: AnalyzeQueryRequest) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        Error::InvalidRequestInputError(format!("Validation failed for AnalyzeQueryRequest: {}", e))
    })?;

    let mut log = Log { errors: Vec::new() };
    let query_len = request.query.len();
    let mut ast = match compiler::compile_ast(request.query, &mut log, false) {
        Ok(ast) => ast,
        Err(Error::QueryCompilationError(phase, errors)) => {
            return Ok(warp::reply::json(&AnalyzeQueryResponse {
                error: Some(QueryCompilationError { phase, errors }),
                suggestions: Vec::new(),
            }));
        }
        Err(e) => return Err(warp::reject::custom(e)),
    };

    let scope = request
        .scope
        .map(|scope| {
            Scope::from_str(scope.as_str()).map_err(|_| {
                warp::reject::custom(Error::BadRequestError(format!(
                    "Invalid scope '{}'",
                    &scope
                )))
            })
        })
        .unwrap_or(Ok(Scope::Post))?;

    // find the statement and nested expression at the current cursor position
    // the cursor position refers to the index of the char to the right of the cursor
    // so, the cursor position at the start of the string is 0 and equal to the length of the string at the end of the string
    // thus, for an expression to lie at the given cursor position its start location must be lower than cursor position (cursor being located in front of the expression doesn't count)
    // and its end location + 1 greater or equal to the cursor position (cursor located after expression counts for autocomplete)
    //
    // | = cursor
    // "|test" start = 0, end = 3, cursor = 0 -> doesn't count
    // "te|st" start = 0, end = 3, cursor = 2 -> counts
    // "test|" start = 0, end = 3, cursor = 4 -> counts

    let pos = request.cursor_pos.unwrap_or(query_len);
    let current_statement = ast
        .node_type
        .statements
        .iter()
        .find(|statement| statement.location.start < pos && statement.location.end + 1 >= pos);

    let suggestions = if let Some(current_statement) = current_statement {
        if let Some(expression_statement) = current_statement
            .node_type
            .downcast_ref::<ExpressionStatement>()
        {
            let expression = find_nested_expression(pos, &expression_statement.expression_node);

            find_expression_autocomplete_suggestions(expression, &scope).await?
        } else if let Some(modifier_statement) =
            current_statement.node_type.downcast_ref::<ModifierNode>()
        {
            let nested_expression = modifier_statement.unnest().find(|expression| {
                expression.location.start < pos && expression.location.end + 1 >= pos
            });

            if let Some(nested_expression) = nested_expression {
                let expression = find_nested_expression(pos, nested_expression);
                find_expression_autocomplete_suggestions(expression, &scope).await?
            } else {
                find_matching_map_keys(
                    &scope.get_modifiers(),
                    &modifier_statement.identifier,
                    current_statement.location,
                    "%",
                    "modifier",
                    "()",
                )
            }
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let error = if log.errors.is_empty() {
        let mut semantic_analysis_visitor = SemanticAnalysisVisitor {};
        ast.accept(&mut semantic_analysis_visitor, &scope, &mut log);
        if !log.errors.is_empty() {
            Some(QueryCompilationError {
                phase: String::from("semantic analysis"),
                errors: log.errors,
            })
        } else {
            None
        }
    } else {
        None
    };

    Ok(warp::reply::json(&AnalyzeQueryResponse {
        error,
        suggestions,
    }))
}

#[inline]
pub fn string_is_valid_ident(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().map(char::is_alphabetic).unwrap_or(false)
        && chars.all(lexer::char_is_valid_identifier)
        && lexer::Tag::try_from(s).is_err()
}

async fn find_expression_autocomplete_suggestions(
    expression: &Node<dyn ExpressionNode>,
    scope: &Scope,
) -> Result<Vec<QueryAutocompleteSuggestion>, Error> {
    if let Some(post_tag_node) = expression.node_type.downcast_ref::<PostTagNode>() {
        let mut connection = acquire_db_connection().await?;
        let found_tags = tag::table
            .filter(
                lower(tag::tag_name).like(format!("{}%", post_tag_node.identifier.to_lowercase())),
            )
            .order_by((char_length(tag::tag_name).asc(), tag::tag_name.asc()))
            .limit(10)
            .load::<Tag>(&mut connection)
            .await
            .map_err(Error::from)?;

        let tag_suggestions = found_tags
            .into_iter()
            .map(|tag| QueryAutocompleteSuggestion {
                text: if string_is_valid_ident(&tag.tag_name) {
                    tag.tag_name.clone()
                } else {
                    format!("`{}`", &tag.tag_name)
                },
                display: tag.tag_name,
                target_location: expression.location,
                suggestion_type: QueryAutocompleteSuggestionType {
                    name: "tag",
                    prefix: None,
                },
            })
            .collect::<Vec<_>>();

        Ok(tag_suggestions)
    } else if let Some(attribute_node) = expression.node_type.downcast_ref::<AttributeNode>() {
        Ok(find_matching_map_keys(
            &scope.get_attributes(),
            &attribute_node.identifier,
            expression.location,
            "@",
            "attribute",
            "",
        ))
    } else if let Some(function_call_node) = expression.node_type.downcast_ref::<FunctionCallNode>()
    {
        Ok(find_matching_map_keys(
            &scope.get_functions(),
            &function_call_node.identifier,
            expression.location,
            ".",
            "function",
            "()",
        ))
    } else if let Some(variable_node) = expression.node_type.downcast_ref::<VariableNode>() {
        Ok(find_matching_map_keys(
            &scope.get_variables(),
            &variable_node.identifier,
            expression.location,
            ":",
            "variable",
            "",
        ))
    } else {
        Ok(Vec::new())
    }
}

fn find_matching_map_keys<T>(
    map: &HashMap<&'static str, T>,
    entered_identifier: &str,
    target_location: Location,
    suggestion_type_prefix: &'static str,
    suggestion_type_name: &'static str,
    suggestion_type_suffix: &'static str,
) -> Vec<QueryAutocompleteSuggestion> {
    let mut matching_keys = map
        .keys()
        .filter(|key| key.starts_with(&entered_identifier.to_lowercase()))
        .collect::<Vec<_>>();

    matching_keys.sort_unstable_by(|a, b| match a.len().cmp(&b.len()) {
        cmp::Ordering::Equal => a.cmp(b),
        o => o,
    });

    matching_keys
        .into_iter()
        .take(10)
        .map(|key| QueryAutocompleteSuggestion {
            text: format!(
                "{}{}{}",
                suggestion_type_prefix, key, suggestion_type_suffix
            ),
            display: String::from(*key),
            target_location,
            suggestion_type: QueryAutocompleteSuggestionType {
                name: suggestion_type_name,
                prefix: Some(suggestion_type_prefix),
            },
        })
        .collect()
}

fn find_nested_expression(
    pos: usize,
    expression: &Node<dyn ExpressionNode>,
) -> &Node<dyn ExpressionNode> {
    let found_nested_expression = expression
        .node_type
        .unnest()
        .find(|expression| expression.location.start < pos && expression.location.end + 1 >= pos);

    if let Some(nested_expression) = found_nested_expression {
        find_nested_expression(pos, nested_expression)
    } else {
        expression
    }
}

pub mod functions {
    use diesel::sql_types::Integer;
    use diesel::sql_types::Text;
    sql_function!(fn lower(x: Text) -> Text);
    sql_function!(fn char_length(x: Text) -> Integer);
    sql_function!(fn substring(s: Text, start: Integer, len: Integer) -> Text);
}
