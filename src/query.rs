use std::{cmp, collections::HashMap};

use chrono::{DateTime, Utc};
use diesel::{OptionalExtension, QueryDsl, RunQueryDsl};
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{Rejection, Reply};

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, TextExpressionMethods},
    error::Error,
    model::{PostQueryObject, PostWindowQueryObject, S3Object, Tag, User},
    perms,
    post::{self, PostGroupAccessDetailed},
    schema::tag,
};

use self::{
    compiler::{
        ast::{
            AttributeNode, ExpressionNode, ExpressionStatement, FunctionCallNode, ModifierNode,
            Node, PostTagNode, SemanticAnalysisVisitor, StatementNode, VariableNode,
        },
        dict, lexer, Location, Log,
    },
    functions::{char_length, lower},
};

pub mod compiler;

const DEFAULT_LIMIT_STR: &str = "50";
const MAX_LIMIT: u16 = 100;
const MAX_LIMIT_STR: &str = "100";
const MAX_RANDOMISE_LIMIT_STR: &str = "10000";

#[derive(Deserialize, Validate)]
pub struct QueryParametersFilter {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    #[validate(length(min = 0, max = 1024))]
    pub query: Option<String>,
    pub exclude_window: Option<bool>,
    pub randomise: Option<bool>,
}

pub struct QueryParameters {
    pub limit: Option<String>,
    pub page: Option<u32>,
    pub ordering: Vec<Ordering>,
    pub variables: HashMap<String, String>,
    pub randomise: bool,
}

#[derive(Debug)]
pub struct Ordering {
    pub expression: String,
    pub direction: Direction,
    pub nullable: bool,
}

#[derive(Debug)]
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
    let posts = diesel::sql_query(sql_query)
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
    pub prev_post: Option<PostWindowObject>,
    pub next_post: Option<PostWindowObject>,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_edit: bool,
    pub description: Option<String>,
    pub is_editable: bool,
    pub tags: Vec<Tag>,
    pub group_access: Vec<PostGroupAccessDetailed>,
}

#[derive(Serialize)]
pub struct PostWindowObject {
    pk: i32,
    page: u32,
}

pub async fn get_post_handler(
    user: Option<User>,
    post_pk: i32,
    query_parameters_filter: QueryParametersFilter,
) -> Result<impl Reply, Rejection> {
    query_parameters_filter.validate().map_err(|e| {
        Error::InvalidRequestInputError(format!(
            "Validation failed for QueryParametersFilter: {}",
            e
        ))
    })?;

    let mut connection = acquire_db_connection()?;

    let (post, s3_object) = perms::load_post_secured(post_pk, &mut connection, user.as_ref())?;

    let page = query_parameters_filter.page;
    let exclude_window = query_parameters_filter.exclude_window;
    let query_parameters = prepare_query_parameters(&query_parameters_filter, &user);
    let (prev_post, next_post) = if !exclude_window.unwrap_or(false) {
        let sql_query = compiler::compile_window_query(
            post.pk,
            query_parameters_filter.query,
            query_parameters,
            &user,
        )?;
        let mut connection = acquire_db_connection()?;
        let result = diesel::sql_query(sql_query)
            .get_result::<PostWindowQueryObject>(&mut connection)
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

    let is_editable = perms::is_post_editable(&mut connection, user.as_ref(), post_pk)?;
    let tags = post::get_post_tags(post_pk, &mut connection).map_err(Error::from)?;
    let group_access = post::get_post_group_access(post_pk, user.as_ref(), &mut connection)
        .map_err(Error::from)?;

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
        prev_post,
        next_post,
        public: post.public,
        public_edit: post.public_edit,
        description: post.description,
        is_editable,
        tags,
        group_access,
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
    variables.insert(
        String::from("current_utc_date"),
        Utc::now().date_naive().to_string(),
    );

    if let Some(ref user) = user {
        variables.insert(String::from("current_user_key"), user.pk.to_string());
    }

    QueryParameters {
        limit: query_parameters_filter.limit.map(|l| l.to_string()),
        page: query_parameters_filter.page,
        ordering: Vec::new(),
        variables,
        randomise: query_parameters_filter.randomise.unwrap_or(false),
    }
}

#[derive(Deserialize, Validate)]
pub struct AnalyzeQueryRequest {
    pub cursor_pos: Option<usize>,
    #[validate(length(min = 0, max = 1024))]
    pub query: String,
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
    let ast = match compiler::compile_ast(request.query, &mut log, false) {
        Ok(ast) => ast,
        Err(Error::QueryCompilationError(phase, errors)) => {
            return Ok(warp::reply::json(&AnalyzeQueryResponse {
                error: Some(QueryCompilationError { phase, errors }),
                suggestions: Vec::new(),
            }))
        }
        Err(e) => return Err(warp::reject::custom(e)),
    };

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

            find_expression_autocomplete_suggestions(expression)?
        } else if let Some(modifier_statement) =
            current_statement.node_type.downcast_ref::<ModifierNode>()
        {
            let nested_expression = modifier_statement.unnest().find(|expression| {
                expression.location.start < pos && expression.location.end + 1 >= pos
            });

            if let Some(nested_expression) = nested_expression {
                let expression = find_nested_expression(pos, nested_expression);
                find_expression_autocomplete_suggestions(expression)?
            } else {
                find_matching_map_keys(
                    &dict::MODIFIERS,
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
        ast.accept(&mut semantic_analysis_visitor, &mut log);
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
}

fn find_expression_autocomplete_suggestions(
    expression: &Node<dyn ExpressionNode>,
) -> Result<Vec<QueryAutocompleteSuggestion>, Error> {
    if let Some(post_tag_node) = expression.node_type.downcast_ref::<PostTagNode>() {
        let mut connection = acquire_db_connection()?;
        let found_tags = tag::table
            .filter(
                lower(tag::tag_name).like(format!("{}%", post_tag_node.identifier.to_lowercase())),
            )
            .order_by((char_length(tag::tag_name).asc(), tag::tag_name.asc()))
            .limit(10)
            .load::<Tag>(&mut connection)
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
            &dict::ATTRIBUTES,
            &attribute_node.identifier,
            expression.location,
            "@",
            "attribute",
            "",
        ))
    } else if let Some(function_call_node) = expression.node_type.downcast_ref::<FunctionCallNode>()
    {
        Ok(find_matching_map_keys(
            &dict::FUNCTIONS,
            &function_call_node.identifier,
            expression.location,
            ".",
            "function",
            "()",
        ))
    } else if let Some(variable_node) = expression.node_type.downcast_ref::<VariableNode>() {
        Ok(find_matching_map_keys(
            &dict::VARIABLES,
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
    use diesel::sql_types::Text;
    sql_function!(fn lower(x: Text) -> Text);
    sql_function!(fn char_length(x: Text) -> Integer);
}
