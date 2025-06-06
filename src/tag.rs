use chrono::{DateTime, Utc};
use diesel::{
    BoolExpressionMethods, ExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl,
    TextExpressionMethods, dsl::exists,
};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{Ordering, Reverse},
    collections::HashMap,
    hash::{Hash, Hasher},
};
use validator::ValidationError;
use warp::{reject::Rejection, reply::Reply};

use crate::{
    acquire_db_connection,
    error::{Error, TransactionRuntimeError},
    model::{NewTag, Tag, TagClosureTable, TagEdge},
    query::functions::{char_length, lower},
    retry_on_constraint_violation,
    schema::{self, tag, tag_alias, tag_closure_table, tag_edge},
};

macro_rules! get_source_object_tag {
    ($tag_relation_table:ident, $source_object_pk:expr, $tag_relation_fk_source_object:expr, $connection:expr) => {
        $tag_relation_table::table
            .inner_join(tag::table)
            .select((tag::table::all_columns(), $tag_relation_table::auto_matched))
            .filter($tag_relation_fk_source_object.eq($source_object_pk))
            .load::<(Tag, bool)>($connection)
            .await?
            .into_iter()
            .map(|(tag, auto_matched)| TagUsage { tag, auto_matched })
            .collect::<Vec<_>>()
    };
}

pub mod auto_matching;
pub mod create;
pub mod history;
pub mod update;

use crate::model::{TagCategory, User, UserPublic};
use crate::schema::{registered_user, tag_category};
pub(crate) use get_source_object_tag;

/// Filter redundant tags by removing tags that are a parent of another included tag or a shorter alias
/// for another included tag.
pub async fn filter_redundant_tags(
    tags: &mut Vec<Tag>,
    connection: &mut AsyncPgConnection,
) -> Result<(), Error> {
    let mut selected_tags = Vec::new();
    for tag in tags.iter() {
        selected_tags.push(get_tag_hierarchy_information(tag, connection).await?);
    }

    tags.retain(|tag| {
        selected_tags.iter().all(|other| {
            if tag.pk == other.tag.pk {
                return true;
            }

            let other_tag = &other.tag;
            let other_aliases = &other.tag_aliases;
            let is_parent = other.parent_depth_map.contains_key(&tag.pk);

            let is_shorter_alias = other_aliases
                .iter()
                .any(|alias| alias.pk == tag.pk && tag.tag_name.len() < other_tag.tag_name.len());

            !is_parent && !is_shorter_alias
        })
    });

    Ok(())
}

pub struct TagHierarchyInformation {
    pub tag: Tag,
    /// Mapping for the parent tag pks to their respective depth
    pub parent_depth_map: HashMap<i64, i32>,
    pub tag_aliases: Vec<Tag>,
}

pub async fn get_tag_hierarchy_information(
    tag: &Tag,
    connection: &mut AsyncPgConnection,
) -> Result<TagHierarchyInformation, Error> {
    let tag_closure_table = tag_closure_table::table
        .filter(
            tag_closure_table::fk_child
                .eq(tag.pk)
                .and(tag_closure_table::depth.gt(0)),
        )
        .load::<TagClosureTable>(connection)
        .await?;

    let mut parent_depth_map = HashMap::new();
    for tag_closure in tag_closure_table.iter() {
        parent_depth_map.insert(tag_closure.fk_parent, tag_closure.depth);
    }

    let tag_aliases = get_tag_aliases(tag.pk, connection).await?;

    Ok(TagHierarchyInformation {
        tag: tag.clone(),
        parent_depth_map,
        tag_aliases,
    })
}

#[derive(Serialize)]
pub struct FindTagResponse {
    exact_match: Option<Tag>,
    suggestions: Vec<Tag>,
}

/// Find tags that match or start with the provided prefix tag_name. Returns the exact match and / or up to ten
/// suggestions starting with the provided prefix, ordered by length of tag_name.
pub async fn find_tag_handler(tag_name: String) -> Result<impl Reply, Rejection> {
    if tag_name.is_empty() {
        return Ok(warp::reply::json(&FindTagResponse {
            exact_match: None,
            suggestions: Vec::new(),
        }));
    }

    let tag_name = percent_encoding::percent_decode(tag_name.as_bytes())
        .decode_utf8()
        .map_err(|_| Error::UtfEncodingError)?;

    let mut connection = acquire_db_connection().await?;
    let mut found_tags = tag::table
        .filter(lower(tag::tag_name).like(format!("{}%", tag_name.to_lowercase())))
        .order_by((char_length(tag::tag_name).asc(), tag::tag_name.asc()))
        .limit(10)
        .load::<Tag>(&mut connection)
        .await
        .map_err(Error::from)?;

    if found_tags.is_empty() {
        Ok(warp::reply::json(&FindTagResponse {
            exact_match: None,
            suggestions: found_tags,
        }))
    } else if found_tags[0].tag_name.len() == tag_name.len() {
        Ok(warp::reply::json(&FindTagResponse {
            exact_match: Some(found_tags.remove(0)),
            suggestions: found_tags,
        }))
    } else {
        Ok(warp::reply::json(&FindTagResponse {
            exact_match: None,
            suggestions: found_tags,
        }))
    }
}

/// Struct containing values commonly held by tag usages, such as post_tag and post_collection_tag,
/// as well as the fully loaded Tag
#[derive(Clone, Serialize)]
pub struct TagUsage {
    pub tag: Tag,
    pub auto_matched: bool,
}

impl From<TagUsage> for Tag {
    fn from(value: TagUsage) -> Self {
        value.tag
    }
}

impl PartialEq for TagUsage {
    fn eq(&self, other: &Self) -> bool {
        self.tag.pk == other.tag.pk
    }
}

impl Eq for TagUsage {}

impl Hash for TagUsage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag.pk.hash(state);
    }
}

impl PartialOrd for TagUsage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TagUsage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tag.pk.cmp(&other.tag.pk)
    }
}

pub async fn get_tag_categories_handler() -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let tag_categories = tag_category::table
        .order(tag_category::id.asc())
        .load::<TagCategory>(&mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&tag_categories))
}

#[derive(Serialize)]
pub struct TagDetailed {
    pub pk: i64,
    pub tag_name: String,
    pub creation_timestamp: DateTime<Utc>,
    pub create_user: UserPublic,
    pub edit_timestamp: DateTime<Utc>,
    pub edit_user: UserPublic,
    pub tag_category: Option<TagCategory>,
    pub auto_match_condition_post: Option<String>,
    pub auto_match_condition_collection: Option<String>,
    pub parents: Vec<Tag>,
    pub aliases: Vec<Tag>,
}

pub async fn load_tag_detailed(
    tag: Tag,
    connection: &mut AsyncPgConnection,
) -> Result<TagDetailed, Error> {
    let aliases = get_tag_aliases(tag.pk, connection)
        .await
        .map_err(Error::from)?;
    let parents = get_tag_parents(tag.pk, connection)
        .await
        .map_err(Error::from)?;

    let (create_user, edit_user) = diesel::alias!(
        schema::registered_user as create_user,
        schema::registered_user as edit_user,
    );
    let (_, tag_category, create_user, edit_user) = tag::table
        .left_join(tag_category::table)
        .inner_join(create_user.on(tag::fk_create_user.eq(create_user.field(registered_user::pk))))
        .inner_join(edit_user.on(tag::fk_edit_user.eq(edit_user.field(registered_user::pk))))
        .filter(tag::pk.eq(tag.pk))
        .get_result::<(Tag, Option<TagCategory>, UserPublic, UserPublic)>(connection)
        .await
        .map_err(Error::from)?;

    Ok(TagDetailed {
        pk: tag.pk,
        tag_name: tag.tag_name,
        creation_timestamp: tag.creation_timestamp,
        create_user,
        edit_timestamp: tag.edit_timestamp,
        edit_user,
        tag_category,
        auto_match_condition_post: tag.auto_match_condition_post,
        auto_match_condition_collection: tag.auto_match_condition_collection,
        parents,
        aliases,
    })
}

#[derive(Serialize)]
pub struct TagJoined {
    pub tag: Tag,
    pub category: Option<TagCategory>,
    pub parents: Vec<Tag>,
    pub aliases: Vec<Tag>,
}

pub async fn get_tag_handler(tag_pk: i64) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let (create_user, edit_user) = diesel::alias!(
        schema::registered_user as create_user,
        schema::registered_user as edit_user,
    );
    let (tag, tag_category, create_user, edit_user) = tag::table
        .left_join(tag_category::table)
        .inner_join(create_user.on(tag::fk_create_user.eq(create_user.field(registered_user::pk))))
        .inner_join(edit_user.on(tag::fk_edit_user.eq(edit_user.field(registered_user::pk))))
        .filter(tag::pk.eq(tag_pk))
        .get_result::<(Tag, Option<TagCategory>, UserPublic, UserPublic)>(&mut connection)
        .await
        .optional()
        .map_err(Error::from)?
        .ok_or(Error::NotFoundError)?;

    let aliases = get_tag_aliases(tag_pk, &mut connection)
        .await
        .map_err(Error::from)?;
    let parents = get_tag_parents(tag_pk, &mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&TagDetailed {
        pk: tag.pk,
        tag_name: tag.tag_name,
        creation_timestamp: tag.creation_timestamp,
        create_user,
        edit_timestamp: tag.edit_timestamp,
        edit_user,
        tag_category,
        auto_match_condition_post: tag.auto_match_condition_post,
        auto_match_condition_collection: tag.auto_match_condition_collection,
        parents,
        aliases,
    }))
}

#[derive(Deserialize)]
pub struct GetTagsFilter {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub filter: Option<String>,
}

#[derive(Serialize)]
pub struct GetTagsResponse {
    tags: Vec<Tag>,
    count: i64,
}

pub async fn get_tags_handler(mut get_tags_filter: GetTagsFilter) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let limit = get_tags_filter.limit.unwrap_or(1000);
    let page = get_tags_filter.page.unwrap_or(0);

    if let Some(ref mut filter) = get_tags_filter.filter {
        *filter = filter.trim().to_string();
        if filter.is_empty() {
            get_tags_filter.filter = None;
        }
    }

    let (tags, count) = if let Some(filter) = get_tags_filter.filter {
        let tags = tag::table
            .filter(lower(tag::tag_name).like(format!("%{}%", filter.to_lowercase())))
            .order_by(lower(tag::tag_name).asc())
            .limit(limit as i64)
            .offset((page * limit) as i64)
            .load::<Tag>(&mut connection)
            .await
            .map_err(Error::from)?;

        let count = tag::table
            .filter(lower(tag::tag_name).like(format!("%{}%", filter.to_lowercase())))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .map_err(Error::from)?;

        (tags, count)
    } else {
        let tags = tag::table
            .order_by(lower(tag::tag_name).asc())
            .limit(limit as i64)
            .offset((page * limit) as i64)
            .load::<Tag>(&mut connection)
            .await
            .map_err(Error::from)?;

        let count = tag::table
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .map_err(Error::from)?;
        (tags, count)
    };

    Ok(warp::reply::json(&GetTagsResponse { tags, count }))
}

#[derive(Serialize)]
pub struct GetTagHierarchyResponse {
    tag: Tag,
    ancestors: Vec<TagHierarchyNode>,
    descendants: Vec<TagHierarchyNode>,
}

#[derive(Serialize)]
pub struct TagHierarchyNode {
    pub tag: Tag,
    pub depth: i32,
    pub edges: Vec<TagEdge>,
}

/// Get the entire hierarchy of the given tag, from its root ancestors to its leaf descendants. Only returns ancestors and descendants, not siblings or cousins etc.
///
/// Returns the loaded tag data, the tag's ancestors including their depth and edges to their children, and the tag's descendants including their depth and edges to their parents.
///
/// Note that ancestors and descendants may contain the same tag multiple times if it is reachable through multiple paths at different depths.
pub async fn get_tag_hierarchy_handler(tag_pk: i64) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let tag = tag::table
        .find(tag_pk)
        .get_result::<Tag>(&mut connection)
        .await
        .map_err(Error::from)?;

    let inner_tag_closure = diesel::alias!(schema::tag_closure_table as inner_tag_closure);
    let ancestor_closures = tag_closure_table::table
        .inner_join(tag::table.on(tag::pk.eq(tag_closure_table::fk_parent)))
        .inner_join(
            tag_edge::table.on(tag_closure_table::fk_parent
                .eq(tag_edge::fk_parent)
                .and(exists(
                    // only include edges to ancestors of the tag or the tag itself, not "uncles" / siblings of ancestors
                    inner_tag_closure.filter(
                        inner_tag_closure
                            .field(tag_closure_table::fk_parent)
                            .eq(tag_edge::fk_child)
                            .and(
                                inner_tag_closure
                                    .field(tag_closure_table::fk_child)
                                    .eq(tag_pk),
                            ),
                    ),
                ))),
        )
        .filter(
            tag_closure_table::fk_child
                .eq(tag_pk)
                .and(tag_closure_table::depth.gt(0)),
        )
        .load::<(TagClosureTable, Tag, TagEdge)>(&mut connection)
        .await
        .map_err(Error::from)?;

    let mut ancestor_map = HashMap::new();
    for (tag_closure, tag, tag_edge) in ancestor_closures {
        let ancestor_node =
            ancestor_map
                .entry(tag_closure.pk)
                .or_insert_with(|| TagHierarchyNode {
                    tag,
                    depth: tag_closure.depth,
                    edges: Vec::new(),
                });

        ancestor_node.edges.push(tag_edge);
    }

    let descendant_closures = tag_closure_table::table
        .inner_join(tag::table.on(tag::pk.eq(tag_closure_table::fk_child)))
        .inner_join(
            tag_edge::table.on(tag_closure_table::fk_child
                .eq(tag_edge::fk_child)
                .and(exists(
                    // only include edges to descendants of the tag or the tag itself, not "nephews" / siblings of descendants
                    inner_tag_closure.filter(
                        inner_tag_closure
                            .field(tag_closure_table::fk_child)
                            .eq(tag_edge::fk_parent)
                            .and(
                                inner_tag_closure
                                    .field(tag_closure_table::fk_parent)
                                    .eq(tag_pk),
                            ),
                    ),
                ))),
        )
        .filter(
            tag_closure_table::fk_parent
                .eq(tag_pk)
                .and(tag_closure_table::depth.gt(0)),
        )
        .load::<(TagClosureTable, Tag, TagEdge)>(&mut connection)
        .await
        .map_err(Error::from)?;

    let mut descendant_map = HashMap::new();
    for (tag_closure, tag, tag_edge) in descendant_closures {
        let descendant_node =
            descendant_map
                .entry(tag_closure.pk)
                .or_insert_with(|| TagHierarchyNode {
                    tag,
                    depth: tag_closure.depth,
                    edges: Vec::new(),
                });

        descendant_node.edges.push(tag_edge);
    }

    Ok(warp::reply::json(&GetTagHierarchyResponse {
        tag,
        ancestors: ancestor_map
            .into_iter()
            .sorted_by_key(|(_, v)| Reverse(v.depth))
            .map(|(_, v)| v)
            .collect(),
        descendants: descendant_map
            .into_iter()
            .sorted_by_key(|(_, v)| v.depth)
            .map(|(_, v)| v)
            .collect(),
    }))
}

pub async fn get_or_create_tag(
    tag_name: &str,
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<(bool, Tag), TransactionRuntimeError> {
    let existing_tag = tag::table
        .filter(lower(tag::tag_name).eq(tag_name.to_lowercase()))
        .first::<Tag>(connection)
        .await
        .optional()?;

    let inserted = existing_tag.is_none();
    let tag = match existing_tag {
        Some(existing_tag) => existing_tag,
        None => diesel::insert_into(tag::table)
            .values(&NewTag {
                tag_name: String::from(tag_name),
                fk_create_user: user.pk,
                tag_category: None,
                auto_match_condition_post: None,
                auto_match_condition_collection: None,
            })
            .get_result::<Tag>(connection)
            .await
            .map_err(retry_on_constraint_violation)?,
    };

    Ok((inserted, tag))
}

pub async fn get_tag_parents_pks(
    tag_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<i64>, diesel::result::Error> {
    tag_edge::table
        .select(tag_edge::fk_parent)
        .filter(tag_edge::fk_child.eq(tag_pk))
        .load::<i64>(connection)
        .await
}

pub async fn get_tag_parents(
    tag_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, diesel::result::Error> {
    let parent_tag = diesel::alias!(schema::tag as parent_tag);

    tag_edge::table
        .inner_join(parent_tag.on(tag_edge::fk_parent.eq(parent_tag.field(tag::pk))))
        .select(parent_tag.fields(tag::all_columns))
        .filter(tag_edge::fk_child.eq(tag_pk))
        .load::<Tag>(connection)
        .await
}

pub async fn get_tag_aliases_pks(
    tag_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<i64>, diesel::result::Error> {
    tag::table
        .select(tag::pk)
        .filter(exists(
            tag_alias::table.filter(
                (tag_alias::fk_source
                    .eq(tag_pk)
                    .and(tag_alias::fk_target.eq(tag::pk)))
                .or(tag_alias::fk_source
                    .eq(tag::pk)
                    .and(tag_alias::fk_target.eq(tag_pk))),
            ),
        ))
        .load::<i64>(connection)
        .await
}

pub async fn get_tag_aliases(
    tag_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, diesel::result::Error> {
    tag::table
        .filter(exists(
            tag_alias::table.filter(
                (tag_alias::fk_source
                    .eq(tag_pk)
                    .and(tag_alias::fk_target.eq(tag::pk)))
                .or(tag_alias::fk_source
                    .eq(tag::pk)
                    .and(tag_alias::fk_target.eq(tag_pk))),
            ),
        ))
        .load::<Tag>(connection)
        .await
}

pub fn validate_tags(tags: &[String]) -> Result<(), ValidationError> {
    for tag in tags {
        if !tag_is_valid(tag) {
            return Err(ValidationError::new(
                "Invalid tag, tags must be 50 or less in length",
            ));
        }
    }

    Ok(())
}

pub fn validate_tag(tag: &str) -> Result<(), ValidationError> {
    if !tag_is_valid(tag) {
        return Err(ValidationError::new(
            "Invalid tag, tags must not be blank and be 50 or less in length",
        ));
    }

    Ok(())
}

#[inline]
pub fn tag_is_valid(tag: &str) -> bool {
    let tag = sanitize_tag(tag);
    tag.len() <= 50 && !tag.is_empty()
}

lazy_static! {
    pub static ref WHITESPACE_REGEX: Regex = Regex::new("\\s+").unwrap();
}

#[inline]
pub fn sanitize_tag(tag: &str) -> String {
    WHITESPACE_REGEX.replace_all(tag.trim(), " ").into_owned()
}

pub fn sanitize_request_tags(request_tags: &[String]) -> Vec<String> {
    request_tags
        .iter()
        .map(String::as_str)
        .map(sanitize_tag)
        .unique()
        .collect::<Vec<_>>()
}
