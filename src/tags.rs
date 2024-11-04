use std::{cmp::Reverse, collections::HashMap};

use diesel::{
    dsl::exists, BoolExpressionMethods, ExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl,
    TextExpressionMethods,
};
use diesel_async::{scoped_futures::ScopedFutureExt, AsyncPgConnection, RunQueryDsl};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};
use warp::{reject::Rejection, reply::Reply};

use crate::{
    acquire_db_connection,
    error::{Error, TransactionRuntimeError},
    model::{NewTag, Tag, TagAlias, TagClosureTable, TagEdge, User},
    query::{
        functions::{char_length, lower},
        load_and_report_missing_pks, report_missing_pks,
    },
    retry_on_constraint_violation, run_retryable_transaction, run_serializable_transaction,
    schema::{self, tag, tag_alias, tag_closure_table, tag_edge},
    util::{self, dedup_vec_optional, dedup_vecs_optional},
};

macro_rules! get_source_object_tag {
    ($tag_relation_table:ident, $source_object_pk:expr, $tag_relation_fk_source_object:expr, $connection:expr) => {
        $tag_relation_table::table
            .inner_join(tag::table)
            .select(tag::table::all_columns())
            .filter($tag_relation_fk_source_object.eq($source_object_pk))
            .load::<Tag>($connection)
    };
}

pub(crate) use get_source_object_tag;

#[derive(Deserialize, Validate)]
pub struct CreateTagsRequest {
    #[validate(custom(function = "validate_tags"))]
    pub tag_names: Vec<String>,
}

#[derive(Serialize)]
pub struct CreateTagsResponse {
    pub existing_tags: Vec<Tag>,
    pub inserted_tags: Vec<Tag>,
}

pub async fn create_tags_handler(
    create_tags_request: CreateTagsRequest,
    _user: User,
) -> Result<impl Reply, Rejection> {
    create_tags_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateTagsRequest: {}",
            e
        )))
    })?;

    let mut tag_names = sanitize_request_tags(&create_tags_request.tag_names);
    util::dedup_vec(&mut tag_names);
    let mut connection = acquire_db_connection().await?;
    run_retryable_transaction(&mut connection, |connection| {
        async move {
            let (existing_tags, inserted_tags) = get_or_create_tags(connection, &tag_names).await?;
            Ok(warp::reply::json(&CreateTagsResponse {
                existing_tags,
                inserted_tags,
            }))
        }
        .scope_boxed()
    })
    .await
    .map_err(warp::reject::custom)
}

/// Get and create all tags for the supplied tag names, returning a tuple of all existing and all created tags.
pub async fn get_or_create_tags(
    connection: &mut AsyncPgConnection,
    tags: &[String],
) -> Result<(Vec<Tag>, Vec<Tag>), TransactionRuntimeError> {
    let lower_tags = tags.iter().map(|s| s.to_lowercase()).collect::<Vec<_>>();
    let existing_tags = tag::table
        .filter(lower(tag::tag_name).eq_any(&lower_tags))
        .load::<Tag>(connection)
        .await?;

    let mut existing_tag_map = HashMap::new();
    for existing_tag in existing_tags {
        existing_tag_map.insert(existing_tag.tag_name.to_lowercase(), existing_tag);
    }

    let mut new_tags = Vec::new();
    let mut set_tags = Vec::new();
    for tag in tags.iter() {
        match existing_tag_map.remove(&tag.to_lowercase()) {
            Some(existing_tag) => set_tags.push(existing_tag),
            None => new_tags.push(NewTag {
                tag_name: tag.clone(),
            }),
        }
    }

    let created_tags = diesel::insert_into(tag::table)
        .values(&new_tags)
        .get_results::<Tag>(connection)
        .await
        .map_err(retry_on_constraint_violation)?;

    Ok((set_tags, created_tags))
}

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

#[derive(Deserialize, Validate)]
pub struct UpsertTagRequest {
    #[validate(custom(function = "validate_tag"))]
    pub tag_name: String,
    #[validate(length(min = 0, max = 25))]
    pub parent_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 25))]
    pub alias_pks: Option<Vec<i64>>,
}

#[derive(Serialize)]
pub struct UpsertTagResponse {
    pub inserted: bool,
    pub tag_pk: i64,
}

/// Creates a tag with the given tag_name, parent and aliases. If the tag already exists, the existing tag is updated
/// and the given parents and aliases are added. Note that added aliases are removed from their pre-existing parent-child
/// hierarchy and added to the hierarchy of the given tag instead, setting all parents of the tag as the parents of all aliases
/// and setting all children of the tag as children of all aliases.
pub async fn upsert_tag_handler(
    mut upsert_tag_request: UpsertTagRequest,
    _user: User,
) -> Result<impl Reply, Rejection> {
    upsert_tag_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for UpsertTagRequest: {}",
            e
        )))
    })?;

    let tag_name = sanitize_tag(&upsert_tag_request.tag_name);

    if let Some(ref mut parent_pks) = upsert_tag_request.parent_pks {
        parent_pks.sort_unstable();
        parent_pks.dedup();
    }

    if let Some(ref mut alias_pks) = upsert_tag_request.alias_pks {
        alias_pks.sort_unstable();
        alias_pks.dedup();
    }

    let mut connection = acquire_db_connection().await?;

    if let Some(ref parent_pks) = upsert_tag_request.parent_pks {
        report_missing_pks!(tag, parent_pks, &mut connection)??;
    }

    if let Some(ref alias_pks) = upsert_tag_request.alias_pks {
        report_missing_pks!(tag, alias_pks, &mut connection)??;
    }

    run_serializable_transaction(&mut connection, |connection| {
        async move {
            let (inserted, tag) = get_or_create_tag(&tag_name, connection).await?;

            if let Some(ref parent_pks) = upsert_tag_request.parent_pks {
                if parent_pks.iter().any(|parent_pk| *parent_pk == tag.pk) {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::InvalidRequestInputError(format!(
                            "Cannot set tag {} as its own parent",
                            tag.pk
                        )),
                    ));
                }

                let curr_parents = get_tag_parents_pks(tag.pk, connection).await?;
                let mut parents_to_set = parent_pks
                    .iter()
                    .cloned()
                    .filter(|parent_pk| !curr_parents.contains(parent_pk))
                    .collect::<Vec<_>>();

                parents_to_set.sort_unstable();
                parents_to_set.dedup();

                if !parents_to_set.is_empty() {
                    let curr_parents = get_tag_parents_pks(tag.pk, connection).await?;
                    if curr_parents.len() + parents_to_set.len() > 25 {
                        return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                            String::from("Cannot set more than 25 parents"),
                        )));
                    }
                    add_tag_parents(tag.pk, &parents_to_set, connection).await?;
                }
            }

            if let Some(ref alias_pks) = upsert_tag_request.alias_pks {
                if !alias_pks.is_empty() {
                    if alias_pks.iter().any(|alias_pk| *alias_pk == tag.pk) {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::InvalidRequestInputError(format!(
                                "Cannot set tag {} as an alias of itself",
                                tag.pk
                            )),
                        ));
                    }

                    let curr_aliases = get_tag_aliases_pks(tag.pk, connection).await?;
                    let aliases_to_set = alias_pks
                        .iter()
                        .cloned()
                        .filter(|alias_pk| !curr_aliases.contains(alias_pk))
                        .collect::<Vec<_>>();

                    if !aliases_to_set.is_empty() {
                        if curr_aliases.len() + aliases_to_set.len() > 25 {
                            return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                                String::from("Cannot set more than 25 aliases"),
                            )));
                        }

                        add_tag_aliases(&tag, &aliases_to_set, connection).await?;
                    }
                }
            }

            Ok(warp::reply::json(&UpsertTagResponse {
                inserted,
                tag_pk: tag.pk,
            }))
        }
        .scope_boxed()
    })
    .await
    .map_err(warp::reject::custom)
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

#[derive(Serialize)]
pub struct TagJoined {
    pub tag: Tag,
    pub parents: Vec<Tag>,
    pub aliases: Vec<Tag>,
}

pub async fn get_tag_handler(tag_pk: i64) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let tag = tag::table
        .find(tag_pk)
        .get_result::<Tag>(&mut connection)
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

    Ok(warp::reply::json(&TagJoined {
        tag,
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

#[derive(Deserialize, Validate)]
pub struct UpdateTagRequest {
    #[validate(length(min = 0, max = 25))]
    pub added_parent_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 25))]
    pub parent_pks_overwrite: Option<Vec<i64>>,
    pub removed_parent_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 25))]
    pub added_alias_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 25))]
    pub alias_pks_overwrite: Option<Vec<i64>>,
    pub removed_alias_pks: Option<Vec<i64>>,
}

pub async fn update_tag_handler(
    tag_pk: i64,
    mut request: UpdateTagRequest,
    _user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for UpdateTagRequest: {}",
            e
        )))
    })?;

    dedup_vec_optional(&mut request.added_parent_pks);
    dedup_vec_optional(&mut request.parent_pks_overwrite);
    dedup_vec_optional(&mut request.removed_parent_pks);
    dedup_vec_optional(&mut request.added_alias_pks);
    dedup_vec_optional(&mut request.alias_pks_overwrite);
    dedup_vec_optional(&mut request.removed_alias_pks);
    dedup_vecs_optional(&mut request.added_parent_pks, &request.parent_pks_overwrite);
    dedup_vecs_optional(&mut request.added_alias_pks, &request.alias_pks_overwrite);

    let mut connection = acquire_db_connection().await?;
    let tag = run_serializable_transaction(&mut connection, |connection| {
        async move {
            let tag = tag::table
                .find(tag_pk)
                .get_result::<Tag>(connection)
                .await
                .optional()?
                .ok_or(TransactionRuntimeError::Rollback(Error::NotFoundError))?;

            if let Some(ref parent_pks_overwrite) = request.parent_pks_overwrite {
                diesel::delete(tag_edge::table)
                    .filter(tag_edge::fk_child.eq(tag.pk))
                    .execute(connection)
                    .await?;

                match request.added_parent_pks {
                    Some(ref mut added_parent_pks) => {
                        added_parent_pks.append(&mut parent_pks_overwrite.clone())
                    }
                    None => request.added_parent_pks = Some(parent_pks_overwrite.clone()),
                }
            }

            if let Some(ref added_parent_pks) = request.added_parent_pks {
                if !added_parent_pks.is_empty() {
                    if added_parent_pks
                        .iter()
                        .any(|parent_pk| *parent_pk == tag.pk)
                    {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::InvalidRequestInputError(format!(
                                "Cannot set tag {} as its own parent",
                                tag.pk
                            )),
                        ));
                    }
                    report_missing_pks!(tag, added_parent_pks, connection)??;
                    add_tag_parents(tag.pk, added_parent_pks, connection).await?;
                }
            }

            if let Some(ref removed_parent_pks) = request.removed_parent_pks {
                if !removed_parent_pks.is_empty() {
                    diesel::delete(tag_edge::table)
                        .filter(
                            tag_edge::fk_child
                                .eq(tag.pk)
                                .and(tag_edge::fk_parent.eq_any(removed_parent_pks)),
                        )
                        .execute(connection)
                        .await?;
                }
            }

            if request
                .added_parent_pks
                .as_ref()
                .map(|v| !v.is_empty())
                .unwrap_or(false)
            {
                let curr_parents = get_tag_parents_pks(tag.pk, connection).await?;
                if curr_parents.len() > 25 {
                    return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                        String::from("Cannot set more than 25 parents"),
                    )));
                }
            }

            if let Some(ref alias_pks_overwrite) = request.alias_pks_overwrite {
                diesel::delete(tag_alias::table)
                    .filter(
                        tag_alias::fk_source
                            .eq(tag.pk)
                            .or(tag_alias::fk_target.eq(tag.pk)),
                    )
                    .execute(connection)
                    .await?;

                match request.added_alias_pks {
                    Some(ref mut added_alias_pks) => {
                        added_alias_pks.append(&mut alias_pks_overwrite.clone())
                    }
                    None => request.added_alias_pks = Some(alias_pks_overwrite.clone()),
                }
            }

            if let Some(ref added_alias_pks) = request.added_alias_pks {
                if !added_alias_pks.is_empty() {
                    if added_alias_pks.iter().any(|alias_pk| *alias_pk == tag.pk) {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::InvalidRequestInputError(format!(
                                "Cannot set tag {} as an alias of itself",
                                tag.pk
                            )),
                        ));
                    }
                    report_missing_pks!(tag, added_alias_pks, connection)??;
                    add_tag_aliases(&tag, added_alias_pks, connection).await?;
                    // remove aliases inherited from one of the added aliases that was not included in the overwrite request
                    if request.alias_pks_overwrite.is_some() {
                        let inserted_aliases = get_tag_aliases(tag.pk, connection).await?;
                        let mut inherited_aliases = inserted_aliases
                            .iter()
                            .map(|t| t.pk)
                            .filter(|alias_pk| !added_alias_pks.contains(alias_pk))
                            .to_owned()
                            .collect::<Vec<_>>();
                        match request.removed_alias_pks {
                            Some(ref mut removed_alias_pks) => {
                                removed_alias_pks.append(&mut inherited_aliases)
                            }
                            None => request.removed_alias_pks = Some(inherited_aliases),
                        }
                    }
                }
            }

            if let Some(ref removed_alias_pks) = request.removed_alias_pks {
                if !removed_alias_pks.is_empty() {
                    // a tag cannot be the alias of another tag without also being an alias of all of that tag's other aliases
                    // thus, removing an alias means that tag loses all its aliases
                    diesel::delete(tag_alias::table)
                        .filter(
                            tag_alias::fk_target
                                .eq_any(removed_alias_pks)
                                .or(tag_alias::fk_source.eq_any(removed_alias_pks)),
                        )
                        .execute(connection)
                        .await?;
                }
            }

            if request
                .added_alias_pks
                .as_ref()
                .map(|v| !v.is_empty())
                .unwrap_or(false)
            {
                let curr_aliases = get_tag_aliases_pks(tag.pk, connection).await?;
                if curr_aliases.len() > 25 {
                    return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                        String::from("Cannot set more than 25 aliases"),
                    )));
                }
            }

            Ok(tag)
        }
        .scope_boxed()
    })
    .await?;

    let aliases = get_tag_aliases(tag.pk, &mut connection)
        .await
        .map_err(Error::from)?;
    let parents = get_tag_parents(tag.pk, &mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&TagJoined {
        tag,
        parents,
        aliases,
    }))
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

pub async fn add_tag_aliases(
    tag: &Tag,
    alias_pks: &[i64],
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    let aliases_to_insert = alias_pks
        .iter()
        .map(|alias_pk| TagAlias {
            fk_source: tag.pk,
            fk_target: *alias_pk,
        })
        .collect::<Vec<_>>();

    diesel::insert_into(tag_alias::table)
        .values(aliases_to_insert)
        .on_conflict_do_nothing()
        .execute(connection)
        .await
        .map_err(retry_on_constraint_violation)?;

    Ok(())
}

pub async fn add_tag_parents(
    tag_pk: i64,
    parent_pks: &[i64],
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    let edges_to_insert = parent_pks
        .iter()
        .map(|parent_pk| TagEdge {
            fk_parent: *parent_pk,
            fk_child: tag_pk,
        })
        .collect::<Vec<_>>();

    diesel::insert_into(tag_edge::table)
        .values(edges_to_insert)
        .on_conflict_do_nothing()
        .execute(connection)
        .await
        .map_err(retry_on_constraint_violation)?;

    Ok(())
}

/// merge tags selected by name and tags selected by pk into a vector of loaded Tag structs,
/// saving all new tags for non-existing tag names
pub async fn handle_entered_and_selected_tags(
    selected_tags: &Option<Vec<i64>>,
    entered_tags: Option<Vec<String>>,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, TransactionRuntimeError> {
    // cannot use hashset because it is not supported as diesel expression
    let mut tags = entered_tags.as_deref().map(sanitize_request_tags);
    if let Some(ref selected_tags) = selected_tags {
        report_missing_pks!(tag, selected_tags, connection)??;
    }

    let mut set_tags = if let Some(ref mut tags) = tags {
        util::dedup_vec(tags);
        let (mut set_tags, created_tags) = get_or_create_tags(connection, tags).await?;
        set_tags.extend(created_tags);
        set_tags
    } else {
        Vec::new()
    };

    if let Some(ref selected_tags) = selected_tags {
        let loaded_selected_tags =
            load_and_report_missing_pks!(Tag, tag, selected_tags, connection)?;
        set_tags.extend(loaded_selected_tags);
    }

    if !set_tags.is_empty() {
        filter_redundant_tags(&mut set_tags, connection).await?;
        if set_tags.len() > 100 {
            return Err(TransactionRuntimeError::Rollback(
                Error::InvalidRequestInputError(format!(
                    "Cannot supply more than 100 tags, supplied: {}",
                    set_tags.len()
                )),
            ));
        }
    }

    Ok(set_tags)
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
