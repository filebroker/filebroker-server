use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    ApplyAutoTagsTask, NewApplyAutoTagsTask, PostCollectionTag, PostTag, Tag, TagCategory,
    get_system_user,
};
use crate::post::update::{
    EditPostCollectionRequest, EditPostRequest, update_post, update_post_collection,
};
use crate::query::compiler::dict::Scope;
use crate::query::compiler::{Junction, compile_conditions};
use crate::query::functions::evaluate_tag_auto_match_condition;
use crate::query::{QueryParametersFilter, prepare_query_parameters};
use crate::schema::{apply_auto_tags_task, post_collection_tag, post_tag, tag, tag_category};
use crate::task::LockedObjectsTaskSentinel;
use crate::util::NOT_BLANK_REGEX;
use crate::{acquire_db_connection, run_serializable_transaction};
use diesel::dsl::not;
use diesel::sql_types::BigInt;
use diesel::{BelongingToDsl, BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use exec_rs::mutex::MutexAsync;
use itertools::Itertools;
use lazy_static::lazy_static;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tokio::sync::Semaphore;

const APPLY_AUTO_TAGS_BATCH_SIZE: i64 = 1000;

pub enum AutoMatchTarget {
    Post,
    Collection,
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post)]
pub struct PostMatchQueryObject {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_pk")]
    pub pk: i64,
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post_collection)]
pub struct PostCollectionMatchQueryObject {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_pk")]
    pub pk: i64,
}

pub async fn create_apply_auto_tag_task(
    tag_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<ApplyAutoTagsTask, Error> {
    diesel::insert_into(apply_auto_tags_task::table)
        .values(NewApplyAutoTagsTask {
            tag_to_apply: Some(tag_pk),
            tag_category_to_apply: None,
            post_to_apply: None,
            post_collection_to_apply: None,
        })
        .get_result::<ApplyAutoTagsTask>(connection)
        .await
        .map_err(Error::from)
}

pub async fn create_apply_tag_category_auto_tags_task(
    tag_category_id: String,
    connection: &mut AsyncPgConnection,
) -> Result<ApplyAutoTagsTask, Error> {
    diesel::insert_into(apply_auto_tags_task::table)
        .values(NewApplyAutoTagsTask {
            tag_to_apply: None,
            tag_category_to_apply: Some(tag_category_id),
            post_to_apply: None,
            post_collection_to_apply: None,
        })
        .get_result::<ApplyAutoTagsTask>(connection)
        .await
        .map_err(Error::from)
}

pub async fn create_apply_auto_tags_for_post_task(
    post_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<ApplyAutoTagsTask, Error> {
    diesel::insert_into(apply_auto_tags_task::table)
        .values(NewApplyAutoTagsTask {
            tag_to_apply: None,
            tag_category_to_apply: None,
            post_to_apply: Some(post_pk),
            post_collection_to_apply: None,
        })
        .get_result::<ApplyAutoTagsTask>(connection)
        .await
        .map_err(Error::from)
}

pub async fn create_apply_auto_tags_for_collection_task(
    post_collection_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<ApplyAutoTagsTask, Error> {
    diesel::insert_into(apply_auto_tags_task::table)
        .values(NewApplyAutoTagsTask {
            tag_to_apply: None,
            tag_category_to_apply: None,
            post_to_apply: None,
            post_collection_to_apply: Some(post_collection_pk),
        })
        .get_result::<ApplyAutoTagsTask>(connection)
        .await
        .map_err(Error::from)
}

lazy_static! {
    pub static ref APPLY_AUTO_TAGS_SEMAPHORE: Semaphore = Semaphore::new(4);
    pub static ref APPLY_AUTO_TAGS_SYNC: MutexAsync<ApplyAutoTagsTarget> = MutexAsync::new();
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ApplyAutoTagsTarget {
    Tag(i64),
    TagCategory(String),
    Post(i64),
    PostCollection(i64),
    Task(i64),
}

impl From<&ApplyAutoTagsTask> for ApplyAutoTagsTarget {
    fn from(task: &ApplyAutoTagsTask) -> Self {
        if let Some(tag) = task.tag_to_apply {
            Self::Tag(tag)
        } else if let Some(ref tag_category) = task.tag_category_to_apply {
            Self::TagCategory(tag_category.clone())
        } else if let Some(post) = task.post_to_apply {
            Self::Post(post)
        } else if let Some(post_collection) = task.post_collection_to_apply {
            Self::PostCollection(post_collection)
        } else {
            Self::Task(task.pk)
        }
    }
}

pub fn spawn_apply_auto_tags_task(task: ApplyAutoTagsTask) {
    tokio::spawn(async move {
        let target = ApplyAutoTagsTarget::from(&task);
        APPLY_AUTO_TAGS_SYNC.evaluate(target, || async move {
            let _semaphore = match APPLY_AUTO_TAGS_SEMAPHORE.acquire().await {
                Ok(semaphore) => semaphore,
                Err(e) => {
                    log::error!("Failed to acquire semaphore for apply_auto_tags_task: {e}");
                    return;
                }
            };

            let connection = acquire_db_connection().await;
            match connection {
                Ok(mut connection) => {
                    let sentinel = LockedObjectsTaskSentinel::acquire_with_values(
                        "apply_auto_tags_task",
                        "pk",
                        "locked_at",
                        "true",
                        task.pk,
                    ).await;
                    let _sentinel = match sentinel {
                        Ok(Some(sentinel)) => sentinel,
                        Ok(None) => {
                            log::info!(
                                "Aborting task apply_auto_tags_task because task {task:?} has already been locked"
                            );
                            return;
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to acquire LockedObjectsTaskSentinel for apply_auto_tags_task {}: {e}",
                                task.pk
                            );
                            return;
                        }
                    };

                    let res = run_apply_auto_tags_task(&task, &mut connection).await;

                    if let Err(e) = res {
                        log::error!("Failed to apply auto tags for task {task:?}: {e}");
                        let res = diesel::update(apply_auto_tags_task::table)
                            .filter(apply_auto_tags_task::pk.eq(task.pk))
                            .set(
                                apply_auto_tags_task::fail_count
                                    .eq(apply_auto_tags_task::fail_count + 1),
                            )
                            .execute(&mut connection)
                            .await;
                        if let Err(e) = res {
                            log::error!(
                                "Failed to increment fail_count for apply_auto_tags_task {}: {e}",
                                task.pk
                            );
                        }
                    } else {
                        let delete_task_res = diesel::delete(apply_auto_tags_task::table)
                            .filter(apply_auto_tags_task::pk.eq(task.pk))
                            .execute(&mut connection)
                            .await;
                        if let Err(e) = delete_task_res {
                            log::error!("Failed to delete apply_auto_tags_task {}: {e}", task.pk);
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to acquire database connection: {e}");
                }
            }
        }).await;
    });
}

pub async fn run_apply_auto_tags_task(
    task: &ApplyAutoTagsTask,
    connection: &mut AsyncPgConnection,
) -> Result<(), Error> {
    if let Some(tag_pk) = task.tag_to_apply {
        apply_auto_tag(tag_pk, connection).await?;
    }

    if let Some(ref tag_category_to_apply) = task.tag_category_to_apply {
        apply_tag_category_auto_tags(tag_category_to_apply, connection).await?;
    }

    if let Some(post_to_apply) = task.post_to_apply {
        run_serializable_transaction(connection, async |connection| {
            apply_auto_tags_for_post(post_to_apply, connection).await
        })
        .await?;
    }

    if let Some(post_collection_to_apply) = task.post_collection_to_apply {
        run_serializable_transaction(connection, async |connection| {
            apply_auto_tags_for_collection(post_collection_to_apply, connection).await
        })
        .await?;
    }

    Ok(())
}

pub async fn apply_auto_tags_for_post(
    post_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    log::debug!("Applying auto tags for post {post_pk}");
    let instant = std::time::Instant::now();
    let auto_tags = find_auto_tags_for_post(post_pk, connection).await?;
    let auto_tag_pks = auto_tags.iter().map(|t| t.pk).collect::<Vec<_>>();
    let unmatched_tag_pks = post_tag::table
        .select(post_tag::fk_tag)
        .filter(
            post_tag::fk_post
                .eq(post_pk)
                .and(post_tag::auto_matched)
                .and(not(post_tag::fk_tag.eq_any(&auto_tag_pks))),
        )
        .load::<i64>(connection)
        .await?;

    if auto_tags.is_empty() && unmatched_tag_pks.is_empty() {
        log::debug!("No auto tags found to apply or remove for post {post_pk}");
        return Ok(());
    }

    let auto_tag_pks = auto_tags.iter().map(|t| t.pk).collect::<Vec<_>>();
    let request = EditPostRequest {
        tags_overwrite: None,
        tag_pks_overwrite: None,
        removed_tag_pks: Some(unmatched_tag_pks),
        added_tag_pks: Some(auto_tag_pks),
        added_tags: None,
        data_url: None,
        source_url: None,
        title: None,
        is_public: None,
        public_edit: None,
        description: None,
        group_access_overwrite: None,
        added_group_access: None,
        removed_group_access: None,
    };

    update_post(post_pk, &get_system_user(), request, connection).await?;
    log::info!(
        "Applied {} auto tags for post {post_pk} after {}ms",
        auto_tags.len(),
        instant.elapsed().as_millis()
    );

    Ok(())
}

pub async fn find_auto_tags_for_post(
    post_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, Error> {
    let tags = tag::table
        .filter(evaluate_tag_auto_match_condition(
            tag::compiled_auto_match_condition_post,
            format!("post.pk = {post_pk}"),
        ))
        .load::<Tag>(connection)
        .await
        .map_err(Error::from)?;

    Ok(tags)
}

pub async fn apply_auto_tags_for_collection(
    post_collection_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    log::debug!("Applying auto tags for collection {post_collection_pk}");
    let instant = std::time::Instant::now();
    let auto_tags = find_auto_tags_for_collection(post_collection_pk, connection).await?;
    let auto_tag_pks = auto_tags.iter().map(|t| t.pk).collect::<Vec<_>>();
    let unmatched_tag_pks = post_collection_tag::table
        .select(post_collection_tag::fk_tag)
        .filter(
            post_collection_tag::fk_post_collection
                .eq(post_collection_pk)
                .and(post_collection_tag::auto_matched)
                .and(not(post_collection_tag::fk_tag.eq_any(&auto_tag_pks))),
        )
        .load::<i64>(connection)
        .await?;

    if auto_tags.is_empty() && unmatched_tag_pks.is_empty() {
        log::debug!("No auto tags found to apply or remove for collection {post_collection_pk}");
        return Ok(());
    }

    let auto_tag_pks = auto_tags.iter().map(|t| t.pk).collect::<Vec<_>>();
    let request = EditPostCollectionRequest {
        tags_overwrite: None,
        tag_pks_overwrite: None,
        removed_tag_pks: Some(unmatched_tag_pks),
        added_tag_pks: Some(auto_tag_pks),
        added_tags: None,
        title: None,
        is_public: None,
        public_edit: None,
        description: None,
        group_access_overwrite: None,
        added_group_access: None,
        removed_group_access: None,
        poster_object_key: None,
        post_pks_overwrite: None,
        post_query_overwrite: None,
        added_post_pks: None,
        added_post_query: None,
        removed_item_pks: None,
        duplicate_mode: None,
    };

    update_post_collection(post_collection_pk, &get_system_user(), request, connection).await?;
    log::info!(
        "Applied {} auto tags for collection {post_collection_pk} after {}ms",
        auto_tags.len(),
        instant.elapsed().as_millis()
    );

    Ok(())
}

pub async fn find_auto_tags_for_collection(
    post_collection_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, Error> {
    let tags = tag::table
        .filter(evaluate_tag_auto_match_condition(
            tag::compiled_auto_match_condition_collection,
            format!("post_collection.pk = {post_collection_pk}"),
        ))
        .load::<Tag>(connection)
        .await
        .map_err(Error::from)?;

    Ok(tags)
}

pub async fn apply_auto_tag(tag_pk: i64, connection: &mut AsyncPgConnection) -> Result<(), Error> {
    let instant = std::time::Instant::now();

    // Candidate discovery deliberately happens outside SERIALIZABLE to avoid locking the entire table
    // for large updates. Batch it instead and recheck batches in SERIALIZABLE transaction.
    let tag = tag::table
        .filter(tag::pk.eq(tag_pk))
        .get_result::<Tag>(connection)
        .await?;

    log::debug!("Applying auto tag {}", tag.tag_name);

    let mut post_candidates = Vec::new();
    let mut post_collection_candidates = Vec::new();

    if let Some(ref compiled_auto_match_condition_post) = tag.compiled_auto_match_condition_post {
        let sql_query =
            compiled_auto_match_condition_post.replace("__filter_condition_placeholder__", "TRUE");

        let posts = diesel::sql_query(sql_query)
            .load::<PostMatchQueryObject>(connection)
            .await?;

        post_candidates.extend(posts.into_iter().map(|post| post.pk));
    }

    if let Some(ref compiled_auto_match_condition_collection) =
        tag.compiled_auto_match_condition_collection
    {
        let sql_query = compiled_auto_match_condition_collection
            .replace("__filter_condition_placeholder__", "TRUE");

        let post_collections = diesel::sql_query(sql_query)
            .load::<PostCollectionMatchQueryObject>(connection)
            .await?;

        post_collection_candidates.extend(
            post_collections
                .into_iter()
                .map(|post_collection| post_collection.pk),
        );
    }

    // Include current auto matches to check if they need to be removed
    post_candidates.extend(
        post_tag::table
            .select(post_tag::fk_post)
            .filter(post_tag::fk_tag.eq(tag_pk).and(post_tag::auto_matched))
            .load::<i64>(connection)
            .await?,
    );

    post_collection_candidates.extend(
        post_collection_tag::table
            .select(post_collection_tag::fk_post_collection)
            .filter(
                post_collection_tag::fk_tag
                    .eq(tag_pk)
                    .and(post_collection_tag::auto_matched),
            )
            .load::<i64>(connection)
            .await?,
    );

    post_candidates.sort_unstable();
    post_candidates.dedup();

    post_collection_candidates.sort_unstable();
    post_collection_candidates.dedup();

    log::debug!(
        "Found {} post and {} collection candidates for auto tag {}",
        post_candidates.len(),
        post_collection_candidates.len(),
        tag.tag_name,
    );

    let mut batch_count = 0_usize;
    let mut updated_posts = 0_usize;
    let mut updated_collections = 0_usize;

    for batch in post_candidates.chunks(APPLY_AUTO_TAGS_BATCH_SIZE as usize) {
        let batch = batch.to_vec();

        let (batch_updated_posts, _) =
            run_serializable_transaction(connection, async |connection| {
                apply_auto_tag_batch(tag_pk, &batch, &[], connection).await
            })
            .await?;

        updated_posts += batch_updated_posts;
        batch_count += 1;
    }

    for batch in post_collection_candidates.chunks(APPLY_AUTO_TAGS_BATCH_SIZE as usize) {
        let batch = batch.to_vec();

        let (_, batch_updated_collections) =
            run_serializable_transaction(connection, async |connection| {
                apply_auto_tag_batch(tag_pk, &[], &batch, connection).await
            })
            .await?;

        updated_collections += batch_updated_collections;
        batch_count += 1;
    }

    log::info!(
        "Applied auto tag {} in {batch_count} batches, updating {updated_posts} posts and {updated_collections} collections after {}ms",
        tag.tag_name,
        instant.elapsed().as_millis()
    );

    Ok(())
}

async fn apply_auto_tag_batch(
    tag_pk: i64,
    post_pks: &[i64],
    post_collection_pks: &[i64],
    connection: &mut AsyncPgConnection,
) -> Result<(usize, usize), TransactionRuntimeError> {
    // reload tag to make sure the serializable batch transaction has the up-to-date conditions
    let tag = tag::table
        .filter(tag::pk.eq(tag_pk))
        .get_result::<Tag>(connection)
        .await?;

    let mut updated_posts = 0_usize;
    let mut updated_collections = 0_usize;

    if !post_pks.is_empty() {
        // recheck candidate set against up-to-date condition
        let matched_posts = if let Some(ref compiled_auto_match_condition_post) =
            tag.compiled_auto_match_condition_post
        {
            let filter_condition = format!("post.pk IN ({})", post_pks.iter().join(","));

            let sql_query = compiled_auto_match_condition_post
                .replace("__filter_condition_placeholder__", &filter_condition);

            diesel::sql_query(sql_query)
                .load::<PostMatchQueryObject>(connection)
                .await?
                .into_iter()
                .map(|post| post.pk)
                .collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };

        let existing_assignments = post_tag::table
            .select((post_tag::fk_post, post_tag::auto_matched))
            .filter(
                post_tag::fk_tag
                    .eq(tag.pk)
                    .and(post_tag::fk_post.eq_any(post_pks)),
            )
            .load::<(i64, bool)>(connection)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();

        for post_pk in post_pks {
            let matches = matched_posts.contains(post_pk);
            let existing_assignment = existing_assignments.get(post_pk).copied();

            let request = match (matches, existing_assignment) {
                // Currently matches, but the exact tag isn't assigned.
                (true, None) => Some(get_add_post_tags_request(vec![tag.pk])),

                // No longer matches and the exact relation was automatically
                // created, so remove it.
                (false, Some(true)) => Some(get_remove_post_tags_request(vec![tag.pk])),

                // Exact manual/automatic assignment already satisfies a
                // current match, or a manual assignment must be preserved.
                _ => None,
            };

            let Some(request) = request else {
                continue;
            };

            match update_post(*post_pk, &get_system_user(), request, connection).await {
                Ok((_, updated, _)) => {
                    if updated {
                        updated_posts += 1;
                    }
                }
                Err(e) => {
                    log::error!(
                        "Failed to reconcile auto tag {} for post {}: {e}",
                        tag.tag_name,
                        post_pk
                    );
                    return Err(e);
                }
            }
        }
    }

    if !post_collection_pks.is_empty() {
        let matched_collections = if let Some(ref compiled_auto_match_condition_collection) =
            tag.compiled_auto_match_condition_collection
        {
            let filter_condition = format!(
                "post_collection.pk IN ({})",
                post_collection_pks.iter().join(",")
            );

            let sql_query = compiled_auto_match_condition_collection
                .replace("__filter_condition_placeholder__", &filter_condition);

            diesel::sql_query(sql_query)
                .load::<PostCollectionMatchQueryObject>(connection)
                .await?
                .into_iter()
                .map(|post_collection| post_collection.pk)
                .collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };

        let existing_assignments = post_collection_tag::table
            .select((
                post_collection_tag::fk_post_collection,
                post_collection_tag::auto_matched,
            ))
            .filter(
                post_collection_tag::fk_tag
                    .eq(tag.pk)
                    .and(post_collection_tag::fk_post_collection.eq_any(post_collection_pks)),
            )
            .load::<(i64, bool)>(connection)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();

        for post_collection_pk in post_collection_pks {
            let matches = matched_collections.contains(post_collection_pk);
            let existing_assignment = existing_assignments.get(post_collection_pk).copied();

            let request = match (matches, existing_assignment) {
                (true, None) => Some(get_add_post_collection_tags_request(vec![tag.pk])),
                (false, Some(true)) => Some(get_remove_post_collection_tags_request(vec![tag.pk])),
                _ => None,
            };

            let Some(request) = request else {
                continue;
            };

            match update_post_collection(
                *post_collection_pk,
                &get_system_user(),
                request,
                connection,
            )
            .await
            {
                Ok((_, updated, _)) => {
                    if updated {
                        updated_collections += 1;
                    }
                }
                Err(e) => {
                    log::error!(
                        "Failed to reconcile auto tag {} for collection {}: {e}",
                        tag.tag_name,
                        post_collection_pk
                    );
                    return Err(e);
                }
            }
        }
    }

    log::debug!(
        "Auto tag {} batch: checked {} posts and {} collections, updated {} posts and {} collections",
        tag.tag_name,
        post_pks.len(),
        post_collection_pks.len(),
        updated_posts,
        updated_collections,
    );

    Ok((updated_posts, updated_collections))
}

struct AutoTagMatches {
    existing_matches: Vec<i64>,
    existing_auto_matches: Vec<i64>,
    new_matches: Vec<i64>,
}

impl AutoTagMatches {
    fn new() -> Self {
        Self {
            existing_matches: Vec::new(),
            existing_auto_matches: Vec::new(),
            new_matches: Vec::new(),
        }
    }

    fn into_changes(self) -> Option<(Vec<i64>, Vec<i64>)> {
        let added_tag_pks = self
            .new_matches
            .iter()
            .filter(|tag_pk| !self.existing_matches.contains(tag_pk))
            .copied()
            .collect::<Vec<_>>();

        let removed_tag_pks = self
            .existing_auto_matches
            .iter()
            .filter(|tag_pk| !self.new_matches.contains(tag_pk))
            .copied()
            .collect::<Vec<_>>();

        if added_tag_pks.is_empty() && removed_tag_pks.is_empty() {
            None
        } else {
            Some((added_tag_pks, removed_tag_pks))
        }
    }
}

pub async fn apply_tag_category_auto_tags(
    tag_category_id: &str,
    connection: &mut AsyncPgConnection,
) -> Result<(), Error> {
    log::debug!("Applying auto tags in category {tag_category_id}");
    let instant = std::time::Instant::now();

    let tag_category = tag_category::table
        .filter(tag_category::id.eq(tag_category_id))
        .get_result::<TagCategory>(connection)
        .await?;

    let tags = Tag::belonging_to(&tag_category)
        .load::<Tag>(connection)
        .await?;

    // Snapshot which tags belong to this category for this task.
    //
    // Tags added to the category later get their own tasks and must not suddenly expand the
    // scope of this already-running category task.
    let tag_pks = tags.iter().map(|tag| tag.pk).collect::<Vec<_>>();

    let mut post_tag_map: HashMap<i64, AutoTagMatches> = HashMap::new();
    let mut post_collection_tag_map: HashMap<i64, AutoTagMatches> = HashMap::new();

    // Build an approximate current state outside SERIALIZABLE. This is only
    // used to determine which objects this task needs to revisit. This is done to break down
    // the potentially large candidate set outside the SERIALIZABLE transaction to avoid locking the
    // entire table and for a long duration.
    let existing_post_tags = post_tag::table
        .filter(post_tag::fk_tag.eq_any(&tag_pks))
        .load::<PostTag>(connection)
        .await?;

    for existing_post_tag in existing_post_tags {
        let entry = post_tag_map
            .entry(existing_post_tag.fk_post)
            .or_insert_with(AutoTagMatches::new);

        entry.existing_matches.push(existing_post_tag.fk_tag);

        if existing_post_tag.auto_matched {
            entry.existing_auto_matches.push(existing_post_tag.fk_tag);
        }
    }

    let existing_post_collection_tags = post_collection_tag::table
        .filter(post_collection_tag::fk_tag.eq_any(&tag_pks))
        .load::<PostCollectionTag>(connection)
        .await?;

    for existing_post_collection_tag in existing_post_collection_tags {
        let entry = post_collection_tag_map
            .entry(existing_post_collection_tag.fk_post_collection)
            .or_insert_with(AutoTagMatches::new);

        entry
            .existing_matches
            .push(existing_post_collection_tag.fk_tag);

        if existing_post_collection_tag.auto_matched {
            entry
                .existing_auto_matches
                .push(existing_post_collection_tag.fk_tag);
        }
    }

    for tag in &tags {
        if let Some(ref compiled_auto_match_condition_post) = tag.compiled_auto_match_condition_post
        {
            let sql_query = compiled_auto_match_condition_post
                .replace("__filter_condition_placeholder__", "TRUE");

            let posts = diesel::sql_query(sql_query)
                .load::<PostMatchQueryObject>(connection)
                .await?;

            for post in posts {
                post_tag_map
                    .entry(post.pk)
                    .or_insert_with(AutoTagMatches::new)
                    .new_matches
                    .push(tag.pk);
            }
        }

        if let Some(ref compiled_auto_match_condition_collection) =
            tag.compiled_auto_match_condition_collection
        {
            let sql_query = compiled_auto_match_condition_collection
                .replace("__filter_condition_placeholder__", "TRUE");

            let post_collections = diesel::sql_query(sql_query)
                .load::<PostCollectionMatchQueryObject>(connection)
                .await?;

            for post_collection in post_collections {
                post_collection_tag_map
                    .entry(post_collection.pk)
                    .or_insert_with(AutoTagMatches::new)
                    .new_matches
                    .push(tag.pk);
            }
        }
    }

    let mut post_candidates = post_tag_map
        .into_iter()
        .filter_map(|(post_pk, matches)| matches.into_changes().map(|_| post_pk))
        .collect::<Vec<_>>();

    let mut post_collection_candidates = post_collection_tag_map
        .into_iter()
        .filter_map(|(post_collection_pk, matches)| {
            matches.into_changes().map(|_| post_collection_pk)
        })
        .collect::<Vec<_>>();

    post_candidates.sort_unstable();
    post_collection_candidates.sort_unstable();

    log::debug!(
        "Found {} post and {} collection candidates for auto tag category {}",
        post_candidates.len(),
        post_collection_candidates.len(),
        tag_category.id,
    );

    let mut batch_count = 0_usize;
    let mut updated_posts = 0_usize;
    let mut updated_collections = 0_usize;

    for batch in post_candidates.chunks(APPLY_AUTO_TAGS_BATCH_SIZE as usize) {
        let batch = batch.to_vec();

        let (batch_updated_posts, _) =
            run_serializable_transaction(connection, async |connection| {
                apply_tag_category_auto_tags_batch(
                    tag_category_id,
                    &tag_pks,
                    &batch,
                    &[],
                    connection,
                )
                .await
            })
            .await?;

        updated_posts += batch_updated_posts;
        batch_count += 1;
    }

    for batch in post_collection_candidates.chunks(APPLY_AUTO_TAGS_BATCH_SIZE as usize) {
        let batch = batch.to_vec();

        let (_, batch_updated_collections) =
            run_serializable_transaction(connection, async |connection| {
                apply_tag_category_auto_tags_batch(
                    tag_category_id,
                    &tag_pks,
                    &[],
                    &batch,
                    connection,
                )
                .await
            })
            .await?;

        updated_collections += batch_updated_collections;
        batch_count += 1;
    }

    log::info!(
        "Applied auto tags in category {} in {} batches, updating {} posts and {} collections after {}ms",
        tag_category.id,
        batch_count,
        updated_posts,
        updated_collections,
        instant.elapsed().as_millis(),
    );

    Ok(())
}

async fn apply_tag_category_auto_tags_batch(
    tag_category_id: &str,
    initial_tag_pks: &[i64],
    post_pks: &[i64],
    post_collection_pks: &[i64],
    connection: &mut AsyncPgConnection,
) -> Result<(usize, usize), TransactionRuntimeError> {
    let tag_category = tag_category::table
        .filter(tag_category::id.eq(tag_category_id))
        .get_result::<TagCategory>(connection)
        .await?;

    // Reload relevant tags to make sure the serializable batch has the up-to-date conditions.
    let tags = Tag::belonging_to(&tag_category)
        .filter(tag::pk.eq_any(initial_tag_pks))
        .load::<Tag>(connection)
        .await?;

    let current_tag_pks = tags.iter().map(|tag| tag.pk).collect::<Vec<_>>();

    let mut updated_posts = 0_usize;
    let mut updated_collections = 0_usize;

    if !post_pks.is_empty() {
        let mut post_tag_map: HashMap<i64, AutoTagMatches> = HashMap::new();

        let existing_post_tags = post_tag::table
            .filter(
                post_tag::fk_post
                    .eq_any(post_pks)
                    .and(post_tag::fk_tag.eq_any(&current_tag_pks)),
            )
            .load::<PostTag>(connection)
            .await?;

        for existing_post_tag in existing_post_tags {
            let entry = post_tag_map
                .entry(existing_post_tag.fk_post)
                .or_insert_with(AutoTagMatches::new);

            entry.existing_matches.push(existing_post_tag.fk_tag);

            if existing_post_tag.auto_matched {
                entry.existing_auto_matches.push(existing_post_tag.fk_tag);
            }
        }

        let filter_condition = format!("post.pk IN({})", post_pks.iter().join(","));

        for tag in &tags {
            let Some(ref compiled_auto_match_condition_post) =
                tag.compiled_auto_match_condition_post
            else {
                continue;
            };

            let sql_query = compiled_auto_match_condition_post
                .replace("__filter_condition_placeholder__", &filter_condition);

            let posts = diesel::sql_query(sql_query)
                .load::<PostMatchQueryObject>(connection)
                .await?;

            for post in posts {
                post_tag_map
                    .entry(post.pk)
                    .or_insert_with(AutoTagMatches::new)
                    .new_matches
                    .push(tag.pk);
            }
        }

        for (post_pk, tag_auto_matches) in post_tag_map {
            let Some((added_tag_pks, removed_tag_pks)) = tag_auto_matches.into_changes() else {
                continue;
            };

            let request = EditPostRequest {
                tags_overwrite: None,
                tag_pks_overwrite: None,
                removed_tag_pks: Some(removed_tag_pks),
                added_tag_pks: Some(added_tag_pks),
                added_tags: None,
                data_url: None,
                source_url: None,
                title: None,
                is_public: None,
                public_edit: None,
                description: None,
                group_access_overwrite: None,
                added_group_access: None,
                removed_group_access: None,
            };

            match update_post(post_pk, &get_system_user(), request, connection).await {
                Ok((_, updated, _)) => {
                    if updated {
                        updated_posts += 1;
                    }
                }
                Err(e) => {
                    log::error!(
                        "Failed to reconcile auto tags from category {} for post {}: {e}",
                        tag_category.id,
                        post_pk
                    );
                    return Err(e);
                }
            }
        }
    }

    if !post_collection_pks.is_empty() {
        let mut post_collection_tag_map: HashMap<i64, AutoTagMatches> = HashMap::new();

        let existing_post_collection_tags = post_collection_tag::table
            .filter(
                post_collection_tag::fk_post_collection
                    .eq_any(post_collection_pks)
                    .and(post_collection_tag::fk_tag.eq_any(&current_tag_pks)),
            )
            .load::<PostCollectionTag>(connection)
            .await?;

        for existing_post_collection_tag in existing_post_collection_tags {
            let entry = post_collection_tag_map
                .entry(existing_post_collection_tag.fk_post_collection)
                .or_insert_with(AutoTagMatches::new);

            entry
                .existing_matches
                .push(existing_post_collection_tag.fk_tag);

            if existing_post_collection_tag.auto_matched {
                entry
                    .existing_auto_matches
                    .push(existing_post_collection_tag.fk_tag);
            }
        }

        let filter_condition = format!(
            "post_collection.pk IN({})",
            post_collection_pks.iter().join(",")
        );

        for tag in &tags {
            let Some(ref compiled_auto_match_condition_collection) =
                tag.compiled_auto_match_condition_collection
            else {
                continue;
            };

            let sql_query = compiled_auto_match_condition_collection
                .replace("__filter_condition_placeholder__", &filter_condition);

            let post_collections = diesel::sql_query(sql_query)
                .load::<PostCollectionMatchQueryObject>(connection)
                .await?;

            for post_collection in post_collections {
                post_collection_tag_map
                    .entry(post_collection.pk)
                    .or_insert_with(AutoTagMatches::new)
                    .new_matches
                    .push(tag.pk);
            }
        }

        for (post_collection_pk, tag_auto_matches) in post_collection_tag_map {
            let Some((added_tag_pks, removed_tag_pks)) = tag_auto_matches.into_changes() else {
                continue;
            };

            let request = EditPostCollectionRequest {
                tags_overwrite: None,
                tag_pks_overwrite: None,
                removed_tag_pks: Some(removed_tag_pks),
                added_tag_pks: Some(added_tag_pks),
                added_tags: None,
                title: None,
                is_public: None,
                public_edit: None,
                description: None,
                group_access_overwrite: None,
                added_group_access: None,
                removed_group_access: None,
                poster_object_key: None,
                post_pks_overwrite: None,
                post_query_overwrite: None,
                added_post_pks: None,
                added_post_query: None,
                removed_item_pks: None,
                duplicate_mode: None,
            };

            match update_post_collection(
                post_collection_pk,
                &get_system_user(),
                request,
                connection,
            )
            .await
            {
                Ok((_, updated, _)) => {
                    if updated {
                        updated_collections += 1;
                    }
                }
                Err(e) => {
                    log::error!(
                        "Failed to reconcile auto tags from category {} for collection {}: {e}",
                        tag_category.id,
                        post_collection_pk
                    );
                    return Err(e);
                }
            }
        }
    }

    log::debug!(
        "Auto tag category {} batch: checked {} posts and {} collections, updated {} posts and {} collections",
        tag_category.id,
        post_pks.len(),
        post_collection_pks.len(),
        updated_posts,
        updated_collections,
    );

    Ok((updated_posts, updated_collections))
}

pub fn compile_tag_auto_match_condition(
    tag: Tag,
    tag_category: Option<TagCategory>,
    target: AutoMatchTarget,
) -> Result<Option<String>, Error> {
    let scope = match target {
        AutoMatchTarget::Post => Scope::TagAutoMatchPost,
        AutoMatchTarget::Collection => Scope::TagAutoMatchCollection,
    };

    let (tag_condition, category_condition) = match target {
        AutoMatchTarget::Post => (
            tag.auto_match_condition_post,
            tag_category.and_then(|tc| tc.auto_match_condition_post),
        ),
        AutoMatchTarget::Collection => (
            tag.auto_match_condition_collection,
            tag_category.and_then(|tc| tc.auto_match_condition_collection),
        ),
    };

    compile_auto_match_condition(tag.tag_name, tag_condition, category_condition, scope)
}

pub fn compile_auto_match_condition(
    tag_name: String,
    tag_auto_match_condition: Option<String>,
    tag_category_auto_match_condition: Option<String>,
    scope: Scope,
) -> Result<Option<String>, Error> {
    log::debug!("Compiling auto match condition {scope:?} for tag: {tag_name}");
    let conditions = tag_auto_match_condition
        .into_iter()
        .chain(tag_category_auto_match_condition)
        .filter(|condition| NOT_BLANK_REGEX.is_match(condition))
        .collect::<Vec<_>>();
    if conditions.is_empty() {
        return Ok(None);
    }

    let query_parameters_filter = QueryParametersFilter {
        limit: None,
        page: None,
        query: None,
        exclude_window: None,
        shuffle: None,
        writable_only: None,
        constriction: None,
    };
    let mut query_parameters = prepare_query_parameters(&query_parameters_filter, &None, &scope)?;
    query_parameters.privileged = true;
    // placeholder for filter_condition in evaluate_tag_auto_match_condition
    query_parameters.predefined_where_conditions =
        Some(vec![String::from("__filter_condition_placeholder__")]);
    query_parameters
        .variables
        .insert(String::from("tag_name"), tag_name);

    let sql_query = compile_conditions(conditions, Junction::Or, query_parameters, &scope, &None)?;

    Ok(Some(sql_query))
}

fn get_add_post_tags_request(tag_pks: Vec<i64>) -> EditPostRequest {
    EditPostRequest {
        tags_overwrite: None,
        tag_pks_overwrite: None,
        removed_tag_pks: None,
        added_tag_pks: Some(tag_pks),
        added_tags: None,
        data_url: None,
        source_url: None,
        title: None,
        is_public: None,
        public_edit: None,
        description: None,
        group_access_overwrite: None,
        added_group_access: None,
        removed_group_access: None,
    }
}

fn get_remove_post_tags_request(tag_pks: Vec<i64>) -> EditPostRequest {
    EditPostRequest {
        tags_overwrite: None,
        tag_pks_overwrite: None,
        removed_tag_pks: Some(tag_pks),
        added_tag_pks: None,
        added_tags: None,
        data_url: None,
        source_url: None,
        title: None,
        is_public: None,
        public_edit: None,
        description: None,
        group_access_overwrite: None,
        added_group_access: None,
        removed_group_access: None,
    }
}

fn get_add_post_collection_tags_request(tag_pks: Vec<i64>) -> EditPostCollectionRequest {
    EditPostCollectionRequest {
        tags_overwrite: None,
        tag_pks_overwrite: None,
        removed_tag_pks: None,
        added_tag_pks: Some(tag_pks),
        added_tags: None,
        title: None,
        is_public: None,
        public_edit: None,
        description: None,
        group_access_overwrite: None,
        added_group_access: None,
        removed_group_access: None,
        poster_object_key: None,
        post_pks_overwrite: None,
        post_query_overwrite: None,
        added_post_pks: None,
        added_post_query: None,
        removed_item_pks: None,
        duplicate_mode: None,
    }
}

fn get_remove_post_collection_tags_request(tag_pks: Vec<i64>) -> EditPostCollectionRequest {
    EditPostCollectionRequest {
        tags_overwrite: None,
        tag_pks_overwrite: None,
        removed_tag_pks: Some(tag_pks),
        added_tag_pks: None,
        added_tags: None,
        title: None,
        is_public: None,
        public_edit: None,
        description: None,
        group_access_overwrite: None,
        added_group_access: None,
        removed_group_access: None,
        poster_object_key: None,
        post_pks_overwrite: None,
        post_query_overwrite: None,
        added_post_pks: None,
        added_post_query: None,
        removed_item_pks: None,
        duplicate_mode: None,
    }
}
