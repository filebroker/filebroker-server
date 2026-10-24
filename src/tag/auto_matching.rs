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
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use lazy_static::lazy_static;
use serde::Serialize;
use std::collections::HashMap;
use tokio::sync::Semaphore;

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
}

pub fn spawn_apply_auto_tags_task(task: ApplyAutoTagsTask) {
    tokio::spawn(async move {
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
                )
                .await;
                let _sentinel = match sentinel {
                    Ok(Some(sentinel)) => sentinel,
                    Ok(None) => {
                        log::info!(
                            "Aborting task apply_auto_tags_task because task {:?} has already been locked",
                            &task
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

                let res = run_serializable_transaction(&mut connection, |connection| {
                    async { run_apply_auto_tags_task(&task, connection).await }.scope_boxed()
                })
                .await;
                if let Err(e) = res {
                    log::error!("Failed to apply auto tags for task {:?}: {e}", &task);
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
    });
}

pub async fn run_apply_auto_tags_task(
    task: &ApplyAutoTagsTask,
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    if let Some(tag_pk) = task.tag_to_apply {
        let tag = tag::table
            .filter(tag::pk.eq(tag_pk))
            .get_result::<Tag>(connection)
            .await?;
        apply_auto_tag(&tag, connection).await?;
    }
    if let Some(ref tag_category_to_apply) = task.tag_category_to_apply {
        let tag_category = tag_category::table
            .filter(tag_category::id.eq(tag_category_to_apply))
            .get_result::<TagCategory>(connection)
            .await?;
        apply_tag_category_auto_tags(&tag_category, connection).await?;
    }
    if let Some(post_to_apply) = task.post_to_apply {
        apply_auto_tags_for_post(post_to_apply, connection).await?;
    }
    if let Some(post_collection_to_apply) = task.post_collection_to_apply {
        apply_auto_tags_for_collection(post_collection_to_apply, connection).await?;
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

pub async fn apply_auto_tag(
    tag: &Tag,
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    log::debug!("Applying auto tag {}", &tag.tag_name);
    let instant = std::time::Instant::now();

    let mut matched_posts = Vec::new();
    let mut matched_collections = Vec::new();

    if let Some(ref compiled_auto_match_condition_post) = tag.compiled_auto_match_condition_post {
        let sql_query =
            compiled_auto_match_condition_post.replace("__filter_condition_placeholder__", "TRUE");

        let posts = diesel::sql_query(sql_query)
            .load::<PostMatchQueryObject>(connection)
            .await?;

        matched_posts.extend(posts.into_iter().map(|p| p.pk));
    }
    if let Some(ref compiled_auto_match_condition_collection) =
        tag.compiled_auto_match_condition_collection
    {
        let sql_query = compiled_auto_match_condition_collection
            .replace("__filter_condition_placeholder__", "TRUE");

        let post_collections = diesel::sql_query(sql_query)
            .load::<PostCollectionMatchQueryObject>(connection)
            .await?;

        matched_collections.extend(post_collections.into_iter().map(|p| p.pk));
    }

    let unmatched_posts = post_tag::table
        .select(post_tag::fk_post)
        .filter(
            post_tag::fk_tag
                .eq(tag.pk)
                .and(post_tag::auto_matched)
                .and(not(post_tag::fk_post.eq_any(&matched_posts))),
        )
        .load::<i64>(connection)
        .await?;
    let unmatched_collections = post_collection_tag::table
        .select(post_collection_tag::fk_post_collection)
        .filter(
            post_collection_tag::fk_tag
                .eq(tag.pk)
                .and(post_collection_tag::auto_matched)
                .and(not(
                    post_collection_tag::fk_post_collection.eq_any(&matched_collections)
                )),
        )
        .load::<i64>(connection)
        .await?;

    if !unmatched_posts.is_empty() {
        log::debug!(
            "Found {} posts that no longer match for auto tag {}",
            unmatched_posts.len(),
            &tag.tag_name
        );

        for unmatched_post in unmatched_posts {
            let request = get_remove_post_tags_request(vec![tag.pk]);
            if let Err(e) =
                update_post(unmatched_post, &get_system_user(), request, connection).await
            {
                log::error!(
                    "Failed to remove unmatched tag {} for post {}: {e}",
                    &tag.tag_name,
                    unmatched_post
                );
                return Err(e);
            }
        }
    }
    if !unmatched_collections.is_empty() {
        log::debug!(
            "Found {} collections that no longer match for auto tag {}",
            unmatched_collections.len(),
            &tag.tag_name
        );

        for unmatched_collection in unmatched_collections {
            let request = get_remove_post_collection_tags_request(vec![tag.pk]);
            if let Err(e) = update_post_collection(
                unmatched_collection,
                &get_system_user(),
                request,
                connection,
            )
            .await
            {
                log::error!(
                    "Failed to remove unmatched tag {} for collection {}: {e}",
                    &tag.tag_name,
                    unmatched_collection
                );
                return Err(e);
            }
        }
    }

    log::debug!(
        "Found {} posts for auto tag {}",
        matched_posts.len(),
        &tag.tag_name
    );
    for post_pk in matched_posts {
        let request = get_add_post_tags_request(vec![tag.pk]);

        if let Err(e) = update_post(post_pk, &get_system_user(), request, connection).await {
            log::error!(
                "Failed to apply auto tag {} for post {}: {e}",
                &tag.tag_name,
                post_pk
            );
            return Err(e);
        }
    }
    log::debug!(
        "Found {} post collections for auto tag {}",
        matched_collections.len(),
        &tag.tag_name
    );
    for post_collection_pk in matched_collections {
        let request = get_add_post_collection_tags_request(vec![tag.pk]);

        if let Err(e) =
            update_post_collection(post_collection_pk, &get_system_user(), request, connection)
                .await
        {
            log::error!(
                "Failed to apply auto tag {} for collection {}: {e}",
                &tag.tag_name,
                post_collection_pk
            );
            return Err(e);
        }
    }

    log::info!(
        "Applied auto tag {} after {}ms",
        &tag.tag_name,
        instant.elapsed().as_millis()
    );
    Ok(())
}

pub async fn apply_tag_category_auto_tags(
    tag_category: &TagCategory,
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    log::debug!("Applying auto tags in category {}", &tag_category.id);
    let instant = std::time::Instant::now();

    let tags = Tag::belonging_to(&tag_category)
        .load::<Tag>(connection)
        .await?;
    let tag_pks = tags.iter().map(|t| t.pk).collect::<Vec<_>>();
    log::debug!(
        "Going to apply {} auto tags for category {}",
        tags.len(),
        &tag_category.id
    );

    struct AutoTagMatches {
        existing_matches: Vec<i64>,
        new_matches: Vec<i64>,
    }
    impl AutoTagMatches {
        fn new() -> Self {
            Self {
                existing_matches: Vec::new(),
                new_matches: Vec::new(),
            }
        }
    }

    let mut post_tag_map: HashMap<i64, AutoTagMatches> = HashMap::new();
    let mut post_collection_tag_map: HashMap<i64, AutoTagMatches> = HashMap::new();

    let existing_post_tags = post_tag::table
        .filter(
            post_tag::fk_tag
                .eq_any(&tag_pks)
                .and(post_tag::auto_matched),
        )
        .load::<PostTag>(connection)
        .await?;
    for existing_post_tag in existing_post_tags {
        post_tag_map
            .entry(existing_post_tag.fk_post)
            .or_insert_with(AutoTagMatches::new)
            .existing_matches
            .push(existing_post_tag.fk_tag);
    }
    let existing_post_collection_tags = post_collection_tag::table
        .filter(
            post_collection_tag::fk_tag
                .eq_any(&tag_pks)
                .and(post_collection_tag::auto_matched),
        )
        .load::<PostCollectionTag>(connection)
        .await?;
    for existing_post_collection_tag in existing_post_collection_tags {
        post_collection_tag_map
            .entry(existing_post_collection_tag.fk_post_collection)
            .or_insert_with(AutoTagMatches::new)
            .existing_matches
            .push(existing_post_collection_tag.fk_tag);
    }

    for tag in tags {
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

    for (post_pk, mut tag_auto_matches) in post_tag_map {
        // remove only those that don't match anymore
        tag_auto_matches
            .existing_matches
            .retain(|tag_pk| !tag_auto_matches.new_matches.contains(tag_pk));
        log::debug!(
            "Applying {} auto tags and remove {} no longer matching tags from category {} for post {post_pk}",
            tag_auto_matches.new_matches.len(),
            tag_auto_matches.existing_matches.len(),
            &tag_category.id
        );
        let request = EditPostRequest {
            tags_overwrite: None,
            tag_pks_overwrite: None,
            removed_tag_pks: Some(tag_auto_matches.existing_matches),
            added_tag_pks: Some(tag_auto_matches.new_matches),
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

        if let Err(e) = update_post(post_pk, &get_system_user(), request, connection).await {
            log::error!("Failed to apply auto tags for post {post_pk}: {e}");
            return Err(e);
        }
    }
    for (post_collection_pk, mut tag_auto_matches) in post_collection_tag_map {
        // remove only those that don't match anymore
        tag_auto_matches
            .existing_matches
            .retain(|tag_pk| !tag_auto_matches.new_matches.contains(tag_pk));
        log::debug!(
            "Applying {} auto tags and remove {} no longer matching tags from category {} for collection {post_collection_pk}",
            tag_auto_matches.new_matches.len(),
            tag_auto_matches.existing_matches.len(),
            &tag_category.id
        );
        let request = EditPostCollectionRequest {
            tags_overwrite: None,
            tag_pks_overwrite: None,
            removed_tag_pks: Some(tag_auto_matches.existing_matches),
            added_tag_pks: Some(tag_auto_matches.new_matches),
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

        if let Err(e) =
            update_post_collection(post_collection_pk, &get_system_user(), request, connection)
                .await
        {
            log::error!("Failed to apply auto tags for collection {post_collection_pk}: {e}");
            return Err(e);
        }
    }

    log::info!(
        "Applied auto tags in category {} after {}ms",
        &tag_category.id,
        instant.elapsed().as_millis()
    );
    Ok(())
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
    log::debug!(
        "Compiling auto match condition {:?} for tag: {tag_name}",
        &scope
    );
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
