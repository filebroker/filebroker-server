use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    Tag, TagCategory, TagEditHistory, TagEditHistoryAlias, TagEditHistoryParent, User, UserPublic,
};
use crate::query::PaginationQueryParams;
use crate::schema::{registered_user, tag, tag_category, tag_edit_history};
use crate::tag::auto_matching::spawn_apply_auto_tags_task;
use crate::tag::update::update_tag;
use crate::tag::{get_tag_aliases, get_tag_parents, load_tag_detailed};
use crate::{acquire_db_connection, run_serializable_transaction};
use chrono::{DateTime, Utc};
use diesel::{BelongingToDsl, ExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl, Table};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::Serialize;
use warp::{Rejection, Reply};

#[derive(Serialize)]
pub struct TagEditHistorySnapshot {
    pub pk: i64,
    pub fk_tag: i64,
    pub edit_user: UserPublic,
    pub edit_timestamp: DateTime<Utc>,
    pub tag_category: Option<TagCategory>,
    pub tag_category_changed: bool,
    pub parents_changed: bool,
    pub aliases_changed: bool,
    pub parents: Vec<Tag>,
    pub aliases: Vec<Tag>,
}

#[derive(Serialize)]
pub struct TagEditHistoryResponse {
    pub edit_timestamp: DateTime<Utc>,
    pub edit_user: UserPublic,
    pub total_snapshot_count: i64,
    pub snapshots: Vec<TagEditHistorySnapshot>,
}

pub async fn get_tag_edit_history_handler(
    pagination: PaginationQueryParams,
    tag_pk: i64,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let (tag, edit_user) = tag::table
        .inner_join(registered_user::table.on(tag::fk_edit_user.eq(registered_user::pk)))
        .filter(tag::pk.eq(tag_pk))
        .get_result::<(Tag, UserPublic)>(&mut connection)
        .await
        .optional()
        .map_err(Error::from)?
        .ok_or(Error::NotFoundError)?;

    let total_snapshot_count = tag_edit_history::table
        .filter(tag_edit_history::fk_tag.eq(tag_pk))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .map_err(Error::from)?;

    let limit = pagination.limit.unwrap_or(10);
    let offset = limit * pagination.page.unwrap_or(0);

    let tag_edit_history_snapshots = tag_edit_history::table
        .left_join(tag_category::table)
        .inner_join(registered_user::table)
        .filter(tag_edit_history::fk_tag.eq(tag_pk))
        .order(tag_edit_history::pk.desc())
        .limit(limit.into())
        .offset(offset.into())
        .load::<(TagEditHistory, Option<TagCategory>, UserPublic)>(&mut connection)
        .await
        .map_err(Error::from)?;

    let mut snapshots = Vec::new();
    for (tag_edit_history_snapshot, tag_category, edit_user) in tag_edit_history_snapshots {
        let parents = get_tag_snapshot_parents(&tag_edit_history_snapshot, &mut connection).await?;
        let aliases = get_tag_snapshot_aliases(&tag_edit_history_snapshot, &mut connection).await?;

        snapshots.push(TagEditHistorySnapshot {
            pk: tag_edit_history_snapshot.pk,
            fk_tag: tag_edit_history_snapshot.fk_tag,
            edit_user,
            edit_timestamp: tag_edit_history_snapshot.edit_timestamp,
            tag_category,
            tag_category_changed: tag_edit_history_snapshot.tag_category_changed,
            parents_changed: tag_edit_history_snapshot.parents_changed,
            aliases_changed: tag_edit_history_snapshot.aliases_changed,
            parents,
            aliases,
        });
    }

    Ok(warp::reply::json(&TagEditHistoryResponse {
        edit_timestamp: tag.edit_timestamp,
        edit_user,
        total_snapshot_count,
        snapshots,
    }))
}

async fn get_tag_snapshot_parents(
    tag_edit_history_snapshot: &TagEditHistory,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, Error> {
    let tag_pk = tag_edit_history_snapshot.fk_tag;
    // snapshots store the previous value and only store tags if they have changed,
    // thus we need to load the tags from the next snapshot where parents_changed is true,
    // or the tag's current parents if no such snapshot exists
    let relevant_snapshot = if tag_edit_history_snapshot.parents_changed {
        Some(tag_edit_history_snapshot.clone())
    } else {
        tag_edit_history::table
            .filter(tag_edit_history::fk_tag.eq(tag_pk))
            .filter(tag_edit_history::pk.gt(tag_edit_history_snapshot.pk))
            .filter(tag_edit_history::parents_changed)
            .order(tag_edit_history::pk.asc())
            .first::<TagEditHistory>(connection)
            .await
            .optional()
            .map_err(Error::from)?
    };
    let parents = if let Some(relevant_snapshot) = relevant_snapshot {
        TagEditHistoryParent::belonging_to(&relevant_snapshot)
            .inner_join(tag::table)
            .select(tag::table::all_columns())
            .load::<Tag>(connection)
            .await
            .map_err(Error::from)?
    } else {
        get_tag_parents(tag_pk, connection).await?
    };

    Ok(parents)
}

async fn get_tag_snapshot_aliases(
    tag_edit_history_snapshot: &TagEditHistory,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, Error> {
    let tag_pk = tag_edit_history_snapshot.fk_tag;
    // snapshots store the previous value and only store tags if they have changed,
    // thus we need to load the tags from the next snapshot where aliases_changed is true,
    // or the tag's current aliases if no such snapshot exists
    let relevant_snapshot = if tag_edit_history_snapshot.aliases_changed {
        Some(tag_edit_history_snapshot.clone())
    } else {
        tag_edit_history::table
            .filter(tag_edit_history::fk_tag.eq(tag_pk))
            .filter(tag_edit_history::pk.gt(tag_edit_history_snapshot.pk))
            .filter(tag_edit_history::aliases_changed)
            .order(tag_edit_history::pk.asc())
            .first::<TagEditHistory>(connection)
            .await
            .optional()
            .map_err(Error::from)?
    };
    let aliases = if let Some(relevant_snapshot) = relevant_snapshot {
        TagEditHistoryAlias::belonging_to(&relevant_snapshot)
            .inner_join(tag::table)
            .select(tag::table::all_columns())
            .load::<Tag>(connection)
            .await
            .map_err(Error::from)?
    } else {
        get_tag_aliases(tag_pk, connection).await?
    };

    Ok(aliases)
}

pub async fn rewind_tag_history_snapshot_handler(
    tag_edit_history_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let (tag_joined, apply_auto_tags_task) =
        run_serializable_transaction(&mut connection, |connection| {
            async {
                let tag_snapshot = tag_edit_history::table
                    .find(tag_edit_history_pk)
                    .get_result::<TagEditHistory>(connection)
                    .await
                    .optional()
                    .map_err(Error::from)?
                    .ok_or(TransactionRuntimeError::Rollback(Error::NotFoundError))?;

                let tag_parents = get_tag_snapshot_parents(&tag_snapshot, connection).await?;
                let tag_aliases = get_tag_snapshot_aliases(&tag_snapshot, connection).await?;

                let (tag, _, apply_auto_tags_task) = update_tag(
                    tag_snapshot.fk_tag,
                    &user,
                    None,
                    Some(tag_parents.into_iter().map(|tag| tag.pk).collect()),
                    None,
                    None,
                    Some(tag_aliases.into_iter().map(|tag| tag.pk).collect()),
                    None,
                    //
                    Some(
                        tag_snapshot
                            .tag_category
                            .unwrap_or_else(|| String::from("")),
                    ),
                    None,
                    None,
                    connection,
                )
                .await?;

                Ok((
                    load_tag_detailed(tag, connection).await?,
                    apply_auto_tags_task,
                ))
            }
            .scope_boxed()
        })
        .await?;

    if let Some(apply_auto_tags_task) = apply_auto_tags_task {
        spawn_apply_auto_tags_task(apply_auto_tags_task);
    }

    Ok(warp::reply::json(&tag_joined))
}
