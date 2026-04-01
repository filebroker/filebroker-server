pub mod create;
pub mod history;
pub mod invite;
pub mod update;

use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    Broker, BrokerAccess, NewUserGroupAuditLog, Tag, User, UserGroup, UserGroupAuditAction,
    UserGroupMembership, UserPublic,
};
use crate::perms::{get_group_membership_condition, load_user_group_secured};
use crate::query::{ApplyOrderFn, apply_key_ordering, order_by_col_fn, order_by_col_with_tie_fn};
use crate::schema::{
    broker, broker_access, registered_user, s3_object, tag, user_group, user_group_audit_log,
    user_group_membership, user_group_tag,
};
use crate::tag::TagUsage;
use crate::{
    acquire_db_connection, run_repeatable_read_transaction, run_retryable_transaction, schema,
};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, Utc};
use diesel::dsl::{not, sum};
use diesel::query_builder::BoxedSelectStatement;
use diesel::sql_types::Bool;
use diesel::{
    BoolExpressionMethods, ExpressionMethods, IntoSql, JoinOnDsl, NullableExpressionMethods,
    OptionalExtension, QueryDsl, Table,
};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(Serialize)]
pub struct UserGroupMembershipDetailed {
    pub group: UserGroupDetailed,
    pub user: UserPublic,
    pub administrator: bool,
    pub is_owner: bool,
    pub revoked: bool,
    pub granted_by: UserPublic,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct UserGroupDetailed {
    pub pk: i64,
    pub name: String,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub owner: UserPublic,
    pub creation_timestamp: DateTime<Utc>,
    pub description: Option<String>,
    pub allow_member_invite: bool,
    pub avatar_object_key: Option<String>,
    pub create_user: UserPublic,
    pub edit_timestamp: DateTime<Utc>,
    pub edit_user: UserPublic,
    // tags are not included in lists of multiple user groups, such as get_user_group_memberships
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<TagUsage>>,
    pub is_editable: bool,
    pub user_can_invite: bool,
}

#[derive(Serialize)]
pub struct UserGroupJoined {
    pub group: UserGroupDetailed,
    pub membership: Option<UserGroupMembershipInnerJoined>,
}

#[derive(Serialize)]
pub struct UserGroupMembershipInnerJoined {
    pub user: UserPublic,
    pub administrator: bool,
    pub is_owner: bool,
    pub revoked: bool,
    pub granted_by: UserPublic,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Deserialize, Validate)]
pub struct GetCurrentUserGroupMembershipsParams {
    #[validate(range(min = 1, max = 50))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
    pub ordering: Option<String>,
}

#[derive(Serialize)]
pub struct GetCurrentUserGroupMembershipsResponse {
    pub total_count: i64,
    pub memberships: Vec<UserGroupMembershipDetailed>,
}

pub async fn get_current_user_group_memberships_handler(
    params: GetCurrentUserGroupMembershipsParams,
    user: User,
) -> Result<impl Reply, Rejection> {
    params.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for GetCurrentUserGroupMembershipsParams: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let (total_count, memberships) =
        run_repeatable_read_transaction(&mut connection, |connection| {
            async {
                let total_count = user_group::table
                    .filter(get_group_membership_condition!(user.pk))
                    .count()
                    .get_result::<i64>(connection)
                    .await?;

                let memberships = get_user_group_memberships(
                    user,
                    connection,
                    Some(params.limit.unwrap_or(50)),
                    params.page,
                    params.ordering,
                )
                .await?;

                Ok((total_count, memberships))
            }
            .scope_boxed()
        })
        .await?;

    Ok(warp::reply::json(&GetCurrentUserGroupMembershipsResponse {
        total_count,
        memberships,
    }))
}

/// Return all group memberships of the given user. This includes groups where the user is the group
/// owner, returning a `UserGroupMembershipDetailed` with `is_owner` = true, even though there is no
/// `user_group_membership` row for the group owner in the DB.
pub async fn get_user_group_memberships(
    user: User,
    connection: &mut AsyncPgConnection,
    limit: Option<u32>,
    page: Option<u32>,
    ordering: Option<String>,
) -> Result<Vec<UserGroupMembershipDetailed>, Error> {
    let user_pk = user.pk;
    let curr_user = user;
    let (user, create_user, edit_user, granted_by_user, owner, membership) = diesel::alias!(
        schema::registered_user as user,
        schema::registered_user as create_user,
        schema::registered_user as edit_user,
        schema::registered_user as granted_by_user,
        schema::registered_user as owner,
        schema::user_group_membership as membership,
    );

    let membership_query = user_group::table
        .inner_join(owner.on(user_group::fk_owner.eq(owner.field(registered_user::pk))))
        .inner_join(
            create_user.on(user_group::fk_create_user.eq(create_user.field(registered_user::pk))),
        )
        .inner_join(edit_user.on(user_group::fk_edit_user.eq(edit_user.field(registered_user::pk))))
        .left_join(
            membership.on(membership
                .field(user_group_membership::fk_group)
                .eq(user_group::pk)
                .and(membership.field(user_group_membership::fk_user).eq(user_pk))),
        )
        .left_join(
            user.on(membership
                .field(user_group_membership::fk_user)
                .eq(user.field(registered_user::pk))),
        )
        .left_join(
            granted_by_user.on(membership
                .field(user_group_membership::fk_granted_by)
                .eq(granted_by_user.field(registered_user::pk))),
        )
        .filter(get_group_membership_condition!(user_pk))
        .select((
            user_group::all_columns,
            owner.fields(registered_user::all_columns),
            create_user.fields(registered_user::all_columns),
            edit_user.fields(registered_user::all_columns),
            membership
                .fields(user_group_membership::all_columns)
                .nullable(),
            user.fields(registered_user::all_columns).nullable(),
            granted_by_user
                .fields(registered_user::all_columns)
                .nullable(),
        ));

    let mut order_map: HashMap<&'static str, Box<ApplyOrderFn<_, _, _>>> = HashMap::new();
    order_map.insert("group.pk", order_by_col_fn(user_group::pk));
    order_map.insert(
        "group.name",
        order_by_col_with_tie_fn(user_group::name, user_group::pk.desc()),
    );
    order_map.insert(
        "creation_timestamp",
        order_by_col_with_tie_fn(user_group::creation_timestamp, user_group::pk.desc()),
    );

    let membership_query_ordered = apply_key_ordering(
        ordering,
        ("group.name", false),
        membership_query.into_boxed(),
        order_map,
    )?;

    let res = if let Some(limit) = limit {
        membership_query_ordered
            .limit(limit as i64)
            .offset((page.unwrap_or(0) * limit) as i64)
            .load::<(
                UserGroup,
                UserPublic,
                UserPublic,
                UserPublic,
                Option<UserGroupMembership>,
                Option<UserPublic>,
                Option<UserPublic>,
            )>(connection)
            .await?
    } else {
        membership_query_ordered
            .load::<(
                UserGroup,
                UserPublic,
                UserPublic,
                UserPublic,
                Option<UserGroupMembership>,
                Option<UserPublic>,
                Option<UserPublic>,
            )>(connection)
            .await?
    };

    let memberships = res
        .into_iter()
        .map(
            |(group, owner, create_user, edit_user, membership, user, granted_by_user)| {
                let is_editable = is_user_group_editable(&group, &curr_user, membership.as_ref());
                let user_can_invite =
                    user_can_invite_to_group(&group, &curr_user, membership.as_ref());
                UserGroupMembershipDetailed {
                    group: UserGroupDetailed {
                        pk: group.pk,
                        name: group.name,
                        public: group.public,
                        owner: owner.clone(),
                        creation_timestamp: group.creation_timestamp,
                        description: group.description,
                        allow_member_invite: group.allow_member_invite,
                        avatar_object_key: group.avatar_object_key,
                        create_user,
                        edit_timestamp: group.edit_timestamp,
                        edit_user,
                        tags: None,
                        is_editable,
                        user_can_invite,
                    },
                    user: user.unwrap_or_else(|| owner.clone()), // if no membership, therefore no membership user -> user is owner
                    administrator: membership.as_ref().map(|m| m.administrator).unwrap_or(true), // if none -> no membership present -> user is group owner -> therefore admin
                    is_owner: user_pk == group.fk_owner,
                    revoked: membership.as_ref().map(|m| m.revoked).unwrap_or(false),
                    granted_by: granted_by_user
                        .unwrap_or_else(|| UserPublic::from(curr_user.clone())),
                    creation_timestamp: membership
                        .map(|m| m.creation_timestamp)
                        .unwrap_or(group.creation_timestamp), // if no membership -> user is owner -> use group creation date as membership creation date
                }
            },
        )
        .collect::<Vec<_>>();

    Ok(memberships)
}

pub async fn load_user_group_detailed(
    user_group: UserGroup,
    user: Option<&User>,
    connection: &mut AsyncPgConnection,
) -> Result<UserGroupDetailed, Error> {
    let user_pk = user.map(|u| u.pk);
    let tags = get_user_group_tags(user_group.pk, connection).await?;

    let (owner, create_user, edit_user, membership) = diesel::alias!(
        schema::registered_user as owner,
        schema::registered_user as create_user,
        schema::registered_user as edit_user,
        schema::user_group_membership as membership,
    );
    let (owner, create_user, edit_user, membership) = user_group::table
        .inner_join(owner.on(user_group::fk_owner.eq(owner.field(registered_user::pk))))
        .inner_join(
            create_user.on(user_group::fk_create_user.eq(create_user.field(registered_user::pk))),
        )
        .inner_join(edit_user.on(user_group::fk_edit_user.eq(edit_user.field(registered_user::pk))))
        .left_join(
            membership.on(membership
                .field(user_group_membership::fk_group)
                .eq(user_group::pk)
                .and(
                    membership
                        .field(user_group_membership::fk_user)
                        .nullable()
                        .eq(user_pk),
                )),
        )
        .select((
            owner.fields(registered_user::table::all_columns()),
            create_user.fields(registered_user::table::all_columns()),
            edit_user.fields(registered_user::table::all_columns()),
            membership
                .fields(user_group_membership::all_columns)
                .nullable(),
        ))
        .filter(user_group::pk.eq(user_group.pk))
        .get_result::<(
            UserPublic,
            UserPublic,
            UserPublic,
            Option<UserGroupMembership>,
        )>(connection)
        .await
        .map_err(Error::from)?;

    let (is_editable, user_can_invite) = if let Some(user) = user {
        (
            is_user_group_editable(&user_group, user, membership.as_ref()),
            user_can_invite_to_group(&user_group, user, membership.as_ref()),
        )
    } else {
        (false, false)
    };

    Ok(UserGroupDetailed {
        pk: user_group.pk,
        name: user_group.name,
        public: user_group.public,
        owner,
        creation_timestamp: user_group.creation_timestamp,
        description: user_group.description,
        allow_member_invite: user_group.allow_member_invite,
        avatar_object_key: user_group.avatar_object_key,
        create_user,
        edit_timestamp: user_group.edit_timestamp,
        edit_user,
        tags: Some(tags),
        is_editable,
        user_can_invite,
    })
}

pub async fn get_user_group_handler(
    user_group_pk: i64,
    user: Option<User>,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let user_group = get_user_group_joined(user_group_pk, user.as_ref(), &mut connection).await?;
    Ok(warp::reply::json(&user_group))
}

pub async fn leave_user_group_handler(
    user_group_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let now = Utc::now();

    run_retryable_transaction(&mut connection, |connection| {
        async move {
            let deleted_membership = diesel::delete(user_group_membership::table)
                .filter(
                    user_group_membership::fk_group
                        .eq(user_group_pk)
                        .and(user_group_membership::fk_user.eq(user.pk)),
                )
                .get_result::<UserGroupMembership>(connection)
                .await
                .optional()?;

            if let Some(deleted_membership) = deleted_membership {
                // don't allow banned users to leave, which would delete their banned membership and allow them to rejoin / be reinvited
                if deleted_membership.revoked {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::UserBannedFromGroupError(user_group_pk),
                    ));
                }
            } else {
                return Err(TransactionRuntimeError::Rollback(Error::NotFoundError));
            }

            diesel::insert_into(user_group_audit_log::table)
                .values(NewUserGroupAuditLog {
                    fk_user_group: user_group_pk,
                    fk_user: user.pk,
                    action: UserGroupAuditAction::Leave,
                    fk_target_user: None,
                    invite_code: None,
                    reason: None,
                    creation_timestamp: now,
                })
                .execute(connection)
                .await?;

            Ok(())
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply())
}

pub async fn get_user_group_joined(
    user_group_pk: i64,
    user: Option<&User>,
    connection: &mut AsyncPgConnection,
) -> Result<UserGroupJoined, Error> {
    let user_pk = user.map(|u| u.pk);

    let (membership_user, create_user, edit_user, granted_by_user, owner, membership) = diesel::alias!(
        schema::registered_user as membership_user,
        schema::registered_user as create_user,
        schema::registered_user as edit_user,
        schema::registered_user as granted_by_user,
        schema::registered_user as owner,
        schema::user_group_membership as membership,
    );

    let (
        user_group,
        owner,
        create_user,
        edit_user,
        membership,
        membership_user,
        membership_granted_by_user,
    ) = user_group::table
        .inner_join(owner.on(user_group::fk_owner.eq(owner.field(registered_user::pk))))
        .inner_join(
            create_user.on(user_group::fk_create_user.eq(create_user.field(registered_user::pk))),
        )
        .inner_join(edit_user.on(user_group::fk_edit_user.eq(edit_user.field(registered_user::pk))))
        .left_join(
            membership.on(membership
                .field(user_group_membership::fk_group)
                .eq(user_group::pk)
                .and(
                    membership
                        .field(user_group_membership::fk_user)
                        .nullable()
                        .eq(user_pk),
                )),
        )
        .left_join(
            membership_user.on(membership
                .field(user_group_membership::fk_user)
                .eq(membership_user.field(registered_user::pk))),
        )
        .left_join(
            granted_by_user.on(membership
                .field(user_group_membership::fk_granted_by)
                .eq(granted_by_user.field(registered_user::pk))),
        )
        .filter(
            user_group::pk.eq(user_group_pk).and(
                user.map(|u| u.is_admin)
                    .unwrap_or(false)
                    .into_sql::<Bool>()
                    .or(user_group::public)
                    .or(get_group_membership_condition!(user_pk)),
            ),
        )
        .select((
            user_group::all_columns,
            owner.fields(registered_user::all_columns),
            create_user.fields(registered_user::all_columns),
            edit_user.fields(registered_user::all_columns),
            membership
                .fields(user_group_membership::all_columns)
                .nullable(),
            membership_user
                .fields(registered_user::all_columns)
                .nullable(),
            granted_by_user
                .fields(registered_user::all_columns)
                .nullable(),
        ))
        .get_result::<(
            UserGroup,
            UserPublic,
            UserPublic,
            UserPublic,
            Option<UserGroupMembership>,
            Option<UserPublic>,
            Option<UserPublic>,
        )>(connection)
        .await
        .optional()
        .map_err(Error::from)?
        .ok_or(Error::InaccessibleObjectError(user_group_pk))?;

    let tags = get_user_group_tags(user_group_pk, connection).await?;
    let (is_editable, user_can_invite) = if let Some(user) = user {
        (
            is_user_group_editable(&user_group, user, membership.as_ref()),
            user_can_invite_to_group(&user_group, user, membership.as_ref()),
        )
    } else {
        (false, false)
    };

    Ok(UserGroupJoined {
        group: UserGroupDetailed {
            pk: user_group.pk,
            name: user_group.name,
            public: user_group.public,
            owner,
            creation_timestamp: user_group.creation_timestamp,
            description: user_group.description,
            allow_member_invite: user_group.allow_member_invite,
            avatar_object_key: user_group.avatar_object_key,
            create_user,
            edit_timestamp: user_group.edit_timestamp,
            edit_user,
            tags: Some(tags),
            is_editable,
            user_can_invite,
        },
        membership: if let Some(membership) = membership
            && let Some(membership_user) = membership_user
            && let Some(membership_granted_by_user) = membership_granted_by_user
        {
            Some(UserGroupMembershipInnerJoined {
                user: membership_user,
                administrator: membership.administrator,
                is_owner: user_pk
                    .map(|upk| upk == user_group.fk_owner)
                    .unwrap_or(false),
                revoked: membership.revoked,
                granted_by: membership_granted_by_user,
                creation_timestamp: membership.creation_timestamp,
            })
        } else if let Some(user) = user
            && user.pk == user_group.fk_owner
        {
            Some(UserGroupMembershipInnerJoined {
                user: UserPublic::from(user.clone()),
                administrator: true,
                is_owner: true,
                revoked: false,
                granted_by: UserPublic::from(user.clone()),
                creation_timestamp: user_group.creation_timestamp,
            })
        } else {
            None
        },
    })
}

pub async fn get_user_group_tags(
    user_group_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<TagUsage>, Error> {
    Ok(user_group_tag::table
        .inner_join(tag::table)
        .select((tag::table::all_columns(), user_group_tag::auto_matched))
        .filter(user_group_tag::fk_user_group.eq(user_group_pk))
        .load::<(Tag, bool)>(connection)
        .await?
        .into_iter()
        .map(|(tag, auto_matched)| TagUsage { tag, auto_matched })
        .collect::<Vec<_>>())
}

pub fn is_user_group_editable(
    user_group: &UserGroup,
    user: &User,
    membership: Option<&UserGroupMembership>,
) -> bool {
    user.is_admin
        || user_group.fk_owner == user.pk
        || membership.is_some_and(|m| m.administrator && !m.revoked)
}

pub fn is_user_group_detailed_editable(
    user_group: &UserGroupDetailed,
    user: &User,
    membership: Option<&UserGroupMembershipInnerJoined>,
) -> bool {
    user.is_admin
        || user_group.owner.pk == user.pk
        || membership.is_some_and(|m| m.administrator && !m.revoked)
}

pub fn user_can_invite_to_group(
    user_group: &UserGroup,
    user: &User,
    membership: Option<&UserGroupMembership>,
) -> bool {
    user.is_admin
        || user_group.fk_owner == user.pk
        || membership
            .is_some_and(|m| (m.administrator || user_group.allow_member_invite) && !m.revoked)
}

#[derive(Deserialize, Validate)]
pub struct GetUserGroupMembersParams {
    #[validate(range(min = 1, max = 50))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
    pub ordering: Option<String>,
    pub admins_only: Option<bool>,
    pub revoked_only: Option<bool>,
}

#[derive(Serialize)]
pub struct GetUserGroupMembersResponse {
    pub total_count: i64,
    pub members: Vec<UserGroupMembershipInnerJoined>,
}

pub async fn get_user_group_members_handler(
    user_group_pk: i64,
    params: GetUserGroupMembersParams,
    user: User,
) -> Result<impl Reply, Rejection> {
    params.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for GetUserGroupMembersParams: {e}"
        )))
    })?;
    let admins_only = params.admins_only.unwrap_or(false);
    let revoked_only = params.revoked_only.unwrap_or(false);

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            let user_group = get_user_group_joined(user_group_pk, Some(&user), connection).await?;
            if !(user.is_admin
                || user_group.group.owner.pk == user.pk
                || user_group.membership.is_some_and(|m| !m.revoked))
            {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(user_group_pk),
                ));
            }

            let filter = user_group_membership::fk_group
                .eq(user_group_pk)
                .and(not(admins_only.into_sql::<Bool>()).or(user_group_membership::administrator))
                .and(
                    revoked_only
                        .into_sql::<Bool>()
                        .and(user_group_membership::revoked)
                        .or(not(revoked_only.into_sql::<Bool>())
                            .and(not(user_group_membership::revoked))),
                );

            let total_count = user_group_membership::table
                .filter(filter)
                .count()
                .get_result::<i64>(connection)
                .await?;

            let (user, granted_by_user) = diesel::alias!(
                schema::registered_user as user,
                schema::registered_user as granted_by_user,
            );

            let member_query = user_group_membership::table
                .inner_join(
                    user.on(user_group_membership::fk_user.eq(user.field(registered_user::pk))),
                )
                .inner_join(
                    granted_by_user.on(user_group_membership::fk_granted_by
                        .eq(granted_by_user.field(registered_user::pk))),
                )
                .filter(filter);

            let mut order_map: HashMap<&'static str, Box<ApplyOrderFn<_, _, _>>> = HashMap::new();

            order_map.insert(
                "user",
                Box::new(move |desc, q: BoxedSelectStatement<'_, _, _, _>| {
                    if desc {
                        q.order((
                            user.field(registered_user::pk).desc(),
                            user_group_membership::creation_timestamp.desc(),
                            user_group_membership::fk_user.desc(),
                        ))
                    } else {
                        q.order((
                            user.field(registered_user::pk),
                            user_group_membership::creation_timestamp.desc(),
                            user_group_membership::fk_user.desc(),
                        ))
                    }
                }),
            );
            order_map.insert(
                "granted_by",
                Box::new(move |desc, q| {
                    if desc {
                        q.order((
                            granted_by_user.field(registered_user::pk).desc(),
                            user_group_membership::creation_timestamp.desc(),
                            user_group_membership::fk_user.desc(),
                        ))
                    } else {
                        q.order((
                            granted_by_user.field(registered_user::pk),
                            user_group_membership::creation_timestamp.desc(),
                            user_group_membership::fk_user.desc(),
                        ))
                    }
                }),
            );
            order_map.insert(
                "creation_timestamp",
                order_by_col_with_tie_fn(
                    user_group_membership::creation_timestamp,
                    user_group_membership::fk_user.desc(),
                ),
            );

            let member_query_ordered = apply_key_ordering(
                params.ordering,
                ("creation_timestamp", true),
                member_query.into_boxed(),
                order_map,
            )?;

            let limit = params.limit.unwrap_or(50);
            let records = member_query_ordered
                .limit(limit as i64)
                .offset((params.page.unwrap_or(0) * limit) as i64)
                .load::<(UserGroupMembership, UserPublic, UserPublic)>(connection)
                .await?;

            let members = records
                .into_iter()
                .map(
                    |(membership, user, granted_by)| UserGroupMembershipInnerJoined {
                        user,
                        administrator: membership.administrator,
                        is_owner: false,
                        revoked: membership.revoked,
                        granted_by,
                        creation_timestamp: membership.creation_timestamp,
                    },
                )
                .collect::<Vec<_>>();

            Ok(GetUserGroupMembersResponse {
                total_count,
                members,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}

#[derive(Deserialize, Validate)]
pub struct GetUserGroupBrokersParams {
    #[validate(range(min = 1, max = 50))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
    pub ordering: Option<String>,
}

#[derive(Serialize)]
pub struct BrokerAccessInnerJoined {
    pub pk: i64,
    pub broker: Broker,
    pub write: bool,
    pub quota: Option<i64>,
    pub used_bytes: i64,
    pub granted_by: UserPublic,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct GetUserGroupBrokersResponse {
    pub total_count: i64,
    pub brokers: Vec<BrokerAccessInnerJoined>,
}

pub async fn get_user_group_brokers_handler(
    user_group_pk: i64,
    params: GetUserGroupBrokersParams,
    user: User,
) -> Result<impl Reply, Rejection> {
    params.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for GetUserGroupBrokersParams: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            let user_group =
                load_user_group_secured(user_group_pk, Some(&user), connection).await?;

            let total_count = broker_access::table
                .filter(broker_access::fk_granted_group.eq(user_group.pk))
                .count()
                .get_result::<i64>(connection)
                .await?;

            let broker_query = broker_access::table
                .inner_join(broker::table)
                .inner_join(
                    registered_user::table.on(broker_access::fk_granted_by.eq(registered_user::pk)),
                )
                .filter(broker_access::fk_granted_group.eq(user_group.pk));

            let mut order_map: HashMap<&'static str, Box<ApplyOrderFn<_, _, _>>> = HashMap::new();
            order_map.insert(
                "broker.name",
                order_by_col_with_tie_fn(broker::name, broker::pk.desc()),
            );
            order_map.insert(
                "creation_timestamp",
                order_by_col_with_tie_fn(
                    broker_access::creation_timestamp,
                    broker_access::pk.desc(),
                ),
            );
            order_map.insert(
                "granted_by",
                order_by_col_with_tie_fn(registered_user::pk, broker_access::pk.desc()),
            );
            order_map.insert("broker.pk", order_by_col_fn(broker::pk));
            order_map.insert("pk", order_by_col_fn(broker_access::pk));

            let broker_query_ordered = apply_key_ordering(
                params.ordering,
                ("broker.name", false),
                broker_query.into_boxed(),
                order_map,
            )?;

            let limit = params.limit.unwrap_or(50);
            let records = broker_query_ordered
                .limit(limit as i64)
                .offset((params.page.unwrap_or(0) * limit) as i64)
                .load::<(BrokerAccess, Broker, UserPublic)>(connection)
                .await?;

            let broker_pks = records
                .iter()
                .map(|(_, broker, _)| broker.pk)
                .collect::<Vec<_>>();
            let broker_usages = broker_access::table
                .inner_join(
                    user_group_membership::table.on(user_group_membership::fk_group
                        .nullable()
                        .eq(broker_access::fk_granted_group)),
                )
                .inner_join(
                    s3_object::table.on(s3_object::fk_uploader
                        .eq(user_group_membership::fk_user)
                        .and(s3_object::fk_broker.eq(broker_access::fk_broker))),
                )
                .inner_join(broker::table.on(broker::pk.eq(broker_access::fk_broker)))
                .filter(broker_access::fk_granted_group.eq(user_group.pk))
                .filter(broker_access::fk_broker.eq_any(broker_pks))
                .filter(s3_object::fk_uploader.ne(broker::fk_owner))
                .group_by(broker_access::pk)
                .select((broker_access::pk, sum(s3_object::size_bytes)))
                .load::<(i64, Option<BigDecimal>)>(connection)
                .await?
                .into_iter()
                .map(|(broker_pk, size_used)| (broker_pk, size_used.unwrap_or(BigDecimal::from(0))))
                .collect::<HashMap<i64, BigDecimal>>();

            let brokers = records
                .into_iter()
                .map(|(broker_access, broker, granted_by)| {
                    let used_bytes = broker_usages
                        .get(&broker_access.pk)
                        .map(|bytes_decimal| {
                            bytes_decimal.to_i64().ok_or_else(|| {
                                Error::InternalError(format!(
                                    "Could not convert broker usage bytes {bytes_decimal} to i64"
                                ))
                            })
                        })
                        .transpose()?
                        .unwrap_or(0);

                    Ok(BrokerAccessInnerJoined {
                        pk: broker_access.pk,
                        broker,
                        write: broker_access.write,
                        quota: broker_access.quota,
                        used_bytes,
                        granted_by,
                        creation_timestamp: broker_access.creation_timestamp,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            Ok(GetUserGroupBrokersResponse {
                total_count,
                brokers,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}
