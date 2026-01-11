use crate::broker::{BrokerJoined, get_broker_joined, is_broker_admin, verify_bucket_connection};
use crate::data::create_bucket;
use crate::diesel::ExpressionMethods;
use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    Broker, BrokerAccess, BrokerAuditAction, NewBroker, NewBrokerAccess, NewBrokerAuditLog, User,
};
use crate::schema::{broker, broker_access, broker_audit_log, registered_user};
use crate::user_group::get_user_group_joined;
use crate::util::NOT_BLANK_REGEX;
use crate::{acquire_db_connection, run_serializable_transaction};
use chrono::Utc;
use diesel::{BoolExpressionMethods, OptionalExtension, PgExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use serde::Deserialize;
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(Deserialize, Validate)]
pub struct CreateBrokerRequest {
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub name: String,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub bucket: String,
    #[validate(length(min = 1, max = 2048), regex(path = *NOT_BLANK_REGEX))]
    pub endpoint: String,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub access_key: String,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub secret_key: String,
    pub is_aws_region: bool,
    pub remove_duplicate_files: bool,
    pub enable_presigned_get: Option<bool>,
    pub is_system_bucket: Option<bool>,
    #[validate(length(max = 30000))]
    pub description: Option<String>,
    #[validate(range(min = 1024))]
    // 1 KB minimum quota, smaller values are unreasonable and are likely accidental
    pub total_quota: Option<i64>,
}

pub async fn create_broker_handler(
    create_broker_request: CreateBrokerRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    create_broker_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateBrokerRequest: {e}"
        )))
    })?;

    let is_system_bucket = create_broker_request.is_system_bucket.unwrap_or(false);
    if is_system_bucket && !user.is_admin {
        return Err(warp::reject::custom(Error::UserNotAdmin));
    }

    let bucket = create_bucket(
        &create_broker_request.bucket,
        &create_broker_request.endpoint,
        &create_broker_request.access_key,
        &create_broker_request.secret_key,
        create_broker_request.is_aws_region,
    )?;

    verify_bucket_connection(&bucket).await?;

    let mut connection = acquire_db_connection().await?;
    let created_broker = run_serializable_transaction(&mut connection, |connection| {
        async {
            if is_system_bucket {
                diesel::update(broker::table)
                    .filter(broker::is_system_bucket.eq(true))
                    .set(broker::is_system_bucket.eq(false))
                    .execute(connection)
                    .await?;
            }
            let broker = diesel::insert_into(broker::table)
                .values(&NewBroker {
                    name: create_broker_request.name,
                    bucket: create_broker_request.bucket,
                    endpoint: create_broker_request.endpoint,
                    access_key: create_broker_request.access_key,
                    secret_key: create_broker_request.secret_key,
                    is_aws_region: create_broker_request.is_aws_region,
                    remove_duplicate_files: create_broker_request.remove_duplicate_files,
                    fk_owner: user.pk,
                    hls_enabled: false,
                    enable_presigned_get: create_broker_request
                        .enable_presigned_get
                        .unwrap_or(true),
                    is_system_bucket,
                    description: create_broker_request.description,
                    total_quota: create_broker_request.total_quota,
                })
                .get_result::<Broker>(connection)
                .await?;

            Ok(broker)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&created_broker))
}

#[derive(Deserialize, Validate)]
pub struct CreateBrokerAccessRequest {
    pub user_pk: Option<i64>,
    pub user_group_pk: Option<i64>,
    #[validate(range(min = 1024))]
    // 1 KB minimum quota, smaller values are unreasonable and are likely accidental
    pub quota: Option<i64>,
    pub is_admin: Option<bool>,
}

pub async fn create_broker_access_handler(
    broker_pk: i64,
    request: CreateBrokerAccessRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateBrokerAccessRequest: {e}"
        )))
    })?;

    let granted_user_pk = request.user_pk;
    let user_group_pk = request.user_group_pk;
    let quota = request.quota;
    let is_admin = request.is_admin.unwrap_or(false);

    if granted_user_pk.is_some() && user_group_pk.is_some() {
        return Err(warp::reject::custom(Error::InvalidRequestInputError(
            String::from("Cannot grant access to both user and user group"),
        )));
    }

    let mut connection = acquire_db_connection().await?;
    let created_broker_access = run_serializable_transaction(&mut connection, |connection| {
        async {
            let BrokerJoined { broker, owner } =
                get_broker_joined(broker_pk, &user, connection).await?;
            if !(owner.pk == user.pk || is_broker_admin(broker_pk, &user, connection).await?) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(broker_pk),
                ));
            }

            // check that referenced user exists
            if let Some(granted_user_pk) = granted_user_pk {
                registered_user::table
                    .filter(registered_user::pk.eq(granted_user_pk))
                    .get_result::<User>(connection)
                    .await
                    .optional()?
                    .ok_or_else(|| {
                        TransactionRuntimeError::Rollback(Error::InaccessibleObjectError(
                            granted_user_pk,
                        ))
                    })?;
            }

            // only allow granting access to groups of which the user is a member
            if let Some(user_group_pk) = user_group_pk {
                let user_group =
                    get_user_group_joined(user_group_pk, Some(&user), connection).await?;
                if !(user.is_admin
                    || user_group.group.owner.pk == user.pk
                    || user_group.membership.is_some_and(|m| !m.revoked))
                {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::InaccessibleObjectError(user_group_pk),
                    ));
                }
            }

            // check if access already exists
            let existing_broker_access = broker_access::table
                .filter(
                    broker_access::fk_broker
                        .eq(broker.pk)
                        .and(broker_access::fk_granted_group.is_not_distinct_from(user_group_pk)),
                )
                .get_result::<BrokerAccess>(connection)
                .await
                .optional()?;
            if existing_broker_access.is_some() {
                return Err(TransactionRuntimeError::Rollback(
                    Error::BrokerAccessAlreadyExistsError(user_group_pk),
                ));
            }

            let public = user_group_pk.is_none() && granted_user_pk.is_none();

            // don't allow giving admin privileges to public access
            if is_admin && public {
                return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                    String::from("Cannot grant admin privileges to public access"),
                )));
            }

            // don't allow public access to have unlimited quota
            if quota.is_none() && public {
                return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                    String::from("Cannot grant unlimited quota to public access"),
                )));
            }

            // only admins can create public broker access
            if public && !user.is_admin {
                return Err(TransactionRuntimeError::Rollback(Error::UserNotAdmin));
            }

            let now = Utc::now();

            diesel::insert_into(broker_audit_log::table)
                .values(NewBrokerAuditLog {
                    fk_broker: broker.pk,
                    fk_user: user.pk,
                    action: BrokerAuditAction::AccessGranted,
                    fk_target_group: user_group_pk,
                    new_quota: quota,
                    creation_timestamp: now,
                    fk_target_user: granted_user_pk,
                })
                .execute(connection)
                .await?;

            let broker_access = diesel::insert_into(broker_access::table)
                .values(NewBrokerAccess {
                    fk_broker: broker.pk,
                    fk_granted_group: user_group_pk,
                    write: is_admin,
                    quota,
                    fk_granted_by: user.pk,
                    creation_timestamp: now,
                    fk_granted_user: granted_user_pk,
                    public,
                })
                .get_result::<BrokerAccess>(connection)
                .await?;

            Ok(broker_access)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&created_broker_access))
}
