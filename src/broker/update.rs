use crate::broker::{BrokerJoined, get_broker_joined, is_broker_admin, verify_bucket_connection};
use crate::data::create_bucket;
use crate::diesel::ExpressionMethods;
use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    Broker, BrokerAccess, BrokerAuditAction, BrokerAuditLog, NewBrokerAuditLog, User, UserGroup,
    UserPublic,
};
use crate::schema::{broker, broker_access, broker_audit_log, registered_user, user_group};
use crate::util::NOT_BLANK_REGEX;
use crate::util::deserialize_double_option;
use crate::util::string_value_updated;
use crate::{
    acquire_db_connection, run_repeatable_read_transaction, run_retryable_transaction,
    run_serializable_transaction,
};
use chrono::{DateTime, Utc};
use diesel::{BoolExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(AsChangeset)]
#[diesel(table_name = broker)]
pub struct BrokerUpdateOptional {
    pub name: Option<String>,
    pub description: Option<String>,
    pub remove_duplicate_files: Option<bool>,
    pub enable_presigned_get: Option<bool>,
    pub is_system_bucket: Option<bool>,
    pub total_quota: Option<Option<i64>>,
}

pub struct BrokerUpdateFieldChanges {
    pub name_changed: bool,
    pub description_changed: bool,
    pub remove_duplicate_files_changed: bool,
    pub enable_presigned_get_changed: bool,
    pub is_system_bucket_changed: bool,
    pub total_quota_changed: bool,
}

impl BrokerUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.name_changed
            || self.description_changed
            || self.remove_duplicate_files_changed
            || self.enable_presigned_get_changed
            || self.is_system_bucket_changed
            || self.total_quota_changed
    }
}

impl BrokerUpdateOptional {
    pub fn get_field_changes(&self, curr_value: &Broker) -> BrokerUpdateFieldChanges {
        BrokerUpdateFieldChanges {
            name_changed: string_value_updated(Some(&curr_value.name), self.name.as_deref()),
            description_changed: string_value_updated(
                curr_value.description.as_deref(),
                self.description.as_deref(),
            ),
            remove_duplicate_files_changed: self
                .remove_duplicate_files
                .map(|v| v != curr_value.remove_duplicate_files)
                .unwrap_or(false),
            enable_presigned_get_changed: self
                .enable_presigned_get
                .map(|v| v != curr_value.enable_presigned_get)
                .unwrap_or(false),
            is_system_bucket_changed: self
                .is_system_bucket
                .map(|v| v != curr_value.is_system_bucket)
                .unwrap_or(false),
            total_quota_changed: self
                .total_quota
                .map(|v| v != curr_value.total_quota)
                .unwrap_or(false),
        }
    }

    pub fn has_changes(&self, curr_value: &Broker) -> bool {
        self.get_field_changes(curr_value).has_changes()
    }
}

#[derive(Clone, Deserialize, Validate)]
pub struct EditBrokerRequest {
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub name: Option<String>,
    #[validate(length(max = 30000))]
    pub description: Option<String>,
    pub remove_duplicate_files: Option<bool>,
    pub enable_presigned_get: Option<bool>,
    pub is_system_bucket: Option<bool>,
    #[validate(range(min = 1024))] // 1 KB minimum quota, smaller values are unreasonable and are likely accidental
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub total_quota: Option<Option<i64>>,
    pub disable_uploads: Option<bool>,
}

pub async fn edit_broker_handler(
    request: EditBrokerRequest,
    broker_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for EditBrokerRequest: {e}"
        )))
    })?;

    let is_system_bucket = request.is_system_bucket.unwrap_or(false);
    if is_system_bucket && !user.is_admin {
        return Err(warp::reject::custom(Error::UserNotAdmin));
    }

    let mut connection = acquire_db_connection().await?;
    let broker = run_serializable_transaction(&mut connection, |connection| {
        async move {
            let BrokerJoined { broker, owner } =
                get_broker_joined(broker_pk, &user, connection).await?;
            if !(owner.pk == user.pk || is_broker_admin(broker_pk, &user, connection).await?) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(broker_pk),
                ));
            }

            if is_system_bucket {
                diesel::update(broker::table)
                    .filter(broker::is_system_bucket.eq(true))
                    .set(broker::is_system_bucket.eq(false))
                    .execute(connection)
                    .await?;
            }

            let update = BrokerUpdateOptional {
                name: request.name,
                description: request.description,
                remove_duplicate_files: request.remove_duplicate_files,
                enable_presigned_get: request.enable_presigned_get,
                is_system_bucket: request.is_system_bucket,
                total_quota: request.total_quota,
            };

            let update_field_changes = update.get_field_changes(&broker);

            let now = Utc::now();

            let updated_broker = if update_field_changes.has_changes() {
                diesel::insert_into(broker_audit_log::table)
                    .values(NewBrokerAuditLog {
                        fk_broker: broker.pk,
                        fk_user: user.pk,
                        action: BrokerAuditAction::Edit,
                        fk_target_group: None,
                        new_quota: if update_field_changes.total_quota_changed {
                            update.total_quota.flatten()
                        } else {
                            broker.total_quota
                        },
                        creation_timestamp: now,
                        fk_target_user: None,
                    })
                    .execute(connection)
                    .await?;

                diesel::update(broker::table)
                    .filter(broker::pk.eq(broker.pk))
                    .set(update)
                    .get_result::<Broker>(connection)
                    .await?
            } else {
                broker
            };

            if let Some(disable_uploads) = request.disable_uploads
                && disable_uploads != updated_broker.disable_uploads
            {
                diesel::insert_into(broker_audit_log::table)
                    .values(NewBrokerAuditLog {
                        fk_broker: updated_broker.pk,
                        fk_user: user.pk,
                        action: if disable_uploads {
                            BrokerAuditAction::DisableUploads
                        } else {
                            BrokerAuditAction::EnableUploads
                        },
                        fk_target_group: None,
                        new_quota: None,
                        creation_timestamp: now,
                        fk_target_user: None,
                    })
                    .execute(connection)
                    .await?;

                let updated_broker = diesel::update(broker::table)
                    .filter(broker::pk.eq(updated_broker.pk))
                    .set(broker::disable_uploads.eq(disable_uploads))
                    .get_result::<Broker>(connection)
                    .await?;

                Ok(updated_broker)
            } else {
                Ok(updated_broker)
            }
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&broker))
}

#[derive(AsChangeset, Clone)]
#[diesel(table_name = broker)]
pub struct BrokerBucketUpdateOptional {
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub is_aws_region: Option<bool>,
}

pub struct BrokerBucketUpdateFieldChanges {
    pub bucket_changed: bool,
    pub endpoint_changed: bool,
    pub access_key_changed: bool,
    pub secret_key_changed: bool,
    pub is_aws_region_changed: bool,
}

impl BrokerBucketUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.bucket_changed
            || self.endpoint_changed
            || self.access_key_changed
            || self.secret_key_changed
            || self.is_aws_region_changed
    }
}

impl BrokerBucketUpdateOptional {
    pub fn get_field_changes(&self, curr_value: &Broker) -> BrokerBucketUpdateFieldChanges {
        BrokerBucketUpdateFieldChanges {
            bucket_changed: string_value_updated(Some(&curr_value.bucket), self.bucket.as_deref()),
            endpoint_changed: string_value_updated(
                Some(&curr_value.endpoint),
                self.endpoint.as_deref(),
            ),
            access_key_changed: string_value_updated(
                Some(&curr_value.access_key),
                self.access_key.as_deref(),
            ),
            secret_key_changed: string_value_updated(
                Some(&curr_value.secret_key),
                self.secret_key.as_deref(),
            ),
            is_aws_region_changed: self
                .is_aws_region
                .map(|v| v != curr_value.is_aws_region)
                .unwrap_or(false),
        }
    }

    pub fn has_changes(&self, curr_value: &Broker) -> bool {
        self.get_field_changes(curr_value).has_changes()
    }
}

#[derive(Clone, Deserialize, Validate)]
pub struct EditBrokerBucketRequest {
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub bucket: Option<String>,
    #[validate(length(min = 1, max = 2048), regex(path = *NOT_BLANK_REGEX))]
    pub endpoint: Option<String>,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub access_key: Option<String>,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub secret_key: Option<String>,
    pub is_aws_region: Option<bool>,
}

pub async fn edit_broket_bucket_handler(
    request: EditBrokerBucketRequest,
    broker_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for EditBrokerBucketRequest: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let BrokerJoined { broker, owner } =
        get_broker_joined(broker_pk, &user, &mut connection).await?;
    if !(owner.pk == user.pk || is_broker_admin(broker_pk, &user, &mut connection).await?) {
        return Err(warp::reject::custom(Error::InaccessibleObjectError(
            broker_pk,
        )));
    }
    drop(connection);

    let update = BrokerBucketUpdateOptional {
        bucket: request.bucket.clone(),
        endpoint: request.endpoint.clone(),
        access_key: request.access_key.clone(),
        secret_key: request.secret_key.clone(),
        is_aws_region: request.is_aws_region,
    };

    if !update.has_changes(&broker) {
        return Ok(warp::reply::json(&broker));
    }

    let bucket = request.bucket.unwrap_or(broker.bucket);
    let endpoint = request.endpoint.unwrap_or(broker.endpoint);
    let access_key = request.access_key.unwrap_or(broker.access_key);
    let secret_key = request.secret_key.unwrap_or(broker.secret_key);
    let is_aws_region = request.is_aws_region.unwrap_or(broker.is_aws_region);

    let bucket = create_bucket(&bucket, &endpoint, &access_key, &secret_key, is_aws_region)?;

    verify_bucket_connection(&bucket).await?;

    let mut connection = acquire_db_connection().await?;
    let broker = run_retryable_transaction(&mut connection, |connection| {
        async {
            diesel::insert_into(broker_audit_log::table)
                .values(NewBrokerAuditLog {
                    fk_broker: broker.pk,
                    fk_user: user.pk,
                    action: BrokerAuditAction::BucketConnectionEdit,
                    fk_target_group: None,
                    new_quota: None,
                    creation_timestamp: Utc::now(),
                    fk_target_user: None,
                })
                .execute(connection)
                .await?;

            let updated_broker = diesel::update(broker::table)
                .filter(broker::pk.eq(broker.pk))
                .set(update)
                .get_result::<Broker>(connection)
                .await?;

            Ok(updated_broker)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&broker))
}

#[derive(Deserialize, Validate)]
pub struct ChangeBrokerAccessQuotaRequest {
    #[validate(range(min = 1024))]
    // 1 KB minimum quota, smaller values are unreasonable and are likely accidental
    pub quota: Option<i64>,
}

pub async fn change_broker_access_quota_handler(
    broker_pk: i64,
    broker_access_pk: i64,
    request: ChangeBrokerAccessQuotaRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for ChangeBrokerAccessQuotaRequest: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let updated_broker_access = run_serializable_transaction(&mut connection, |connection| {
        async {
            let BrokerJoined { broker, owner } =
                get_broker_joined(broker_pk, &user, connection).await?;
            if !(owner.pk == user.pk || is_broker_admin(broker_pk, &user, connection).await?) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(broker_pk),
                ));
            }

            let curr_broker_access = broker_access::table
                .filter(
                    broker_access::pk
                        .eq(broker_access_pk)
                        .and(broker_access::fk_broker.eq(broker.pk)),
                )
                .get_result::<BrokerAccess>(connection)
                .await?;

            if curr_broker_access.quota == request.quota {
                return Ok(curr_broker_access);
            }

            // don't allow public access to have unlimited quota
            if request.quota.is_none() && curr_broker_access.fk_granted_group.is_none() {
                return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                    String::from("Cannot grant unlimited quota to public access"),
                )));
            }

            let broker_access = diesel::update(broker_access::table)
                .filter(broker_access::pk.eq(curr_broker_access.pk))
                .set(broker_access::quota.eq(request.quota))
                .get_result::<BrokerAccess>(connection)
                .await
                .optional()?
                .ok_or_else(|| {
                    TransactionRuntimeError::Rollback(Error::InaccessibleObjectError(
                        broker_access_pk,
                    ))
                })?;

            diesel::insert_into(broker_audit_log::table)
                .values(NewBrokerAuditLog {
                    fk_broker: broker.pk,
                    fk_user: user.pk,
                    action: BrokerAuditAction::AccessQuotaEdit,
                    fk_target_group: broker_access.fk_granted_group,
                    new_quota: request.quota,
                    creation_timestamp: Utc::now(),
                    fk_target_user: broker_access.fk_granted_user,
                })
                .execute(connection)
                .await?;

            Ok(broker_access)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&updated_broker_access))
}

#[derive(Deserialize)]
pub struct ChangeBrokerAccessAdminRequest {
    pub is_admin: bool,
}

pub async fn change_broker_access_admin_handler(
    broker_pk: i64,
    broker_access_pk: i64,
    request: ChangeBrokerAccessAdminRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let updated_broker_access = run_serializable_transaction(&mut connection, |connection| {
        async {
            let BrokerJoined { broker, owner } =
                get_broker_joined(broker_pk, &user, connection).await?;
            if !(owner.pk == user.pk || is_broker_admin(broker_pk, &user, connection).await?) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(broker_pk),
                ));
            }

            let curr_broker_access = broker_access::table
                .filter(
                    broker_access::pk
                        .eq(broker_access_pk)
                        .and(broker_access::fk_broker.eq(broker.pk)),
                )
                .get_result::<BrokerAccess>(connection)
                .await?;

            if curr_broker_access.write == request.is_admin {
                return Ok(curr_broker_access);
            }

            // don't allow giving admin privileges to public access
            if request.is_admin && curr_broker_access.fk_granted_group.is_none() {
                return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                    String::from("Cannot grant admin privileges to public access"),
                )));
            }

            let broker_access = diesel::update(broker_access::table)
                .filter(broker_access::pk.eq(curr_broker_access.pk))
                .set(broker_access::write.eq(request.is_admin))
                .get_result::<BrokerAccess>(connection)
                .await?;

            diesel::insert_into(broker_audit_log::table)
                .values(NewBrokerAuditLog {
                    fk_broker: broker.pk,
                    fk_user: user.pk,
                    action: if request.is_admin {
                        BrokerAuditAction::AccessAdminPromote
                    } else {
                        BrokerAuditAction::AccessAdminDemote
                    },
                    fk_target_group: broker_access.fk_granted_group,
                    new_quota: None,
                    creation_timestamp: Utc::now(),
                    fk_target_user: broker_access.fk_granted_user,
                })
                .execute(connection)
                .await?;

            Ok(broker_access)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&updated_broker_access))
}

pub async fn delete_broker_access_handler(
    broker_pk: i64,
    broker_access_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let deleted_broker_access = run_serializable_transaction(&mut connection, |connection| {
        async {
            let BrokerJoined { broker, owner } =
                get_broker_joined(broker_pk, &user, connection).await?;
            if !(owner.pk == user.pk || is_broker_admin(broker_pk, &user, connection).await?) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(broker_pk),
                ));
            }

            let broker_access = diesel::delete(broker_access::table)
                .filter(
                    broker_access::pk
                        .eq(broker_access_pk)
                        .and(broker_access::fk_broker.eq(broker.pk)),
                )
                .get_result::<BrokerAccess>(connection)
                .await
                .optional()?
                .ok_or_else(|| {
                    TransactionRuntimeError::Rollback(Error::InaccessibleObjectError(
                        broker_access_pk,
                    ))
                })?;

            diesel::insert_into(broker_audit_log::table)
                .values(NewBrokerAuditLog {
                    fk_broker: broker.pk,
                    fk_user: user.pk,
                    action: BrokerAuditAction::AccessRevoked,
                    fk_target_group: broker_access.fk_granted_group,
                    new_quota: None,
                    creation_timestamp: Utc::now(),
                    fk_target_user: broker_access.fk_granted_user,
                })
                .execute(connection)
                .await?;

            Ok(broker_access)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&deleted_broker_access))
}

#[derive(Deserialize, Validate)]
pub struct GetBrokerAuditLogsParams {
    #[validate(range(min = 1, max = 100))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
}

#[derive(Serialize)]
pub struct BrokerAuditLogInnerJoined {
    pub pk: i64,
    pub user: UserPublic,
    pub action: BrokerAuditAction,
    pub target_group: Option<UserGroup>,
    pub new_quota: Option<i64>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct GetBrokerAuditLogsResponse {
    pub total_count: i64,
    pub audit_logs: Vec<BrokerAuditLogInnerJoined>,
}

pub async fn get_broker_audit_logs_handler(
    params: GetBrokerAuditLogsParams,
    broker_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    params.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for GetBrokerAuditLogsParams: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            let BrokerJoined { broker, owner } =
                get_broker_joined(broker_pk, &user, connection).await?;
            // only allow broker admins to view audit logs
            if !(owner.pk == user.pk || is_broker_admin(broker_pk, &user, connection).await?) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(broker_pk),
                ));
            }

            let total_count = broker_audit_log::table
                .filter(broker_audit_log::fk_broker.eq(broker.pk))
                .count()
                .get_result::<i64>(connection)
                .await?;

            let limit = params.limit.unwrap_or(50);
            let offset = limit * params.page.unwrap_or(0);

            let logs = broker_audit_log::table
                .inner_join(
                    registered_user::table.on(broker_audit_log::fk_user.eq(registered_user::pk)),
                )
                .left_join(user_group::table)
                .filter(broker_audit_log::fk_broker.eq(broker.pk))
                .order(broker_audit_log::pk.desc())
                .limit(limit.into())
                .offset(offset.into())
                .load::<(BrokerAuditLog, UserPublic, Option<UserGroup>)>(connection)
                .await?;

            let audit_logs = logs
                .into_iter()
                .map(
                    |(audit_log, user, target_group)| BrokerAuditLogInnerJoined {
                        pk: audit_log.pk,
                        user,
                        action: audit_log.action,
                        target_group,
                        new_quota: audit_log.new_quota,
                        creation_timestamp: audit_log.creation_timestamp,
                    },
                )
                .collect::<Vec<_>>();

            Ok(GetBrokerAuditLogsResponse {
                total_count,
                audit_logs,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}
