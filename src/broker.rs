use crate::data::create_bucket;
use crate::diesel::ExpressionMethods;
use crate::diesel::NullableExpressionMethods;
use crate::error::{Error, TransactionRuntimeError};
use crate::model::{Broker, BrokerAccess, ObjectType, User, UserGroup, UserPublic};
use crate::perms::{get_broker_access_condition, get_broker_access_write_condition};
use crate::perms::{
    get_broker_write_condition, get_group_membership_administrator_condition,
    get_group_membership_condition,
};
use crate::query::{ApplyOrderFn, apply_key_ordering, order_by_col_fn, order_by_col_with_tie_fn};
use crate::schema::{
    broker, broker_access, registered_user, s3_object, user_group, user_group_membership,
};
use crate::{acquire_db_connection, perms, run_repeatable_read_transaction};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, Utc};
use diesel::dsl::{case_when, exists, max, not, sum};
use diesel::query_builder::BoxedSelectStatement;
use diesel::sql_types::{Array, BigInt, Bool, Nullable, Numeric};
use diesel::{
    BoolExpressionMethods, IntoSql, JoinOnDsl, OptionalExtension, PgSortExpressionMethods, QueryDsl,
};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use s3::Bucket;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use url::Url;
use uuid::Uuid;
use validator::Validate;
use warp::{Rejection, Reply};

pub mod create;
pub mod update;

#[derive(Serialize)]
pub struct BrokerJoined {
    pub broker: Broker,
    pub owner: UserPublic,
}

pub async fn get_broker_joined(
    broker_pk: i64,
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<BrokerJoined, Error> {
    let (broker, owner) = broker::table
        .inner_join(registered_user::table.on(broker::fk_owner.eq(registered_user::pk)))
        .filter(
            broker::pk.eq(broker_pk).and(
                user.is_admin
                    .into_sql::<Bool>()
                    .or(broker::fk_owner.eq(user.pk))
                    .or(exists(
                        broker_access::table.filter(get_broker_access_condition!(user.pk)),
                    )),
            ),
        )
        .get_result::<(Broker, UserPublic)>(connection)
        .await
        .optional()?
        .ok_or(Error::InaccessibleObjectError(broker_pk))?;

    Ok(BrokerJoined { broker, owner })
}

/// Get the most applicable broker_access quota for the given user. `None` means unlimited quota,
/// and is returned for broker owner, broker admins, or when a broker_access with unlimited quota exists.
/// If the user doesn't have access at all, `Some(0)` is returned.
pub async fn get_broker_access_quota(
    broker: &Broker,
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<Option<i64>, Error> {
    if broker.fk_owner == user.pk {
        return Ok(None);
    }
    if is_broker_admin(broker.pk, user, connection).await? {
        return Ok(None);
    }

    let access = get_applicable_broker_access(broker.pk, user.pk, connection).await?;

    if let Some(access) = access {
        Ok(access.quota)
    } else {
        Ok(Some(0))
    }
}

/// Get the most applicable broker_access for the given user. User-specific access is preferred over group-specific access, and the highest quota is returned (nulls first).
pub async fn get_applicable_broker_access(
    broker_pk: i64,
    user_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Option<BrokerAccess>, Error> {
    broker_access::table
        .filter(
            broker_access::fk_broker.eq(broker_pk).and(
                broker_access::public
                    .or(broker_access::fk_granted_user.eq(user_pk))
                    .or(broker_access::fk_granted_group.eq_any(
                        user_group::table
                            .select(user_group::pk)
                            .filter(get_group_membership_condition!(user_pk))
                            .nullable(),
                    )),
            ),
        )
        .order((
            broker_access::fk_granted_user.asc().nulls_last(),
            broker_access::quota.desc().nulls_first(),
        ))
        .first::<BrokerAccess>(connection)
        .await
        .optional()
        .map_err(Error::from)
}

pub async fn get_broker_quota_used_by_user(
    broker_pk: i64,
    user_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<u128, Error> {
    let used_quota: BigDecimal = s3_object::table
        .select(sum(s3_object::size_bytes))
        .filter(
            s3_object::fk_broker
                .eq(broker_pk)
                .and(s3_object::fk_uploader.eq(user_pk))
                .and(s3_object::object_type.eq(ObjectType::Original)),
        )
        .get_result::<Option<BigDecimal>>(connection)
        .await?
        .unwrap_or_default();

    used_quota
        .to_u128()
        .ok_or(Error::InternalError(String::from(
            "Used quota cannot be converted to u128",
        )))
}

pub async fn is_broker_public(
    broker_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<bool, Error> {
    broker::table
        .select(broker::pk)
        .filter(
            broker::pk.eq(broker_pk).and(exists(
                broker_access::table.filter(
                    broker_access::fk_broker
                        .eq(broker::pk)
                        .and(broker_access::fk_granted_group.is_null()),
                ),
            )),
        )
        .get_result::<i64>(connection)
        .await
        .optional()
        .map_err(Error::from)
        .map(|result| result.is_some())
}

pub async fn is_broker_admin(
    broker_pk: i64,
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<bool, Error> {
    broker::table
        .select(broker::pk)
        .filter(
            broker::pk
                .eq(broker_pk)
                .and(get_broker_write_condition!(Some(user.pk))),
        )
        .get_result::<i64>(connection)
        .await
        .optional()
        .map_err(Error::from)
        .map(|result| result.is_some())
}

#[derive(Serialize)]
pub struct BrokerDetailed {
    pub pk: i64,
    pub name: String,
    pub description: Option<String>,
    pub bucket: String,
    pub endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,
    pub is_aws_region: bool,
    pub remove_duplicate_files: bool,
    pub owner: UserPublic,
    pub creation_timestamp: DateTime<Utc>,
    pub hls_enabled: bool,
    pub enable_presigned_get: bool,
    pub is_system_bucket: bool,
    pub total_quota: Option<i64>,
    pub disable_uploads: bool,
    pub is_public: bool,
    pub is_admin: bool,
    pub used_bytes: i64,
    pub used_quota: i64,
    pub quota_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    // only include for single broker, not broker lists
    pub total_used_bytes: Option<i64>,
}

pub async fn get_broker_handler(broker_pk: i64, user: User) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let broker_detailed = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            let BrokerJoined { broker, owner } =
                get_broker_joined(broker_pk, &user, connection).await?;

            let is_public = is_broker_public(broker.pk, connection).await?;
            let is_admin =
                owner.pk == user.pk || is_broker_admin(broker.pk, &user, connection).await?;

            let (used_bytes, used_quota) = s3_object::table
                .group_by(s3_object::fk_broker)
                .select((
                    sum(s3_object::size_bytes),
                    sum(case_when(
                        s3_object::object_type.eq(ObjectType::Original),
                        s3_object::size_bytes,
                    )
                    .otherwise(0)),
                ))
                .filter(
                    s3_object::fk_uploader
                        .eq(user.pk)
                        .and(s3_object::fk_broker.eq(broker.pk)),
                )
                .get_result::<(Option<BigDecimal>, Option<BigDecimal>)>(connection)
                .await
                .optional()
                .map_err(Error::from)?
                .map(|(usage_bytes, usage_quota)| {
                    let usage_bytes = usage_bytes.unwrap_or_default();
                    let used_bytes = usage_bytes.to_i64().ok_or_else(|| {
                        Error::InternalError(format!(
                            "Could not convert broker usage bytes {usage_bytes} to i64"
                        ))
                    })?;

                    let usage_quota = usage_quota.unwrap_or_default();
                    let used_quota = usage_quota.to_i64().ok_or_else(|| {
                        Error::InternalError(format!(
                            "Could not convert broker usage bytes {usage_quota} to i64"
                        ))
                    })?;

                    Ok::<(i64, i64), Error>((used_bytes, used_quota))
                })
                .unwrap_or(Ok((0, 0)))?;

            let is_unlimited = broker::table
                .select(broker::pk)
                .filter(
                    broker::pk.eq(broker.pk).and(
                        broker::fk_owner.eq(user.pk).or(exists(
                            broker_access::table.filter(
                                get_broker_access_condition!(user.pk)
                                    .and(broker_access::quota.is_null()),
                            ),
                        )),
                    ),
                )
                .get_result::<i64>(connection)
                .await
                .optional()
                .map_err(Error::from)
                .map(|result| result.is_some())?;

            let quota_bytes = if is_unlimited {
                None
            } else {
                broker::table
                    .inner_join(
                        broker_access::table.on(get_broker_access_condition!(user.pk)
                            .and(broker_access::quota.is_not_null())),
                    )
                    .group_by(broker::pk)
                    .select(max(broker_access::quota))
                    .filter(broker::pk.eq(broker.pk))
                    .get_result::<Option<i64>>(connection)
                    .await
                    .optional()
                    .map_err(Error::from)?
                    .flatten()
            };

            let total_used_bytes = if is_admin {
                s3_object::table
                    .select(sum(s3_object::size_bytes))
                    .filter(s3_object::fk_broker.eq(broker.pk))
                    .get_result::<Option<BigDecimal>>(connection)
                    .await
                    .optional()
                    .map_err(Error::from)?
                    .flatten()
                    .map(|usage_bytes| {
                        usage_bytes.to_i64().ok_or_else(|| {
                            Error::InternalError(format!(
                                "Could not convert broker usage bytes {usage_bytes} to i64"
                            ))
                        })
                    })
                    .transpose()?
            } else {
                None
            };

            Ok(BrokerDetailed {
                pk: broker.pk,
                name: broker.name,
                description: broker.description,
                bucket: broker.bucket,
                endpoint: broker.endpoint,
                access_key: if is_admin {
                    Some(broker.access_key)
                } else {
                    None
                },
                secret_key: if is_admin {
                    Some(broker.secret_key)
                } else {
                    None
                },
                is_aws_region: broker.is_aws_region,
                remove_duplicate_files: broker.remove_duplicate_files,
                owner,
                creation_timestamp: broker.creation_timestamp,
                hls_enabled: broker.hls_enabled,
                enable_presigned_get: broker.enable_presigned_get,
                is_system_bucket: broker.is_system_bucket,
                total_quota: broker.total_quota,
                disable_uploads: broker.disable_uploads,
                is_public,
                is_admin,
                used_bytes,
                used_quota,
                quota_bytes,
                total_used_bytes,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&broker_detailed))
}

#[derive(Deserialize, Validate)]
pub struct GetBrokersParams {
    #[validate(range(min = 1, max = 50))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
    pub ordering: Option<String>,
    pub admin_only: Option<bool>,
}

#[derive(Serialize)]
pub struct GetBrokersResponse {
    pub total_count: i64,
    pub brokers: Vec<BrokerDetailed>,
}

pub async fn get_brokers_handler(
    params: GetBrokersParams,
    user: User,
) -> Result<impl Reply, Rejection> {
    params.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for GetBrokersParams: {e}"
        )))
    })?;
    let admin_only = params.admin_only.unwrap_or(false);

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            let filter = admin_only
                .into_sql::<Bool>()
                .and(get_broker_write_condition!(Some(user.pk)))
                .or(not(admin_only.into_sql::<Bool>()).and(
                    user.is_admin
                        .into_sql::<Bool>()
                        .or(broker::fk_owner.eq(user.pk))
                        .or(exists(
                            broker_access::table.filter(get_broker_access_condition!(user.pk)),
                        )),
                ));

            let total_count = broker::table
                .filter(filter)
                .count()
                .get_result::<i64>(connection)
                .await?;

            let broker_query = broker::table
                .inner_join(registered_user::table.on(broker::fk_owner.eq(registered_user::pk)))
                .filter(filter);

            let mut order_map: HashMap<&'static str, Box<ApplyOrderFn<_, _, _>>> = HashMap::new();
            order_map.insert(
                "name",
                order_by_col_with_tie_fn(broker::name, broker::pk.desc()),
            );
            order_map.insert(
                "creation_timestamp",
                order_by_col_with_tie_fn(broker::creation_timestamp, broker::pk.desc()),
            );
            order_map.insert(
                "owner",
                Box::new(move |desc, q: BoxedSelectStatement<'_, _, _, _>| {
                    if desc {
                        q.order((registered_user::pk.desc(), broker::pk.desc()))
                    } else {
                        q.order((registered_user::pk.asc(), broker::pk.desc()))
                    }
                }),
            );

            let broker_query_ordered = apply_key_ordering(
                params.ordering,
                ("name", false),
                broker_query.into_boxed(),
                order_map,
            )?;

            let limit = params.limit.unwrap_or(50);
            let records = broker_query_ordered
                .limit(limit as i64)
                .offset((params.page.unwrap_or(0) * limit) as i64)
                .load::<(Broker, UserPublic)>(connection)
                .await?;
            let broker_pks = records.iter().map(|(b, _)| b.pk).collect::<Vec<i64>>();

            let public_brokers = broker::table
                .select(broker::pk)
                .filter(
                    broker::pk.eq_any(&broker_pks).and(exists(
                        broker_access::table.filter(
                            broker_access::fk_broker
                                .eq(broker::pk)
                                .and(broker_access::fk_granted_group.is_null()),
                        ),
                    )),
                )
                .load::<i64>(connection)
                .await?;
            let admin_brokers = broker::table
                .select(broker::pk)
                .filter(
                    broker::pk
                        .eq_any(&broker_pks)
                        .and(get_broker_write_condition!(Some(user.pk))),
                )
                .load::<i64>(connection)
                .await?;

            let BrokerUsageInformation {
                broker_quota_usages,
                unlimited_access_brokers,
                broker_quota_limits,
            } = load_broker_quota_usages(&broker_pks, &user, connection).await?;

            let brokers = records
                .into_iter()
                .map(|(broker, owner)| {
                    let is_admin = admin_brokers.contains(&broker.pk);
                    let is_public = public_brokers.contains(&broker.pk);
                    let BrokerQuotaUsage {
                        used_bytes,
                        used_quota,
                    } = broker_quota_usages
                        .get(&broker.pk)
                        .copied()
                        .unwrap_or_default();
                    let quota_bytes = if unlimited_access_brokers.contains(&broker.pk) {
                        None
                    } else {
                        broker_quota_limits.get(&broker.pk).cloned().flatten()
                    };

                    Ok(BrokerDetailed {
                        pk: broker.pk,
                        name: broker.name,
                        description: broker.description,
                        bucket: broker.bucket,
                        endpoint: broker.endpoint,
                        access_key: if is_admin {
                            Some(broker.access_key)
                        } else {
                            None
                        },
                        secret_key: if is_admin {
                            Some(broker.secret_key)
                        } else {
                            None
                        },
                        is_aws_region: broker.is_aws_region,
                        remove_duplicate_files: broker.remove_duplicate_files,
                        owner,
                        creation_timestamp: broker.creation_timestamp,
                        hls_enabled: broker.hls_enabled,
                        enable_presigned_get: broker.enable_presigned_get,
                        is_system_bucket: broker.is_system_bucket,
                        total_quota: broker.total_quota,
                        disable_uploads: broker.disable_uploads,
                        is_public,
                        is_admin,
                        used_bytes,
                        used_quota,
                        quota_bytes,
                        total_used_bytes: None,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            Ok(GetBrokersResponse {
                total_count,
                brokers,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}

#[derive(Serialize)]
pub struct BrokerAvailability {
    pub broker: Broker,
    pub used_bytes: i64,
    pub used_quota: i64,
    pub quota_bytes: Option<i64>,
}

pub async fn get_available_brokers_handler(user: User) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let brokers = perms::get_available_brokers_secured(&mut connection, Some(&user)).await?;
    let broker_pks = brokers.iter().map(|b| b.pk).collect::<Vec<i64>>();

    let BrokerUsageInformation {
        broker_quota_usages,
        unlimited_access_brokers,
        broker_quota_limits,
    } = load_broker_quota_usages(&broker_pks, &user, &mut connection).await?;

    let mut broker_availabilities = Vec::new();
    for broker in brokers {
        let BrokerQuotaUsage {
            used_bytes,
            used_quota,
        } = broker_quota_usages
            .get(&broker.pk)
            .copied()
            .unwrap_or_default();
        let quota_bytes = if unlimited_access_brokers.contains(&broker.pk) {
            None
        } else {
            broker_quota_limits.get(&broker.pk).cloned().flatten()
        };

        broker_availabilities.push(BrokerAvailability {
            broker,
            used_bytes,
            used_quota,
            quota_bytes,
        });
    }

    Ok(warp::reply::json(&broker_availabilities))
}

struct BrokerUsageInformation {
    broker_quota_usages: HashMap<i64, BrokerQuotaUsage>,
    unlimited_access_brokers: HashSet<i64>,
    broker_quota_limits: HashMap<i64, Option<i64>>,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct BrokerQuotaUsage {
    /// total bytes used by user on the broker, including derived system data like thumbs and HLS segments
    pub used_bytes: i64,
    /// total quota relevant bytes used by user on the broker, only including direct uploads
    pub used_quota: i64,
}

async fn load_broker_quota_usages(
    broker_pks: &[i64],
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<BrokerUsageInformation, Error> {
    let broker_quota_usages = s3_object::table
        .group_by(s3_object::fk_broker)
        .select((
            s3_object::fk_broker,
            sum(s3_object::size_bytes),
            sum(case_when(
                s3_object::object_type.eq(ObjectType::Original),
                s3_object::size_bytes,
            )
            .otherwise(0)),
        ))
        .filter(
            s3_object::fk_uploader
                .eq(user.pk)
                .and(s3_object::fk_broker.eq_any(broker_pks)),
        )
        .load::<(i64, Option<BigDecimal>, Option<BigDecimal>)>(connection)
        .await
        .map_err(Error::from)?
        .into_iter()
        .map(|(broker_pk, size_used, quota_used)| {
            let size_used = size_used.unwrap_or_default();
            let used_bytes = size_used.to_i64().ok_or_else(|| {
                Error::InternalError(format!(
                    "Could not convert broker usage bytes {size_used} to i64"
                ))
            })?;

            let quota_used = quota_used.unwrap_or_default();
            let used_quota = quota_used.to_i64().ok_or_else(|| {
                Error::InternalError(format!(
                    "Could not convert broker quota bytes {quota_used} to i64"
                ))
            })?;

            Ok((
                broker_pk,
                BrokerQuotaUsage {
                    used_bytes,
                    used_quota,
                },
            ))
        })
        .collect::<Result<HashMap<i64, BrokerQuotaUsage>, Error>>()?;

    let unlimited_access_brokers =
        broker::table
            .select(broker::pk)
            .filter(
                broker::pk
                    .eq_any(broker_pks)
                    .and(
                        broker::fk_owner.eq(user.pk).or(exists(
                            broker_access::table.filter(
                                get_broker_access_condition!(user.pk)
                                    .and(broker_access::quota.is_null()),
                            ),
                        )),
                    ),
            )
            .load::<i64>(connection)
            .await
            .map_err(Error::from)?
            .into_iter()
            .collect::<HashSet<i64>>();

    let broker_quota_limits = broker::table
        .inner_join(
            broker_access::table
                .on(get_broker_access_condition!(user.pk).and(broker_access::quota.is_not_null())),
        )
        .group_by(broker::pk)
        .select((broker::pk, max(broker_access::quota)))
        .filter(
            broker::pk
                .eq_any(broker_pks)
                .and(not(broker::pk.eq_any(&unlimited_access_brokers))),
        )
        .load::<(i64, Option<i64>)>(connection)
        .await
        .map_err(Error::from)?
        .into_iter()
        .collect::<HashMap<i64, Option<i64>>>();

    Ok(BrokerUsageInformation {
        broker_quota_usages,
        unlimited_access_brokers,
        broker_quota_limits,
    })
}

#[derive(Serialize)]
pub struct VerifyBucketConnectionResponse {
    pub is_valid: bool,
    pub error_message: Option<String>,
}

pub async fn verify_bucket_connection_handler(
    broker_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let BrokerJoined { broker, .. } = get_broker_joined(broker_pk, &user, &mut connection).await?;
    drop(connection);

    let result = {
        let bucket = create_bucket(
            &broker.bucket,
            &broker.endpoint,
            &broker.access_key,
            &broker.secret_key,
            broker.is_aws_region,
        )?;

        verify_bucket_connection(&bucket).await
    };

    Ok(warp::reply::json(&VerifyBucketConnectionResponse {
        is_valid: result.is_ok(),
        error_message: result.err().map(|e| e.to_string()),
    }))
}

pub async fn verify_bucket_connection(bucket: &Bucket) -> Result<(), Error> {
    if let Err(e) = Url::parse(&bucket.url()) {
        return Err(Error::InvalidBucketError(e.to_string()));
    }

    // test connection
    let mut test_path = Uuid::new_v4().to_string();
    test_path.insert_str(0, ".filebroker-test-");
    if let Err(e) = bucket.put_object(&test_path, &[]).await {
        return Err(Error::InvalidBucketError(e.to_string()));
    }
    if let Err(e) = bucket.get_object(&test_path).await {
        return Err(Error::InvalidBucketError(e.to_string()));
    };
    if let Err(e) = bucket.delete_object(&test_path).await {
        return Err(Error::InvalidBucketError(e.to_string()));
    }

    Ok(())
}

#[derive(Debug, QueryableByName)]
pub struct BrokerAccessUsage {
    #[diesel(sql_type = BigInt)]
    broker_access_pk: i64,

    #[diesel(sql_type = Nullable<Numeric>)]
    used_bytes: Option<BigDecimal>,

    #[diesel(sql_type = Nullable<Numeric>)]
    used_quota: Option<BigDecimal>,
}

#[derive(Deserialize, Validate)]
pub struct GetBrokerAccessParams {
    #[validate(range(min = 1, max = 15))]
    pub limit: Option<u32>,
    #[validate(range(min = 0, max = 1000))]
    pub page: Option<u32>,
    pub ordering: Option<String>,
}

#[derive(Serialize)]
pub struct BrokerAccessInnerJoined {
    pub pk: i64,
    pub granted_group: Option<UserGroup>,
    pub write: bool,
    pub quota: Option<i64>,
    pub used_bytes: i64,
    pub used_quota: i64,
    pub granted_by: UserPublic,
    pub creation_timestamp: DateTime<Utc>,
    pub granted_user: Option<UserPublic>,
    #[serde(rename = "is_public")]
    pub public: bool,
}

#[derive(Serialize)]
pub struct GetBrokerAccessResponse {
    pub total_count: i64,
    pub broker_access: Vec<BrokerAccessInnerJoined>,
}

pub async fn get_broker_access_handler(
    broker_pk: i64,
    params: GetBrokerAccessParams,
    user: User,
) -> Result<impl Reply, Rejection> {
    params.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for GetBrokerAccessParams: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let response = run_repeatable_read_transaction(&mut connection, |connection| {
        async {
            let BrokerJoined { broker, owner } =
                get_broker_joined(broker_pk, &user, connection).await?;
            // only allow broker admins to view broker access
            if !(owner.pk == user.pk || is_broker_admin(broker_pk, &user, connection).await?) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(broker_pk),
                ));
            }

            let total_count = broker_access::table
                .filter(broker_access::fk_broker.eq(broker.pk))
                .count()
                .get_result::<i64>(connection)
                .await?;

            let (granted_user, granted_by_user) = diesel::alias!(
                registered_user as granted_user,
                registered_user as granted_by_user
            );
            let broker_query = broker_access::table
                .left_join(user_group::table)
                .left_join(
                    granted_user.on(broker_access::fk_granted_user
                        .eq(granted_user.field(registered_user::pk).nullable())),
                )
                .inner_join(granted_by_user.on(
                    broker_access::fk_granted_by.eq(granted_by_user.field(registered_user::pk)),
                ))
                .filter(broker_access::fk_broker.eq(broker.pk));

            let mut order_map: HashMap<&'static str, Box<ApplyOrderFn<_, _, _>>> = HashMap::new();
            order_map.insert(
                "user_group.name",
                order_by_col_with_tie_fn(user_group::name, user_group::pk.desc()),
            );
            order_map.insert(
                "granted_user",
                order_by_col_with_tie_fn(
                    granted_user.field(registered_user::pk),
                    broker_access::pk.desc(),
                ),
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
                order_by_col_with_tie_fn(
                    granted_by_user.field(registered_user::pk),
                    broker_access::pk.desc(),
                ),
            );
            order_map.insert("user_group.pk", order_by_col_fn(user_group::pk));
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
                .load::<(
                    BrokerAccess,
                    Option<UserGroup>,
                    Option<UserPublic>,
                    UserPublic,
                )>(connection)
                .await?;

            let broker_access_pks = records
                .iter()
                .map(|(broker_access, ..)| broker_access.pk)
                .collect::<Vec<_>>();
            let broker_access_usages = diesel::sql_query(
                r#"
                WITH user_applicable_access AS (
                    SELECT
                        so.fk_uploader        AS user_pk,
                        ba.pk                 AS broker_access_pk,
                        ROW_NUMBER() OVER (
                            PARTITION BY so.fk_uploader
                            ORDER BY
                                ba.fk_granted_user ASC NULLS LAST,
                                ba.quota DESC NULLS FIRST
                        ) AS rn
                    FROM s3_object so

                    JOIN broker_access ba
                      ON ba.fk_broker = so.fk_broker

                    LEFT JOIN user_group_membership ugm
                      ON ugm.fk_group = ba.fk_granted_group
                     AND ugm.fk_user = so.fk_uploader
                     AND NOT ugm.revoked

                    WHERE so.fk_broker = $1
                      AND ba.pk = ANY($2)
                      AND so.fk_uploader <> $3

                      AND (
                           ba.public
                        OR ba.fk_granted_user = so.fk_uploader
                        OR ugm.fk_user IS NOT NULL
                      )
                ),

                resolved_access AS (
                    SELECT
                        user_pk,
                        broker_access_pk
                    FROM user_applicable_access
                    WHERE rn = 1
                )

                SELECT
                    ra.broker_access_pk,
                    SUM(so.size_bytes) AS used_bytes,
                    SUM(so.size_bytes) FILTER (WHERE so.object_type = 'original') AS used_quota
                FROM resolved_access ra
                JOIN s3_object so
                  ON so.fk_uploader = ra.user_pk
                 AND so.fk_broker   = $1
                WHERE so.fk_uploader <> $3
                GROUP BY ra.broker_access_pk;
                "#,
            )
            .bind::<BigInt, _>(broker.pk)
            .bind::<Array<BigInt>, _>(broker_access_pks)
            .bind::<BigInt, _>(owner.pk)
            .load::<BrokerAccessUsage>(connection)
            .await?
            .into_iter()
            .map(|broker_access_usage| {
                let size_used = broker_access_usage.used_bytes.unwrap_or_default();
                let used_bytes = size_used.to_i64().ok_or_else(|| {
                    Error::InternalError(format!(
                        "Could not convert broker access usage bytes {size_used} to i64"
                    ))
                })?;

                let quota_used = broker_access_usage.used_quota.unwrap_or_default();
                let used_quota = quota_used.to_i64().ok_or_else(|| {
                    Error::InternalError(format!(
                        "Could not convert broker access quota bytes {quota_used} to i64"
                    ))
                })?;

                Ok((
                    broker_access_usage.broker_access_pk,
                    BrokerQuotaUsage {
                        used_bytes,
                        used_quota,
                    },
                ))
            })
            .collect::<Result<HashMap<i64, BrokerQuotaUsage>, Error>>()?;

            let broker_access = records
                .into_iter()
                .map(|(broker_access, granted_group, granted_user, granted_by)| {
                    let BrokerQuotaUsage {
                        used_bytes,
                        used_quota,
                    } = broker_access_usages
                        .get(&broker_access.pk)
                        .copied()
                        .unwrap_or_default();

                    Ok(BrokerAccessInnerJoined {
                        pk: broker_access.pk,
                        granted_group,
                        write: broker_access.write,
                        quota: broker_access.quota,
                        used_bytes,
                        used_quota,
                        granted_by,
                        creation_timestamp: broker_access.creation_timestamp,
                        granted_user,
                        public: broker_access.public,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            Ok(GetBrokerAccessResponse {
                total_count,
                broker_access,
            })
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&response))
}
