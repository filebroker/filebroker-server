use crate::diesel::ExpressionMethods;
use crate::diesel::NullableExpressionMethods;
use crate::error::Error;
use crate::model::{Broker, User, UserPublic};
use crate::perms::get_broker_group_access_write_condition;
use crate::perms::{
    get_broker_access_write_condition, get_group_access_or_public_condition,
    get_group_membership_administrator_condition, get_group_membership_condition,
};
use crate::query::{ApplyOrderFn, apply_key_ordering, order_by_col_with_tie_fn};
use crate::schema::{
    broker, broker_access, registered_user, s3_object, user_group, user_group_membership,
};
use crate::{acquire_db_connection, perms, run_repeatable_read_transaction};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, Utc};
use diesel::dsl::{exists, max, not, sum};
use diesel::query_builder::BoxedSelectStatement;
use diesel::sql_types::Bool;
use diesel::{BoolExpressionMethods, IntoSql, JoinOnDsl, QueryDsl};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use validator::Validate;
use warp::{Rejection, Reply};

pub mod create;

#[derive(Serialize)]
pub struct BrokerDetailed {
    pub pk: i64,
    pub name: String,
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
    pub is_public: bool,
    pub is_admin: bool,
    pub used_bytes: i64,
    pub quota_bytes: Option<i64>,
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
                .and(get_broker_access_write_condition!(Some(user.pk)))
                .or(not(admin_only.into_sql::<Bool>()).and(
                    user.is_admin
                        .into_sql::<Bool>()
                        .or(broker::fk_owner.eq(user.pk))
                        .or(exists(broker_access::table.filter(
                            get_group_access_or_public_condition!(
                                broker_access::fk_broker,
                                broker::pk,
                                Some(user.pk),
                                broker_access::fk_granted_group.is_null(),
                                broker_access::fk_granted_group
                            ),
                        ))),
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
                        .and(get_broker_access_write_condition!(Some(user.pk))),
                )
                .load::<i64>(connection)
                .await?;

            let (broker_usages, unlimited_brokers, broker_quotas) =
                load_broker_quota_usages(&broker_pks, &user, connection).await?;

            let brokers = records
                .into_iter()
                .map(|(broker, owner)| {
                    let is_admin = admin_brokers.contains(&broker.pk);
                    let is_public = public_brokers.contains(&broker.pk);
                    let used_bytes = broker_usages
                        .get(&broker.pk)
                        .map(|usage_bytes| {
                            usage_bytes.to_i64().ok_or_else(|| {
                                Error::InternalError(format!(
                                    "Could not convert broker usage bytes {usage_bytes} to i64"
                                ))
                            })
                        })
                        .unwrap_or(Ok(0))?;
                    let quota_bytes = if unlimited_brokers.contains(&broker.pk) {
                        None
                    } else {
                        broker_quotas.get(&broker.pk).cloned().flatten()
                    };

                    Ok(BrokerDetailed {
                        pk: broker.pk,
                        name: broker.name,
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
                        is_public,
                        is_admin,
                        used_bytes,
                        quota_bytes,
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
    pub quota_bytes: Option<i64>,
}

pub async fn get_available_brokers_handler(user: User) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let brokers = perms::get_brokers_secured(&mut connection, Some(&user)).await?;
    let broker_pks = brokers.iter().map(|b| b.pk).collect::<Vec<i64>>();

    let (broker_usages, unlimited_brokers, broker_quotas) =
        load_broker_quota_usages(&broker_pks, &user, &mut connection).await?;

    let mut broker_availabilities = Vec::new();
    for broker in brokers {
        let used_bytes = broker_usages
            .get(&broker.pk)
            .map(|usage_bytes| {
                usage_bytes.to_i64().ok_or_else(|| {
                    Error::InternalError(format!(
                        "Could not convert broker usage bytes {usage_bytes} to i64"
                    ))
                })
            })
            .unwrap_or(Ok(0))?;
        let quota_bytes = if unlimited_brokers.contains(&broker.pk) {
            None
        } else {
            broker_quotas.get(&broker.pk).cloned().flatten()
        };

        broker_availabilities.push(BrokerAvailability {
            broker,
            used_bytes,
            quota_bytes,
        });
    }

    Ok(warp::reply::json(&broker_availabilities))
}

async fn load_broker_quota_usages(
    broker_pks: &[i64],
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<
    (
        HashMap<i64, BigDecimal>,
        HashSet<i64>,
        HashMap<i64, Option<i64>>,
    ),
    Error,
> {
    let broker_usages = s3_object::table
        .group_by(s3_object::fk_broker)
        .select((s3_object::fk_broker, sum(s3_object::size_bytes)))
        .filter(
            s3_object::fk_uploader
                .eq(user.pk)
                .and(s3_object::fk_broker.eq_any(broker_pks)),
        )
        .load::<(i64, Option<BigDecimal>)>(connection)
        .await
        .map_err(Error::from)?
        .into_iter()
        .map(|(broker_pk, size_used)| (broker_pk, size_used.unwrap_or(BigDecimal::from(0))))
        .collect::<HashMap<i64, BigDecimal>>();

    let unlimited_brokers = broker::table
        .select(broker::pk)
        .filter(
            broker::pk.eq_any(broker_pks).and(
                broker::fk_owner.eq(user.pk).or(exists(
                    broker_access::table.filter(
                        get_group_access_or_public_condition!(
                            broker_access::fk_broker,
                            broker::pk,
                            &Some(user.pk),
                            broker_access::fk_granted_group.is_null(),
                            broker_access::fk_granted_group
                        )
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

    let broker_quotas = broker::table
        .inner_join(
            broker_access::table.on(get_group_access_or_public_condition!(
                broker_access::fk_broker,
                broker::pk,
                &Some(user.pk),
                broker_access::fk_granted_group.is_null(),
                broker_access::fk_granted_group
            )
            .and(broker_access::quota.is_not_null())),
        )
        .group_by(broker::pk)
        .select((broker::pk, max(broker_access::quota)))
        .filter(
            broker::pk
                .eq_any(broker_pks)
                .and(not(broker::pk.eq_any(&unlimited_brokers))),
        )
        .load::<(i64, Option<i64>)>(connection)
        .await
        .map_err(Error::from)?
        .into_iter()
        .collect::<HashMap<i64, Option<i64>>>();

    Ok((broker_usages, unlimited_brokers, broker_quotas))
}
