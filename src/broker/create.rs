use crate::data::create_bucket;
use crate::diesel::ExpressionMethods;
use crate::error::Error;
use crate::model::{Broker, NewBroker, User};
use crate::schema::broker;
use crate::util::NOT_BLANK_REGEX;
use crate::{acquire_db_connection, run_serializable_transaction};
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use serde::Deserialize;
use url::Url;
use uuid::Uuid;
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

    if let Err(e) = Url::parse(&bucket.url()) {
        return Err(warp::reject::custom(Error::InvalidBucketError(
            e.to_string(),
        )));
    }

    // test connection
    let mut test_path = Uuid::new_v4().to_string();
    test_path.insert_str(0, ".filebroker-test-");
    if let Err(e) = bucket.put_object(&test_path, &[]).await {
        return Err(warp::reject::custom(Error::InvalidBucketError(
            e.to_string(),
        )));
    }
    if let Err(e) = bucket.delete_object(&test_path).await {
        return Err(warp::reject::custom(Error::InvalidBucketError(
            e.to_string(),
        )));
    }

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
