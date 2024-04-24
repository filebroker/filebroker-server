use std::{ffi::OsStr, io, path::Path};

use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::Utc;
use diesel::{
    dsl::{exists, not, sum},
    NullableExpressionMethods, OptionalExtension, PgSortExpressionMethods, QueryDsl,
};
use diesel_async::{scoped_futures::ScopedFutureExt, AsyncPgConnection, RunQueryDsl};
use futures::{stream::IntoAsyncRead, TryStream};
use s3::{error::S3Error, Bucket};
use uuid::Uuid;

use crate::{
    acquire_db_connection,
    data::encode::{generate_hls_playlist, generate_thumbnail},
    diesel::{BoolExpressionMethods, ExpressionMethods},
    error::Error,
    model::{Broker, BrokerAccess, S3Object, User},
    perms::{get_group_access_or_public_condition, get_group_membership_condition},
    run_serializable_transaction,
    schema::{broker_access, s3_object, user_group, user_group_membership},
};

use super::s3utils;

pub async fn upload_file<S>(
    broker: &Broker,
    user: &User,
    bucket: &Bucket,
    mut reader: s3utils::FileReader<IntoAsyncRead<S>>,
    content_type: String,
    filename: String,
    disable_hls_transcoding: Option<bool>,
) -> Result<(S3Object, bool), Error>
where
    S: TryStream<Error = std::io::Error> + Unpin,
    S::Ok: AsRef<[u8]>,
{
    let uuid = Uuid::new_v4();
    let object_key =
        if let Some(extension) = Path::new(&filename).extension().and_then(OsStr::to_str) {
            format!("{}.{}", &uuid, extension)
        } else {
            uuid.to_string()
        };

    log::info!("Starting S3 upload for {}", &object_key);
    let status = match bucket
        .put_object_stream_with_content_type(&mut reader, &object_key, &content_type)
        .await
    {
        Ok(status) => Ok(status),
        Err(S3Error::Io(e)) if e.kind() == io::ErrorKind::InvalidInput => {
            log::warn!("Failed upload for object {} because it does not match the provided Filebroker-Upload-Size header", &object_key);
            Err(Error::InvalidUploadSizeError)
        }
        Err(e) => Err(e.into()),
    }?;
    if status >= 300 {
        return Err(Error::S3ResponseError(status));
    }
    log::info!("Finished S3 upload for {}", &object_key);

    let digest = reader.hasher.finish();
    let hash = data_encoding::HEXUPPER.encode(digest.as_ref());

    let mut connection = acquire_db_connection().await?;

    if broker.remove_duplicate_files {
        let existing_object = s3_object::table
            .filter(
                s3_object::sha256_hash
                    .eq(&hash)
                    .and(s3_object::fk_broker.eq(broker.pk)),
            )
            .limit(1)
            .get_result::<S3Object>(&mut connection)
            .await
            .optional()
            .map_err(|e| Error::QueryError(e.to_string()))?;

        if let Some(existing_object) = existing_object {
            // don't hold on to db connection while waiting for deletion
            drop(connection);
            log::info!("Found existing object {} with same hash as new object {}, going to delete new object", &existing_object.object_key, &object_key);
            match bucket.delete_object(&object_key).await {
                Ok(delete_response) => {
                    let status_code = delete_response.status_code();
                    if status_code < 300 {
                        // existing object found and deletion of new object succeeded -> return existing object
                        return Ok((existing_object, true));
                    } else {
                        log::error!(
                            "Deleting object {} for existing hash {} failed with status code {}. Going to use new object.",
                            &object_key,
                            &hash,
                            status_code
                        );
                    }
                }
                Err(e) => {
                    log::error!(
                        "Deleting object {} for existing hash {} failed with error {}. Going to use new object.",
                        &object_key,
                        &hash,
                        &e
                    );
                }
            }

            connection = acquire_db_connection().await?;
        }
    }

    let is_video = content_type.starts_with("video/");
    let broker_hls_enabled = broker.hls_enabled;
    let hls_transcoding_disabled = disable_hls_transcoding.unwrap_or(false);

    let source_filename = if filename.len() > 255 {
        None
    } else {
        Some(filename)
    };

    let mime_type = content_type.clone();
    let inserted_s3_object = run_serializable_transaction(&mut connection, |connection| {
        let object_key = object_key.clone();
        async move {
            if broker.fk_owner != user.pk {
                check_broker_quota_usage(broker, user, reader.file_size, connection).await?;
            }

            let inserted_object = diesel::insert_into(s3_object::table)
                .values(&S3Object {
                    object_key,
                    sha256_hash: Some(hash),
                    size_bytes: reader.file_size as i64,
                    mime_type,
                    fk_broker: broker.pk,
                    fk_uploader: user.pk,
                    thumbnail_object_key: None,
                    creation_timestamp: Utc::now(),
                    filename: source_filename,
                    hls_master_playlist: None,
                    hls_disabled: hls_transcoding_disabled,
                    hls_locked_at: None,
                    thumbnail_locked_at: None,
                    hls_fail_count: None,
                    thumbnail_fail_count: None,
                    thumbnail_disabled: false,
                })
                .get_result::<S3Object>(connection)
                .await
                .map_err(|e| Error::QueryError(e.to_string()))?;

            Ok(inserted_object)
        }
        .scope_boxed()
    })
    .await;

    let s3_object = match inserted_s3_object {
        Ok(s3_object) => s3_object,
        Err(Error::QuotaExceededError(quota, remaining_quota)) => {
            log::warn!("User {} exceeded quota for broker {} after completed upload of {}, going to delete object", user.pk, broker.pk, &object_key);
            match bucket.delete_object(&object_key).await {
                Ok(delete_response) => {
                    let status_code = delete_response.status_code();
                    if status_code >= 300 {
                        log::error!(
                            "Deleting object {} for user {} failed with status code {}",
                            &object_key,
                            user.pk,
                            status_code
                        );
                    }
                }
                Err(e) => {
                    log::error!(
                        "Deleting object {} for user {} failed with error {}",
                        &object_key,
                        user.pk,
                        &e
                    );
                }
            }
            return Err(Error::QuotaExceededError(quota, remaining_quota));
        }
        Err(e) => return Err(e),
    };

    let path = s3_object.object_key.clone();
    let bucket_owned = bucket.clone();
    let broker_owned = broker.clone();
    let user_owned = user.clone();
    tokio::spawn(async move {
        if let Err(e) = generate_thumbnail(
            bucket_owned,
            path,
            uuid,
            content_type,
            broker_owned,
            user_owned,
            false,
        )
        .await
        {
            log::error!("Failed to generate thumbnail: {}", e);
        }
    });

    let path = s3_object.object_key.clone();
    let bucket_owned = bucket.clone();
    let broker_owned = broker.clone();
    let user_owned = user.clone();
    if is_video && broker_hls_enabled && !hls_transcoding_disabled {
        tokio::spawn(async move {
            if let Err(e) =
                generate_hls_playlist(bucket_owned, path, uuid, broker_owned, user_owned, false)
                    .await
            {
                log::error!("Error occurred transcoding video to HLS: {}", e);
            }
        });
    }

    Ok((s3_object, false))
}

pub async fn check_broker_quota_usage(
    broker: &Broker,
    user: &User,
    upload_size: usize,
    connection: &mut AsyncPgConnection,
) -> Result<(), Error> {
    let access: BrokerAccess =
        broker_access::table
            .filter(broker_access::fk_broker.eq(broker.pk).and(
                get_group_access_or_public_condition!(
                    broker_access::fk_broker,
                    broker.pk,
                    &Some(user.pk),
                    broker_access::fk_granted_group.is_null(),
                    broker_access::fk_granted_group
                ),
            ))
            .order(broker_access::quota.desc().nulls_first())
            .get_result::<BrokerAccess>(connection)
            .await
            .optional()
            .map_err(Error::from)?
            .ok_or(Error::InaccessibleObjectError(broker.pk))?;

    if let Some(quota) = access.quota {
        let used_quota: BigDecimal = s3_object::table
            .select(sum(s3_object::size_bytes))
            .filter(
                s3_object::fk_broker
                    .eq(broker.pk)
                    .and(s3_object::fk_uploader.eq(user.pk)),
            )
            .get_result::<Option<BigDecimal>>(connection)
            .await
            .map_err(Error::from)?
            .unwrap_or_default();

        if used_quota
            .to_u128()
            .ok_or(Error::InternalError(String::from(
                "Used quota cannot be converted to u128",
            )))?
            + upload_size as u128
            > quota as u128
        {
            return Err(Error::QuotaExceededError(
                quota,
                quota - used_quota.to_i64().unwrap_or(0),
            ));
        }
    }

    Ok(())
}
