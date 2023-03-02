use std::{ffi::OsStr, path::Path};

use chrono::Utc;
use diesel::{OptionalExtension, QueryDsl};
use futures::{stream::IntoAsyncRead, TryStream};
use s3::Bucket;
use uuid::Uuid;

use crate::{
    acquire_db_connection,
    data::encode::generate_thumbnail,
    diesel::{BoolExpressionMethods, ExpressionMethods, RunQueryDsl},
    error::Error,
    model::{Broker, S3Object, User},
    schema::s3_object,
};

use super::s3utils;

pub async fn upload_file<S>(
    broker: &Broker,
    user: &User,
    bucket: &Bucket,
    mut reader: s3utils::FileReader<IntoAsyncRead<S>>,
    content_type: String,
    filename: String,
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
    let status = bucket.put_object_stream(&mut reader, &object_key).await?;
    if status >= 300 {
        return Err(Error::S3ResponseError(status));
    }
    log::info!("Finished S3 upload for {}", &object_key);

    let digest = reader.hasher.finish();
    let hash = data_encoding::HEXUPPER.encode(digest.as_ref());

    let mut connection = acquire_db_connection()?;

    if broker.remove_duplicate_files {
        let existing_object = s3_object::table
            .filter(
                s3_object::sha256_hash
                    .eq(&hash)
                    .and(s3_object::fk_broker.eq(broker.pk)),
            )
            .limit(1)
            .get_result::<S3Object>(&mut connection)
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

            connection = acquire_db_connection()?;
        }
    }

    let path = object_key.clone();
    let mime_type = content_type.clone();
    let bucket = bucket.clone();
    let broker_owned = broker.clone();
    let user_owned = user.clone();
    tokio::spawn(async move {
        if let Err(e) =
            generate_thumbnail(bucket, path, uuid, content_type, broker_owned, user_owned).await
        {
            log::error!("Failed to generate thumbnail: {}", e);
        }
    });

    let source_filename = if filename.len() > 255 {
        None
    } else {
        Some(filename)
    };

    let s3_object = diesel::insert_into(s3_object::table)
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
        })
        .get_result::<S3Object>(&mut connection)
        .map_err(|e| Error::QueryError(e.to_string()))?;

    Ok((s3_object, false))
}
