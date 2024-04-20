WITH cte1 AS (
    SELECT unnest(
        tag.pk
        || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
        || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
        || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
    ) AS tag_keys
    FROM tag WHERE lower(tag.tag_name) = 'video'
), cte0 AS (
    SELECT unnest(
        tag.pk
        || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
        || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
        || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
    ) AS tag_keys
    FROM tag WHERE lower(tag.tag_name) = 'masseffect'
), countCte AS (
    SELECT CASE 
    WHEN (SELECT COUNT(*) FROM (SELECT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user WHERE EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte0)) AND EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte1)) LIMIT 100000) limitedPks) < 100000 
    THEN (SELECT COUNT(*) FROM (SELECT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user WHERE EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte0)) AND EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte1)) AND
    (post.fk_create_user = 1
    OR (post.public AND TRUE)
    OR EXISTS(
        SELECT * FROM post_group_access
        WHERE post_group_access.fk_post = post.pk
        AND TRUE
        AND post_group_access.fk_granted_group IN(
            SELECT pk FROM user_group
            WHERE fk_owner = 1
            OR EXISTS(
                SELECT * FROM user_group_membership
                WHERE NOT revoked AND fk_user = 1 AND fk_group = user_group.pk
            )
        )
    ))) pks) END AS full_count
) 
SELECT post.pk AS post_pk, post.data_url AS post_data_url, post.source_url AS post_source_url, post.title AS post_title, post.creation_timestamp AS post_creation_timestamp, post.score AS post_score, post.thumbnail_url AS post_thumbnail_url, post.public AS post_public, post.public_edit AS post_public_edit, post.description AS post_description, s3_object.thumbnail_object_key AS post_thumbnail_object_key, create_user.pk AS post_create_user_pk, create_user.user_name AS post_create_user_user_name, create_user.user_name AS post_create_user_user_name, create_user.email AS post_create_user_email, create_user.avatar_url AS post_create_user_avatar_url, create_user.creation_timestamp AS post_create_user_creation_timestamp, create_user.email_confirmed AS post_create_user_email_confirmed, create_user.display_name AS post_create_user_display_name, create_user.password_fail_count AS post_create_user_password_fail_count, s3_object.object_key AS post_s3_object_object_key, s3_object.sha256_hash AS post_s3_object_sha256_hash, s3_object.size_bytes AS post_s3_object_size_bytes, s3_object.mime_type AS post_s3_object_mime_type, s3_object.fk_broker AS post_s3_object_fk_broker, s3_object.fk_uploader AS post_s3_object_fk_uploader, s3_object.thumbnail_object_key AS post_s3_object_thumbnail_object_key, s3_object.creation_timestamp AS post_s3_object_creation_timestamp, s3_object.filename AS post_s3_object_filename, s3_object.hls_master_playlist AS post_s3_object_hls_master_playlist, s3_object.hls_disabled AS post_s3_object_hls_disabled, s3_object.hls_locked_at AS post_s3_object_hls_locked_at, s3_object.thumbnail_locked_at AS post_s3_object_thumbnail_locked_at, s3_object.hls_fail_count AS post_s3_object_hls_fail_count, s3_object.thumbnail_fail_count AS post_s3_object_thumbnail_fail_count, s3_object.thumbnail_disabled AS post_s3_object_thumbnail_disabled, (SELECT full_count FROM countCte), 50 AS evaluated_limit 
FROM post 
LEFT JOIN s3_object ON s3_object.object_key = post.s3_object 
INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user 
WHERE EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte0)) AND EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte1)) AND
(post.fk_create_user = 1
OR (post.public AND TRUE)
OR EXISTS(
    SELECT * FROM post_group_access
    WHERE post_group_access.fk_post = post.pk
    AND TRUE
    AND post_group_access.fk_granted_group IN(
        SELECT pk FROM user_group
        WHERE fk_owner = 1
        OR EXISTS(
            SELECT * FROM user_group_membership
            WHERE NOT revoked AND fk_user = 1 AND fk_group = user_group.pk
        )
    )
)) ORDER BY post.pk DESC LIMIT LEAST(50, 100) OFFSET (50) * 20;

----------------------------------------------------------------

WITH cte1 AS (
    SELECT unnest(
        tag.pk
        || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
        || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
        || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
    ) AS tag_keys
    FROM tag WHERE lower(tag.tag_name) = 'video'
), cte0 AS (
    SELECT unnest(
        tag.pk
        || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
        || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
        || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
    ) AS tag_keys
    FROM tag WHERE lower(tag.tag_name) = 'masseffect'
), countCte AS (
    SELECT CASE 
    WHEN (SELECT COUNT(*) FROM (SELECT DISTINCT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user LEFT JOIN post_tag ON post_tag.fk_post = post.pk GROUP BY post.pk HAVING array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte0) AND array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte1) LIMIT 100000) limitedPks) < 100000 
    THEN (SELECT COUNT(*) FROM (SELECT DISTINCT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user LEFT JOIN post_tag ON post_tag.fk_post = post.pk WHERE 
    (post.fk_create_user = 1
    OR (post.public AND TRUE)
    OR EXISTS(
        SELECT * FROM post_group_access
        WHERE post_group_access.fk_post = post.pk
        AND TRUE
        AND post_group_access.fk_granted_group IN(
            SELECT pk FROM user_group
            WHERE fk_owner = 1
            OR EXISTS(
                SELECT * FROM user_group_membership
                WHERE NOT revoked AND fk_user = 1 AND fk_group = user_group.pk
            )
        )
    )) GROUP BY post.pk HAVING array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte0) AND array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte1)) pks) END AS full_count
), postPks AS (
    SELECT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user LEFT JOIN post_tag ON post_tag.fk_post = post.pk 
    WHERE (post.fk_create_user = 1
    OR (post.public AND TRUE)
    OR EXISTS(
        SELECT * FROM post_group_access
        WHERE post_group_access.fk_post = post.pk
        AND TRUE
        AND post_group_access.fk_granted_group IN(
            SELECT pk FROM user_group
            WHERE fk_owner = 1
            OR EXISTS(
                SELECT * FROM user_group_membership
                WHERE NOT revoked AND fk_user = 1 AND fk_group = user_group.pk
            )
        )
    )) GROUP BY post.pk HAVING array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte0) AND array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte1) ORDER BY post.pk DESC LIMIT LEAST(50, 100) OFFSET (50) * 0
) 
SELECT post.pk AS post_pk, post.data_url AS post_data_url, post.source_url AS post_source_url, post.title AS post_title, post.creation_timestamp AS post_creation_timestamp, post.score AS post_score, post.thumbnail_url AS post_thumbnail_url, post.public AS post_public, post.public_edit AS post_public_edit, post.description AS post_description, s3_object.thumbnail_object_key AS post_thumbnail_object_key, create_user.pk AS post_create_user_pk, create_user.user_name AS post_create_user_user_name, create_user.user_name AS post_create_user_user_name, create_user.email AS post_create_user_email, create_user.avatar_url AS post_create_user_avatar_url, create_user.creation_timestamp AS post_create_user_creation_timestamp, create_user.email_confirmed AS post_create_user_email_confirmed, create_user.display_name AS post_create_user_display_name, create_user.password_fail_count AS post_create_user_password_fail_count, s3_object.object_key AS post_s3_object_object_key, s3_object.sha256_hash AS post_s3_object_sha256_hash, s3_object.size_bytes AS post_s3_object_size_bytes, s3_object.mime_type AS post_s3_object_mime_type, s3_object.fk_broker AS post_s3_object_fk_broker, s3_object.fk_uploader AS post_s3_object_fk_uploader, s3_object.thumbnail_object_key AS post_s3_object_thumbnail_object_key, s3_object.creation_timestamp AS post_s3_object_creation_timestamp, s3_object.filename AS post_s3_object_filename, s3_object.hls_master_playlist AS post_s3_object_hls_master_playlist, s3_object.hls_disabled AS post_s3_object_hls_disabled, s3_object.hls_locked_at AS post_s3_object_hls_locked_at, s3_object.thumbnail_locked_at AS post_s3_object_thumbnail_locked_at, s3_object.hls_fail_count AS post_s3_object_hls_fail_count, s3_object.thumbnail_fail_count AS post_s3_object_thumbnail_fail_count, s3_object.thumbnail_disabled AS post_s3_object_thumbnail_disabled, (SELECT full_count FROM countCte), 50 AS evaluated_limit 
FROM post 
LEFT JOIN s3_object ON s3_object.object_key = post.s3_object 
INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user 
WHERE post.pk IN(SELECT pk FROM postPks) 
ORDER BY post.pk DESC;




---------------------------------------------------------------- larger: 


WITH cte3 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'krogan'
                    ), cte2 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'bbc'
                    ), cte5 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'vorcha'
                    ), cte0 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'liara'
                    ), cte4 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'yahg'
                    ), cte1 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'beast'
                    ), countCte AS (SELECT CASE WHEN (SELECT COUNT(*) FROM (SELECT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user WHERE EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte0)) AND ((((EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte1)) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte2))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte3))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte4))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte5))) LIMIT 100000) limitedPks) < 100000 THEN (SELECT COUNT(*) FROM (SELECT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user WHERE EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte0)) AND ((((EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte1)) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte2))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte3))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte4))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte5))) AND 
            (post.fk_create_user = 1
            OR (post.public AND TRUE)
            OR EXISTS(
                SELECT * FROM post_group_access
                WHERE post_group_access.fk_post = post.pk
                AND TRUE
                AND post_group_access.fk_granted_group IN(
                    SELECT pk FROM user_group
                    WHERE fk_owner = 1
                    OR EXISTS(
                        SELECT * FROM user_group_membership
                        WHERE NOT revoked AND fk_user = 1 AND fk_group = user_group.pk
                    )
                )
            ))) pks) END AS full_count) SELECT post.pk AS post_pk, post.data_url AS post_data_url, post.source_url AS post_source_url, post.title AS post_title, post.creation_timestamp AS post_creation_timestamp, post.score AS post_score, post.thumbnail_url AS post_thumbnail_url, post.public AS post_public, post.public_edit AS post_public_edit, post.description AS post_description, s3_object.thumbnail_object_key AS post_thumbnail_object_key, create_user.pk AS post_create_user_pk, create_user.user_name AS post_create_user_user_name, create_user.user_name AS post_create_user_user_name, create_user.email AS post_create_user_email, create_user.avatar_url AS post_create_user_avatar_url, create_user.creation_timestamp AS post_create_user_creation_timestamp, create_user.email_confirmed AS post_create_user_email_confirmed, create_user.display_name AS post_create_user_display_name, create_user.password_fail_count AS post_create_user_password_fail_count, s3_object.object_key AS post_s3_object_object_key, s3_object.sha256_hash AS post_s3_object_sha256_hash, s3_object.size_bytes AS post_s3_object_size_bytes, s3_object.mime_type AS post_s3_object_mime_type, s3_object.fk_broker AS post_s3_object_fk_broker, s3_object.fk_uploader AS post_s3_object_fk_uploader, s3_object.thumbnail_object_key AS post_s3_object_thumbnail_object_key, s3_object.creation_timestamp AS post_s3_object_creation_timestamp, s3_object.filename AS post_s3_object_filename, s3_object.hls_master_playlist AS post_s3_object_hls_master_playlist, s3_object.hls_disabled AS post_s3_object_hls_disabled, s3_object.hls_locked_at AS post_s3_object_hls_locked_at, s3_object.thumbnail_locked_at AS post_s3_object_thumbnail_locked_at, s3_object.hls_fail_count AS post_s3_object_hls_fail_count, s3_object.thumbnail_fail_count AS post_s3_object_thumbnail_fail_count, s3_object.thumbnail_disabled AS post_s3_object_thumbnail_disabled, (SELECT full_count FROM countCte), 50 AS evaluated_limit FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user WHERE EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte0)) AND ((((EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte1)) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte2))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte3))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte4))) OR EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte5))) AND 
            (post.fk_create_user = 1
            OR (post.public AND TRUE)
            OR EXISTS(
                SELECT * FROM post_group_access
                WHERE post_group_access.fk_post = post.pk
                AND TRUE
                AND post_group_access.fk_granted_group IN(
                    SELECT pk FROM user_group
                    WHERE fk_owner = 1
                    OR EXISTS(
                        SELECT * FROM user_group_membership
                        WHERE NOT revoked AND fk_user = 1 AND fk_group = user_group.pk
                    )
                )
            )) ORDER BY post.pk DESC LIMIT LEAST(50, 100) OFFSET (50) * 0;



-------------------------------

WITH cte3 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'krogan'
                    ), cte1 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'beast'
                    ), cte0 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'liara'
                    ), cte5 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'vorcha'
                    ), cte2 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'bbc'
                    ), cte4 AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = 'yahg'
                    ), countCte AS (SELECT CASE WHEN (SELECT COUNT(*) FROM (SELECT DISTINCT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user LEFT JOIN post_tag ON post_tag.fk_post = post.pk GROUP BY post.pk HAVING array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte0) AND ((((array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte1) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte2)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte3)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte4)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte5)) LIMIT 100000) limitedPks) < 100000 THEN (SELECT COUNT(*) FROM (SELECT DISTINCT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user LEFT JOIN post_tag ON post_tag.fk_post = post.pk WHERE 
            (post.fk_create_user = 1
            OR (post.public AND TRUE)
            OR EXISTS(
                SELECT * FROM post_group_access
                WHERE post_group_access.fk_post = post.pk
                AND TRUE
                AND post_group_access.fk_granted_group IN(
                    SELECT pk FROM user_group
                    WHERE fk_owner = 1
                    OR EXISTS(
                        SELECT * FROM user_group_membership
                        WHERE NOT revoked AND fk_user = 1 AND fk_group = user_group.pk
                    )
                )
            )) GROUP BY post.pk HAVING array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte0) AND ((((array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte1) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte2)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte3)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte4)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte5))) pks) END AS full_count), postPks AS (SELECT post.pk FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user LEFT JOIN post_tag ON post_tag.fk_post = post.pk WHERE 
            (post.fk_create_user = 1
            OR (post.public AND TRUE)
            OR EXISTS(
                SELECT * FROM post_group_access
                WHERE post_group_access.fk_post = post.pk
                AND TRUE
                AND post_group_access.fk_granted_group IN(
                    SELECT pk FROM user_group
                    WHERE fk_owner = 1
                    OR EXISTS(
                        SELECT * FROM user_group_membership
                        WHERE NOT revoked AND fk_user = 1 AND fk_group = user_group.pk
                    )
                )
            )) GROUP BY post.pk HAVING array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte0) AND ((((array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte1) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte2)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte3)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte4)) OR array_agg(post_tag.fk_tag) && ARRAY(SELECT tag_keys FROM cte5)) ORDER BY post.pk DESC LIMIT LEAST(50, 100) OFFSET (50) * 0)  SELECT post.pk AS post_pk, post.data_url AS post_data_url, post.source_url AS post_source_url, post.title AS post_title, post.creation_timestamp AS post_creation_timestamp, post.score AS post_score, post.thumbnail_url AS post_thumbnail_url, post.public AS post_public, post.public_edit AS post_public_edit, post.description AS post_description, s3_object.thumbnail_object_key AS post_thumbnail_object_key, create_user.pk AS post_create_user_pk, create_user.user_name AS post_create_user_user_name, create_user.user_name AS post_create_user_user_name, create_user.email AS post_create_user_email, create_user.avatar_url AS post_create_user_avatar_url, create_user.creation_timestamp AS post_create_user_creation_timestamp, create_user.email_confirmed AS post_create_user_email_confirmed, create_user.display_name AS post_create_user_display_name, create_user.password_fail_count AS post_create_user_password_fail_count, s3_object.object_key AS post_s3_object_object_key, s3_object.sha256_hash AS post_s3_object_sha256_hash, s3_object.size_bytes AS post_s3_object_size_bytes, s3_object.mime_type AS post_s3_object_mime_type, s3_object.fk_broker AS post_s3_object_fk_broker, s3_object.fk_uploader AS post_s3_object_fk_uploader, s3_object.thumbnail_object_key AS post_s3_object_thumbnail_object_key, s3_object.creation_timestamp AS post_s3_object_creation_timestamp, s3_object.filename AS post_s3_object_filename, s3_object.hls_master_playlist AS post_s3_object_hls_master_playlist, s3_object.hls_disabled AS post_s3_object_hls_disabled, s3_object.hls_locked_at AS post_s3_object_hls_locked_at, s3_object.thumbnail_locked_at AS post_s3_object_thumbnail_locked_at, s3_object.hls_fail_count AS post_s3_object_hls_fail_count, s3_object.thumbnail_fail_count AS post_s3_object_thumbnail_fail_count, s3_object.thumbnail_disabled AS post_s3_object_thumbnail_disabled, (SELECT full_count FROM countCte), 50 AS evaluated_limit FROM post LEFT JOIN s3_object ON s3_object.object_key = post.s3_object INNER JOIN registered_user create_user ON create_user.pk = post.fk_create_user WHERE post.pk IN(SELECT pk FROM postPks) ORDER BY post.pk DESC;
