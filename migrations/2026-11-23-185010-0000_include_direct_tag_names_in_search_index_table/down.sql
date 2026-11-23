-- Stop maintaining the assigned-tag portion of the search text.

DROP TRIGGER tag_name_search_index_update ON tag;
DROP FUNCTION refresh_search_indexes_from_tag_name_update();

DROP TRIGGER post_tag_search_index_insert ON post_tag;
DROP TRIGGER post_tag_search_index_delete ON post_tag;
DROP TRIGGER post_tag_search_index_update ON post_tag;
DROP FUNCTION refresh_post_search_index_from_tag_usage();

DROP TRIGGER post_collection_tag_search_index_insert ON post_collection_tag;
DROP TRIGGER post_collection_tag_search_index_delete ON post_collection_tag;
DROP TRIGGER post_collection_tag_search_index_update ON post_collection_tag;
DROP FUNCTION refresh_post_collection_search_index_from_tag_usage();

DROP TRIGGER user_group_tag_search_index_insert ON user_group_tag;
DROP TRIGGER user_group_tag_search_index_delete ON user_group_tag;
DROP TRIGGER user_group_tag_search_index_update ON user_group_tag;
DROP FUNCTION refresh_user_group_search_index_from_tag_usage();

-- Restore the refresh functions from the original full-text migration.
CREATE OR REPLACE FUNCTION update_post_search_index(target_post_pk BIGINT)
RETURNS VOID
LANGUAGE SQL
AS $$
    INSERT INTO post_search_index (fk_post, search_text)
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.title,
            p.description,
            m.artist,
            m.album,
            m.composer,
            m.genre
        )
    FROM post AS p
    LEFT JOIN s3_object_metadata AS m ON m.object_key = p.s3_object
    WHERE p.pk = target_post_pk
    ON CONFLICT (fk_post) DO UPDATE
    SET search_text = EXCLUDED.search_text;
$$;

CREATE OR REPLACE FUNCTION update_post_collection_search_index()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO post_collection_search_index (fk_post_collection, search_text)
    VALUES (
        NEW.pk,
        concat_ws(' ', NEW.title, NEW.description)
    )
    ON CONFLICT (fk_post_collection) DO UPDATE
    SET search_text = EXCLUDED.search_text;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_user_group_search_index()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO user_group_search_index (fk_user_group, search_text)
    VALUES (
        NEW.pk,
        concat_ws(' ', NEW.name, NEW.description)
    )
    ON CONFLICT (fk_user_group) DO UPDATE
    SET search_text = EXCLUDED.search_text;

    RETURN NEW;
END;
$$;

DROP FUNCTION update_post_search_indexes(BIGINT[]);
DROP FUNCTION update_post_collection_search_indexes(BIGINT[]);
DROP FUNCTION update_user_group_search_indexes(BIGINT[]);

-- Remove tag names from the materialized search_text for currently tagged
-- entities. Entities with no tags already have their original search text,
-- since assignment removals recomputed it before the triggers were dropped.

WITH tagged AS (
    SELECT DISTINCT fk_post AS pk
    FROM post_tag
), old_text AS (
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.title,
            p.description,
            m.artist,
            m.album,
            m.composer,
            m.genre
        ) AS search_text
    FROM tagged
    INNER JOIN post AS p ON p.pk = tagged.pk
    LEFT JOIN s3_object_metadata AS m ON m.object_key = p.s3_object
)
UPDATE post_search_index AS si
SET search_text = old_text.search_text
FROM old_text
WHERE si.fk_post = old_text.pk
  AND si.search_text IS DISTINCT FROM old_text.search_text;

WITH tagged AS (
    SELECT DISTINCT fk_post_collection AS pk
    FROM post_collection_tag
), old_text AS (
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.title,
            p.description
        ) AS search_text
    FROM tagged
    INNER JOIN post_collection AS p ON p.pk = tagged.pk
)
UPDATE post_collection_search_index AS si
SET search_text = old_text.search_text
FROM old_text
WHERE si.fk_post_collection = old_text.pk
  AND si.search_text IS DISTINCT FROM old_text.search_text;

WITH tagged AS (
    SELECT DISTINCT fk_user_group AS pk
    FROM user_group_tag
), old_text AS (
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.name,
            p.description
        ) AS search_text
    FROM tagged
    INNER JOIN user_group AS p ON p.pk = tagged.pk
)
UPDATE user_group_search_index AS si
SET search_text = old_text.search_text
FROM old_text
WHERE si.fk_user_group = old_text.pk
  AND si.search_text IS DISTINCT FROM old_text.search_text;
