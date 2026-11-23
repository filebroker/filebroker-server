-- Add directly assigned tag names to the existing combined full-text index.
-- Tag aliases and hierarchy relationships are deliberately NOT expanded.
-- The search_vector generated column and both existing GIN indexes automatically
-- reflect updates to search_text; no index recreation is needed.

-- Refresh multiple entities in a single statement, so bulk tag edits don't
-- repeatedly update the same search row for every assigned tag.

CREATE FUNCTION update_post_search_indexes(target_pks BIGINT[])
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
            m.genre,
            tags.names
        )
    FROM post AS p
    LEFT JOIN s3_object_metadata AS m ON m.object_key = p.s3_object
    LEFT JOIN LATERAL (
        SELECT string_agg(t.tag_name, ' ' ORDER BY t.pk) AS names
        FROM post_tag AS pt
        INNER JOIN tag AS t ON t.pk = pt.fk_tag
        WHERE pt.fk_post = p.pk
    ) AS tags ON TRUE
    WHERE p.pk = ANY(target_pks)
    ON CONFLICT (fk_post) DO UPDATE
    SET search_text = EXCLUDED.search_text
    WHERE post_search_index.search_text IS DISTINCT FROM EXCLUDED.search_text;
$$;

CREATE FUNCTION update_post_collection_search_indexes(target_pks BIGINT[])
RETURNS VOID
LANGUAGE SQL
AS $$
    INSERT INTO post_collection_search_index (fk_post_collection, search_text)
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.title,
            p.description,
            tags.names
        )
    FROM post_collection AS p
    LEFT JOIN LATERAL (
        SELECT string_agg(t.tag_name, ' ' ORDER BY t.pk) AS names
        FROM post_collection_tag AS pt
        INNER JOIN tag AS t ON t.pk = pt.fk_tag
        WHERE pt.fk_post_collection = p.pk
    ) AS tags ON TRUE
    WHERE p.pk = ANY(target_pks)
    ON CONFLICT (fk_post_collection) DO UPDATE
    SET search_text = EXCLUDED.search_text
    WHERE post_collection_search_index.search_text IS DISTINCT FROM EXCLUDED.search_text;
$$;

CREATE FUNCTION update_user_group_search_indexes(target_pks BIGINT[])
RETURNS VOID
LANGUAGE SQL
AS $$
    INSERT INTO user_group_search_index (fk_user_group, search_text)
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.name,
            p.description,
            tags.names
        )
    FROM user_group AS p
    LEFT JOIN LATERAL (
        SELECT string_agg(t.tag_name, ' ' ORDER BY t.pk) AS names
        FROM user_group_tag AS pt
        INNER JOIN tag AS t ON t.pk = pt.fk_tag
        WHERE pt.fk_user_group = p.pk
    ) AS tags ON TRUE
    WHERE p.pk = ANY(target_pks)
    ON CONFLICT (fk_user_group) DO UPDATE
    SET search_text = EXCLUDED.search_text
    WHERE user_group_search_index.search_text IS DISTINCT FROM EXCLUDED.search_text;
$$;

-- Preserve the old entry points used by the existing post / metadata / collection / group triggers, routing them through the batch helpers.
CREATE OR REPLACE FUNCTION update_post_search_index(target_post_pk BIGINT)
RETURNS VOID
LANGUAGE SQL
AS $$
    SELECT update_post_search_indexes(ARRAY[target_post_pk]);
$$;

CREATE OR REPLACE FUNCTION update_post_collection_search_index()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM update_post_collection_search_indexes(ARRAY[NEW.pk]);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_user_group_search_index()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM update_user_group_search_indexes(ARRAY[NEW.pk]);
    RETURN NEW;
END;
$$;

-- INSERT/DELETE triggers use transition tables to deduplicate affected entities. UPDATE is row-level, and only reacts to changes of the keys.
CREATE FUNCTION refresh_post_search_index_from_tag_usage()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM update_post_search_indexes(
            ARRAY(SELECT DISTINCT fk_post FROM new_rows)
        );
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM update_post_search_indexes(
            ARRAY(SELECT DISTINCT fk_post FROM old_rows)
        );
    ELSE
        PERFORM update_post_search_indexes(ARRAY[OLD.fk_post, NEW.fk_post]);
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER post_tag_search_index_insert
AFTER INSERT ON post_tag
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_post_search_index_from_tag_usage();

CREATE TRIGGER post_tag_search_index_delete
AFTER DELETE ON post_tag
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_post_search_index_from_tag_usage();

CREATE TRIGGER post_tag_search_index_update
AFTER UPDATE ON post_tag
FOR EACH ROW
WHEN (
    OLD.fk_post IS DISTINCT FROM NEW.fk_post
    OR OLD.fk_tag IS DISTINCT FROM NEW.fk_tag
)
EXECUTE FUNCTION refresh_post_search_index_from_tag_usage();

-- INSERT/DELETE triggers use transition tables to deduplicate affected entities. UPDATE is row-level, and only reacts to changes of the keys.
CREATE FUNCTION refresh_post_collection_search_index_from_tag_usage()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM update_post_collection_search_indexes(
            ARRAY(SELECT DISTINCT fk_post_collection FROM new_rows)
        );
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM update_post_collection_search_indexes(
            ARRAY(SELECT DISTINCT fk_post_collection FROM old_rows)
        );
    ELSE
        PERFORM update_post_collection_search_indexes(ARRAY[OLD.fk_post_collection, NEW.fk_post_collection]);
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER post_collection_tag_search_index_insert
AFTER INSERT ON post_collection_tag
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_post_collection_search_index_from_tag_usage();

CREATE TRIGGER post_collection_tag_search_index_delete
AFTER DELETE ON post_collection_tag
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_post_collection_search_index_from_tag_usage();

CREATE TRIGGER post_collection_tag_search_index_update
AFTER UPDATE ON post_collection_tag
FOR EACH ROW
WHEN (
    OLD.fk_post_collection IS DISTINCT FROM NEW.fk_post_collection
    OR OLD.fk_tag IS DISTINCT FROM NEW.fk_tag
)
EXECUTE FUNCTION refresh_post_collection_search_index_from_tag_usage();

-- INSERT/DELETE triggers use transition tables to deduplicate affected entities. UPDATE is row-level, and only reacts to changes of the keys.
CREATE FUNCTION refresh_user_group_search_index_from_tag_usage()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM update_user_group_search_indexes(
            ARRAY(SELECT DISTINCT fk_user_group FROM new_rows)
        );
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM update_user_group_search_indexes(
            ARRAY(SELECT DISTINCT fk_user_group FROM old_rows)
        );
    ELSE
        PERFORM update_user_group_search_indexes(ARRAY[OLD.fk_user_group, NEW.fk_user_group]);
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER user_group_tag_search_index_insert
AFTER INSERT ON user_group_tag
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_user_group_search_index_from_tag_usage();

CREATE TRIGGER user_group_tag_search_index_delete
AFTER DELETE ON user_group_tag
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_user_group_search_index_from_tag_usage();

CREATE TRIGGER user_group_tag_search_index_update
AFTER UPDATE ON user_group_tag
FOR EACH ROW
WHEN (
    OLD.fk_user_group IS DISTINCT FROM NEW.fk_user_group
    OR OLD.fk_tag IS DISTINCT FROM NEW.fk_tag
)
EXECUTE FUNCTION refresh_user_group_search_index_from_tag_usage();

-- Renaming a tag changes its directly assigned name in every related search index.
-- Transition tables ensure a multi-row UPDATE refreshes each affected entity once.
-- A tag-name change is independent of tag hierarchy and alias changes.
CREATE FUNCTION refresh_search_indexes_from_tag_name_update()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    changed_tag_pks BIGINT[];
BEGIN
    SELECT COALESCE(array_agg(new_rows.pk), ARRAY[]::BIGINT[])
    INTO changed_tag_pks
    FROM new_rows
    INNER JOIN old_rows ON old_rows.pk = new_rows.pk
    WHERE old_rows.tag_name IS DISTINCT FROM new_rows.tag_name;

    IF cardinality(changed_tag_pks) = 0 THEN
        RETURN NULL;
    END IF;

    PERFORM update_post_search_indexes(
        ARRAY(
            SELECT DISTINCT fk_post
            FROM post_tag
            WHERE fk_tag = ANY(changed_tag_pks)
        )
    );

    PERFORM update_post_collection_search_indexes(
        ARRAY(
            SELECT DISTINCT fk_post_collection
            FROM post_collection_tag
            WHERE fk_tag = ANY(changed_tag_pks)
        )
    );

    PERFORM update_user_group_search_indexes(
        ARRAY(
            SELECT DISTINCT fk_user_group
            FROM user_group_tag
            WHERE fk_tag = ANY(changed_tag_pks)
        )
    );

    RETURN NULL;
END;
$$;

-- Transition tables require a general UPDATE trigger (not UPDATE OF tag_name).
-- The function above filters out updates that do not change tag_name.
CREATE TRIGGER tag_name_search_index_update
AFTER UPDATE ON tag
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_search_indexes_from_tag_name_update();

-- No separate tag DELETE trigger is necessary. The fk_tag foreign keys of
-- post_tag, post_collection_tag, and user_group_tag use ON DELETE CASCADE;
-- PostgreSQL fires the existing DELETE triggers on those relation tables for
-- cascaded deletions as well. Their refresh functions reconstruct search_text
-- after the deleted tag's assignments have been removed.

-- One-time backfill, set-based and limited to entities with assigned tags.
-- Recomputes the complete search_text rather than appending to the old value.
-- The existing generated search_vector and GIN indexes update automatically.

WITH tag_names AS (
    SELECT
        pt.fk_post AS pk,
        string_agg(t.tag_name, ' ' ORDER BY t.pk) AS names
    FROM post_tag AS pt
    INNER JOIN tag AS t ON t.pk = pt.fk_tag
    GROUP BY pt.fk_post
), new_text AS (
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.title,
            p.description,
            m.artist,
            m.album,
            m.composer,
            m.genre,
            tag_names.names
        ) AS search_text
    FROM tag_names
    INNER JOIN post AS p ON p.pk = tag_names.pk
    LEFT JOIN s3_object_metadata AS m ON m.object_key = p.s3_object
)
UPDATE post_search_index AS si
SET search_text = new_text.search_text
FROM new_text
WHERE si.fk_post = new_text.pk
  AND si.search_text IS DISTINCT FROM new_text.search_text;

WITH tag_names AS (
    SELECT
        pt.fk_post_collection AS pk,
        string_agg(t.tag_name, ' ' ORDER BY t.pk) AS names
    FROM post_collection_tag AS pt
    INNER JOIN tag AS t ON t.pk = pt.fk_tag
    GROUP BY pt.fk_post_collection
), new_text AS (
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.title,
            p.description,
            tag_names.names
        ) AS search_text
    FROM tag_names
    INNER JOIN post_collection AS p ON p.pk = tag_names.pk
)
UPDATE post_collection_search_index AS si
SET search_text = new_text.search_text
FROM new_text
WHERE si.fk_post_collection = new_text.pk
  AND si.search_text IS DISTINCT FROM new_text.search_text;

WITH tag_names AS (
    SELECT
        pt.fk_user_group AS pk,
        string_agg(t.tag_name, ' ' ORDER BY t.pk) AS names
    FROM user_group_tag AS pt
    INNER JOIN tag AS t ON t.pk = pt.fk_tag
    GROUP BY pt.fk_user_group
), new_text AS (
    SELECT
        p.pk,
        concat_ws(
            ' ',
            p.name,
            p.description,
            tag_names.names
        ) AS search_text
    FROM tag_names
    INNER JOIN user_group AS p ON p.pk = tag_names.pk
)
UPDATE user_group_search_index AS si
SET search_text = new_text.search_text
FROM new_text
WHERE si.fk_user_group = new_text.pk
  AND si.search_text IS DISTINCT FROM new_text.search_text;
