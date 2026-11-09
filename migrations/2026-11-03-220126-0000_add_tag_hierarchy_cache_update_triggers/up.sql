-- Tag cache invalidation protocol.
--
-- Listeners subscribe to the tag_cache_changed channel on the primary database.
-- Notifications contain a JSON object with the committed generation and either
-- the affected tag IDs or a request to rebuild the entire cache:
--
--   {"generation": 123, "tag_pks": [11, 29, 500881]}
--   {"generation": 123, "reload": true}
--
-- Notifications are generated once per modifying statement/trigger invocation,
-- not once per affected tag. Multiple notifications from the same transaction
-- may contain different tag IDs but always carry the same generation. They are
-- delivered only after commit, and are discarded if the transaction rolls back.
--
-- The generation is replicated along with the tag data. Before refreshing from
-- a read replica, listeners must ensure that the replica's generation is at
-- least as high as the generation in the received notification. A reconnect or
-- other notification gap requires a full cache reload.
--
-- Updating the singleton generation row serializes concurrent cache-affecting
-- transactions. This guarantees commit-ordered generations at the cost of
-- possible write contention and serialization retries.
CREATE TABLE tag_cache_generation (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    generation BIGINT NOT NULL DEFAULT 0,
    last_transaction_id xid8
);

INSERT INTO tag_cache_generation (singleton, generation)
VALUES (TRUE, 0);


-- Record a cache invalidation and emit a single batched notification. Calls
-- without affected IDs have no effect. The generation advances at most once
-- per top-level transaction, even when several triggers invoke this function.
CREATE FUNCTION notify_tag_cache_changed(tag_pks BIGINT[])
RETURNS VOID AS
$BODY$
DECLARE
    affected_tag_pks BIGINT[];
    transaction_id xid8;
    current_generation BIGINT;
    payload TEXT;
BEGIN
    SELECT array_agg(DISTINCT tag_pk ORDER BY tag_pk)
    INTO affected_tag_pks
    FROM unnest(tag_pks) AS affected(tag_pk)
    WHERE tag_pk IS NOT NULL;

    IF affected_tag_pks IS NULL THEN
        RETURN;
    END IF;

    transaction_id := pg_current_xact_id();

    UPDATE tag_cache_generation
    SET generation = generation + 1,
        last_transaction_id = transaction_id
    WHERE singleton
      AND last_transaction_id IS DISTINCT FROM transaction_id
    RETURNING generation INTO current_generation;

    IF current_generation IS NULL THEN
        SELECT generation INTO current_generation
        FROM tag_cache_generation
        WHERE singleton;
    END IF;

    -- Fall back to a complete refresh when the affected set is large or its
    -- JSON representation would exceed PostgreSQL's NOTIFY payload limit.
    IF cardinality(affected_tag_pks) <= 512 THEN
        payload := jsonb_build_object(
            'generation', current_generation,
            'tag_pks', affected_tag_pks
        )::TEXT;
    END IF;

    IF payload IS NULL OR octet_length(payload) >= 7900 THEN
        payload := jsonb_build_object(
            'generation', current_generation,
            'reload', TRUE
        )::TEXT;
    END IF;

    PERFORM pg_notify('tag_cache_changed', payload);
END;
$BODY$
LANGUAGE plpgsql;


-- Closure rows with depth > 0 contribute to the cached descendant lists.
-- The parent of each changed row is the entry whose effective IDs may change.
-- Depth-zero self-references do not contribute to those lists; tag creation
-- and deletion are notified independently by the triggers on tag.
CREATE FUNCTION notify_tag_cache_closure_insert_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        SELECT DISTINCT fk_parent
        FROM new_rows
        WHERE depth > 0
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION notify_tag_cache_closure_delete_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        SELECT DISTINCT fk_parent
        FROM old_rows
        WHERE depth > 0
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION notify_tag_cache_closure_update_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        SELECT fk_parent FROM old_rows WHERE depth > 0
        UNION
        SELECT fk_parent FROM new_rows WHERE depth > 0
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER after_insert_tag_closure_notify_cache
    AFTER INSERT ON tag_closure_table
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_closure_insert_trigger();

CREATE TRIGGER after_delete_tag_closure_notify_cache
    AFTER DELETE ON tag_closure_table
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_closure_delete_trigger();

CREATE TRIGGER after_update_tag_closure_notify_cache
    AFTER UPDATE ON tag_closure_table
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_closure_update_trigger();


-- Alias relationships are stored as one row per unordered tag pair. Both
-- endpoints must be refreshed. Existing alias synchronization and hierarchy
-- triggers maintain the associated transitive relationships.
CREATE FUNCTION notify_tag_cache_alias_insert_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        SELECT fk_source FROM new_rows
        UNION
        SELECT fk_target FROM new_rows
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION notify_tag_cache_alias_delete_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        SELECT fk_source FROM old_rows
        UNION
        SELECT fk_target FROM old_rows
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION notify_tag_cache_alias_update_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        SELECT fk_source FROM old_rows
        UNION
        SELECT fk_target FROM old_rows
        UNION
        SELECT fk_source FROM new_rows
        UNION
        SELECT fk_target FROM new_rows
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER after_insert_tag_alias_notify_cache
    AFTER INSERT ON tag_alias
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_alias_insert_trigger();

CREATE TRIGGER after_delete_tag_alias_notify_cache
    AFTER DELETE ON tag_alias
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_alias_delete_trigger();

CREATE TRIGGER after_update_tag_alias_notify_cache
    AFTER UPDATE ON tag_alias
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_alias_update_trigger();


-- The cache contains tag names and matching counts in addition to topology.
-- Ignore updates that do not change cached columns. Transition tables allow
-- bulk insertions and updates to be reported in a single notification.
CREATE FUNCTION notify_tag_cache_tag_insert_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        SELECT pk FROM new_rows
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION notify_tag_cache_tag_delete_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        SELECT pk FROM old_rows
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION notify_tag_cache_tag_update_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM notify_tag_cache_changed(ARRAY(
        WITH changed AS (
            SELECT old_tag.pk AS old_pk, new_tag.pk AS new_pk
            FROM old_rows AS old_tag
            FULL JOIN new_rows AS new_tag ON new_tag.pk = old_tag.pk
            WHERE old_tag.pk IS NULL
               OR new_tag.pk IS NULL
               OR (
                   old_tag.tag_name,
                   old_tag.matching_post_count,
                   old_tag.matching_post_collection_count,
                   old_tag.matching_user_group_count
               ) IS DISTINCT FROM (
                   new_tag.tag_name,
                   new_tag.matching_post_count,
                   new_tag.matching_post_collection_count,
                   new_tag.matching_user_group_count
               )
        )
        SELECT old_pk FROM changed
        UNION
        SELECT new_pk FROM changed
    ));
    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER after_insert_tag_notify_cache
    AFTER INSERT ON tag
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_tag_insert_trigger();

CREATE TRIGGER after_delete_tag_notify_cache
    AFTER DELETE ON tag
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_tag_delete_trigger();

-- Column-specific UPDATE triggers cannot use transition tables. Filter
-- updates by comparing the old and new cached values in the trigger function.
CREATE TRIGGER after_update_tag_notify_cache
    AFTER UPDATE ON tag
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_tag_cache_tag_update_trigger();
