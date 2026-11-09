ALTER TABLE tag
    ADD COLUMN matching_post_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN matching_post_collection_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN matching_user_group_count BIGINT NOT NULL DEFAULT 0;

CREATE TABLE reconcile_tag_matching_counts_task (
    pk BIGSERIAL PRIMARY KEY,
    fk_tag BIGINT REFERENCES tag(pk) ON DELETE CASCADE NOT NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    reconcile_post_count BOOLEAN NOT NULL DEFAULT FALSE,
    reconcile_post_collection_count BOOLEAN NOT NULL DEFAULT FALSE,
    reconcile_user_group_count BOOLEAN NOT NULL DEFAULT FALSE,

    -- Any new work increments this version, including ordinary tag usage changes.
    invalidation_version BIGINT NOT NULL DEFAULT 1,
    -- Only changes to matching semantics (aliases, hierarchy, rename) increment this.
    topology_version BIGINT NOT NULL DEFAULT 1,

    locked_at TIMESTAMP WITH TIME ZONE,
    fail_count INTEGER NOT NULL DEFAULT 0,

    UNIQUE (fk_tag),

    CHECK (
        reconcile_post_count
        OR reconcile_post_collection_count
        OR reconcile_user_group_count
    )
);

CREATE INDEX reconcile_tag_matching_counts_task_pending_idx ON reconcile_tag_matching_counts_task(fail_count, creation_timestamp)
    WHERE locked_at IS NULL AND fail_count < 3;


-- Creates match count reconciliation tasks for the alias group of each supplied tag.
--
-- Alias relationships are synchronised transitively, so every tag belongs to exactly one alias group and every member
-- of that group is directly related to every other member.
--
-- New work is represented by the current canonical member of each alias group, defined as MIN(tag.pk).
--
-- Existing non-canonical task rows can remain after an alias-group merge or split. They are deliberately retained,
-- but future changes in their current alias group also bump the appropriate versions.
-- A worker may publish a slightly stale usage count if only invalidation_version changed;
-- it must discard its result if topology_version changed. In either case the task
-- remains pending so the new work can be processed by a later cron run.
--
-- creation_timestamp is deliberately never updated. Repeated invalidations therefore do not postpone the configured reconciliation delay.
CREATE FUNCTION create_reconcile_tag_matching_counts_tasks(
    tag_pks BIGINT[],
    post_count_dirty BOOLEAN,
    post_collection_count_dirty BOOLEAN,
    user_group_count_dirty BOOLEAN,
    topology_changed BOOLEAN
) RETURNS VOID AS
$BODY$
DECLARE
    group_tag_pks BIGINT[];
    canonical_tag_pks BIGINT[];
BEGIN
    WITH seed_tags AS (
        SELECT DISTINCT tag.pk
        FROM unnest(tag_pks) AS requested_tag(pk)
        INNER JOIN tag
            ON tag.pk = requested_tag.pk
    ),
    alias_group_members AS (
        -- The tag itself.
        SELECT
            seed_tags.pk AS seed_pk,
            seed_tags.pk AS member_pk
        FROM seed_tags

        UNION

        -- Aliases where the seed is stored as source.
        SELECT
            seed_tags.pk AS seed_pk,
            tag_alias.fk_target AS member_pk
        FROM seed_tags
        INNER JOIN tag_alias
            ON tag_alias.fk_source = seed_tags.pk

        UNION

        -- Aliases where the seed is stored as target.
        SELECT
            seed_tags.pk AS seed_pk,
            tag_alias.fk_source AS member_pk
        FROM seed_tags
        INNER JOIN tag_alias
            ON tag_alias.fk_target = seed_tags.pk
    ),
    alias_group_canonicals AS (
        SELECT
            seed_pk,
            MIN(member_pk) AS canonical_pk
        FROM alias_group_members
        GROUP BY seed_pk
    )
    SELECT
        COALESCE(
            (
                SELECT array_agg(DISTINCT member_pk ORDER BY member_pk)
                FROM alias_group_members
            ),
            ARRAY[]::BIGINT[]
        ),
        COALESCE(
            (
                SELECT array_agg(DISTINCT canonical_pk ORDER BY canonical_pk)
                FROM alias_group_canonicals
            ),
            ARRAY[]::BIGINT[]
        )
    INTO
        group_tag_pks,
        canonical_tag_pks;

    IF cardinality(canonical_tag_pks) = 0 THEN
        RETURN;
    END IF;

    -- Create or invalidate the normal canonical task for each affected
    -- current alias group.
    INSERT INTO reconcile_tag_matching_counts_task AS task (
        fk_tag,
        reconcile_post_count,
        reconcile_post_collection_count,
        reconcile_user_group_count
    )
    SELECT
        canonical_tag_pk,
        post_count_dirty,
        post_collection_count_dirty,
        user_group_count_dirty
    FROM unnest(canonical_tag_pks) AS canonical_tag(canonical_tag_pk)
    ON CONFLICT (fk_tag) DO UPDATE
    SET
        reconcile_post_count =
            task.reconcile_post_count
            OR EXCLUDED.reconcile_post_count,

        reconcile_post_collection_count =
            task.reconcile_post_collection_count
            OR EXCLUDED.reconcile_post_collection_count,

        reconcile_user_group_count =
            task.reconcile_user_group_count
            OR EXCLUDED.reconcile_user_group_count,

        invalidation_version =
            task.invalidation_version + 1,

        topology_version =
            task.topology_version + CASE WHEN topology_changed THEN 1 ELSE 0 END,

        -- A new invalidation represents new work and revives a task that may
        -- previously have exhausted its retry count.
        fail_count = CASE WHEN topology_changed THEN 0 ELSE task.fail_count END;

    -- An old alias topology may have left non-canonical tasks behind.
    -- Invalidate those too, so a running obsolete task can never publish a
    -- result that predates a mutation affecting its current alias group.
    UPDATE reconcile_tag_matching_counts_task AS task
    SET
        reconcile_post_count =
            task.reconcile_post_count
            OR post_count_dirty,

        reconcile_post_collection_count =
            task.reconcile_post_collection_count
            OR post_collection_count_dirty,

        reconcile_user_group_count =
            task.reconcile_user_group_count
            OR user_group_count_dirty,

        invalidation_version =
            task.invalidation_version + 1,

        topology_version =
            task.topology_version + CASE WHEN topology_changed THEN 1 ELSE 0 END,

        fail_count = CASE WHEN topology_changed THEN 0 ELSE task.fail_count END
    WHERE task.fk_tag = ANY(group_tag_pks)
      AND NOT (task.fk_tag = ANY(canonical_tag_pks));
END;
$BODY$
LANGUAGE plpgsql;


-- A direct usage change affects the directly changed tag and every query tag
-- for which that tag is a descendant.
--
-- tag_closure_table contains self references, but the explicit changed-tag
-- branch is retained so this remains robust even if that implementation
-- detail changes.
--
-- Alias-group expansion and canonicalization are handled by
-- create_reconcile_tag_matching_counts_tasks().
CREATE FUNCTION enqueue_reconcile_tag_matching_counts_for_usage(
    changed_tag_pks BIGINT[],
    post_count_dirty BOOLEAN,
    post_collection_count_dirty BOOLEAN,
    user_group_count_dirty BOOLEAN
) RETURNS VOID AS
$BODY$
DECLARE
    affected_tag_pks BIGINT[];
BEGIN
    WITH changed_tags AS (
        SELECT DISTINCT pk
        FROM unnest(changed_tag_pks) AS changed_tag(pk)
    ),
    affected_tags AS (
        SELECT pk AS fk_tag
        FROM changed_tags

        UNION

        SELECT tag_closure_table.fk_parent
        FROM tag_closure_table
        INNER JOIN changed_tags
            ON changed_tags.pk = tag_closure_table.fk_child
    )
    SELECT COALESCE(
        array_agg(fk_tag),
        ARRAY[]::BIGINT[]
           )
    INTO affected_tag_pks
    FROM affected_tags;

    PERFORM create_reconcile_tag_matching_counts_tasks(
        affected_tag_pks,
        post_count_dirty,
        post_collection_count_dirty,
        user_group_count_dirty,
        FALSE
    );
END;
$BODY$
LANGUAGE plpgsql;


-- Generic tag-usage trigger functions.

CREATE FUNCTION enqueue_reconcile_tag_matching_counts_usage_insert_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM enqueue_reconcile_tag_matching_counts_for_usage(
        ARRAY(
            SELECT DISTINCT fk_tag
            FROM new_rows
        ),
        TG_ARGV[0]::BOOLEAN,
        TG_ARGV[1]::BOOLEAN,
        TG_ARGV[2]::BOOLEAN
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE FUNCTION enqueue_reconcile_tag_matching_counts_usage_delete_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM enqueue_reconcile_tag_matching_counts_for_usage(
        ARRAY(
            SELECT DISTINCT fk_tag
            FROM old_rows
        ),
        TG_ARGV[0]::BOOLEAN,
        TG_ARGV[1]::BOOLEAN,
        TG_ARGV[2]::BOOLEAN
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE FUNCTION enqueue_reconcile_tag_matching_counts_usage_update_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM enqueue_reconcile_tag_matching_counts_for_usage(
        ARRAY[OLD.fk_tag, NEW.fk_tag],
        TG_ARGV[0]::BOOLEAN,
        TG_ARGV[1]::BOOLEAN,
        TG_ARGV[2]::BOOLEAN
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


-- post_tag

CREATE TRIGGER after_insert_post_tag_reconcile_matching_counts
    AFTER INSERT ON post_tag
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_insert_trigger(
        'true',
        'false',
        'false'
    );

CREATE TRIGGER after_delete_post_tag_reconcile_matching_counts
    AFTER DELETE ON post_tag
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_delete_trigger(
        'true',
        'false',
        'false'
    );

CREATE TRIGGER after_update_post_tag_reconcile_matching_counts
    AFTER UPDATE ON post_tag
    FOR EACH ROW
    WHEN (
        OLD.fk_post IS DISTINCT FROM NEW.fk_post
        OR OLD.fk_tag IS DISTINCT FROM NEW.fk_tag
    )
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_update_trigger(
        'true',
        'false',
        'false'
    );


-- post_collection_tag

CREATE TRIGGER after_insert_post_collection_tag_reconcile_matching_counts
    AFTER INSERT ON post_collection_tag
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_insert_trigger(
        'false',
        'true',
        'false'
    );

CREATE TRIGGER after_delete_post_collection_tag_reconcile_matching_counts
    AFTER DELETE ON post_collection_tag
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_delete_trigger(
        'false',
        'true',
        'false'
    );

CREATE TRIGGER after_update_post_collection_tag_reconcile_matching_counts
    AFTER UPDATE ON post_collection_tag
    FOR EACH ROW
    WHEN (
        OLD.fk_post_collection IS DISTINCT FROM NEW.fk_post_collection
        OR OLD.fk_tag IS DISTINCT FROM NEW.fk_tag
    )
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_update_trigger(
        'false',
        'true',
        'false'
    );


-- user_group_tag

CREATE TRIGGER after_insert_user_group_tag_reconcile_matching_counts
    AFTER INSERT ON user_group_tag
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_insert_trigger(
        'false',
        'false',
        'true'
    );

CREATE TRIGGER after_delete_user_group_tag_reconcile_matching_counts
    AFTER DELETE ON user_group_tag
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_delete_trigger(
        'false',
        'false',
        'true'
    );

CREATE TRIGGER after_update_user_group_tag_reconcile_matching_counts
    AFTER UPDATE ON user_group_tag
    FOR EACH ROW
    WHEN (
        OLD.fk_user_group IS DISTINCT FROM NEW.fk_user_group
        OR OLD.fk_tag IS DISTINCT FROM NEW.fk_tag
    )
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_usage_update_trigger(
        'false',
        'false',
        'true'
    );


-- tag_closure_table
--
-- Each inserted/deleted closure row directly identifies a query tag whose
-- effective descendant set changed: fk_parent.
--
-- Alias hierarchy is already propagated into tag_closure_table, so no
-- independent ancestor traversal is required here.

CREATE FUNCTION enqueue_reconcile_tag_matching_counts_closure_insert_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM create_reconcile_tag_matching_counts_tasks(
        ARRAY(
            SELECT DISTINCT fk_parent
            FROM new_rows
        ),
        TRUE,
        TRUE,
        TRUE,
        TRUE
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE FUNCTION enqueue_reconcile_tag_matching_counts_closure_delete_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM create_reconcile_tag_matching_counts_tasks(
        ARRAY(
            SELECT DISTINCT fk_parent
            FROM old_rows
        ),
        TRUE,
        TRUE,
        TRUE,
        TRUE
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE FUNCTION enqueue_reconcile_tag_matching_counts_closure_update_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    IF OLD.fk_parent IS DISTINCT FROM NEW.fk_parent
       OR OLD.fk_child IS DISTINCT FROM NEW.fk_child
    THEN
        PERFORM create_reconcile_tag_matching_counts_tasks(
            ARRAY[OLD.fk_parent, NEW.fk_parent],
            TRUE,
            TRUE,
            TRUE,
            TRUE
        );
    END IF;

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE TRIGGER after_insert_tag_closure_reconcile_matching_counts
    AFTER INSERT ON tag_closure_table
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_closure_insert_trigger();

CREATE TRIGGER after_delete_tag_closure_reconcile_matching_counts
    AFTER DELETE ON tag_closure_table
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_closure_delete_trigger();

CREATE TRIGGER after_update_tag_closure_reconcile_matching_counts
    AFTER UPDATE ON tag_closure_table
    FOR EACH ROW
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_closure_update_trigger();


-- tag_alias
--
-- Only AFTER triggers are necessary.
--
-- If an alias change commits before a worker finalizes, it increments
-- topology_version for every existing task in each affected current group.
--
-- If the worker finalizes first, the alias transaction subsequently creates
-- or updates the new canonical reconciliation task before it can commit.
--
-- PostgreSQL statement-level AFTER triggers see the completed statement, so
-- these functions resolve alias groups using the post-change topology.

CREATE FUNCTION enqueue_reconcile_tag_matching_counts_alias_insert_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM create_reconcile_tag_matching_counts_tasks(
        ARRAY(
            SELECT fk_source
            FROM new_rows

            UNION

            SELECT fk_target
            FROM new_rows
        ),
        TRUE,
        TRUE,
        TRUE,
        TRUE
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE FUNCTION enqueue_reconcile_tag_matching_counts_alias_delete_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM create_reconcile_tag_matching_counts_tasks(
        ARRAY(
            SELECT fk_source
            FROM old_rows

            UNION

            SELECT fk_target
            FROM old_rows
        ),
        TRUE,
        TRUE,
        TRUE,
        TRUE
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE FUNCTION enqueue_reconcile_tag_matching_counts_alias_update_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM create_reconcile_tag_matching_counts_tasks(
        ARRAY(
            SELECT fk_source
            FROM old_rows

            UNION

            SELECT fk_target
            FROM old_rows

            UNION

            SELECT fk_source
            FROM new_rows

            UNION

            SELECT fk_target
            FROM new_rows
        ),
        TRUE,
        TRUE,
        TRUE,
        TRUE
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE TRIGGER after_insert_tag_alias_reconcile_matching_counts
    AFTER INSERT ON tag_alias
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_alias_insert_trigger();

CREATE TRIGGER after_delete_tag_alias_reconcile_matching_counts
    AFTER DELETE ON tag_alias
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_alias_delete_trigger();

CREATE TRIGGER after_update_tag_alias_reconcile_matching_counts
    AFTER UPDATE ON tag_alias
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_alias_update_trigger();


-- A rename does not itself change the semantic count, but the reconciliation
-- compiler currently identifies a tag by tag_name. Invalidating the current
-- alias group prevents an in-flight reconciliation compiled from an old name
-- from being accepted as current.
CREATE FUNCTION enqueue_reconcile_tag_matching_counts_tag_name_update_trigger()
RETURNS TRIGGER AS
$BODY$
BEGIN
    PERFORM create_reconcile_tag_matching_counts_tasks(
        ARRAY[NEW.pk],
        TRUE,
        TRUE,
        TRUE,
        TRUE
    );

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;


CREATE TRIGGER after_update_tag_name_reconcile_matching_counts
    AFTER UPDATE OF tag_name ON tag
    FOR EACH ROW
    WHEN (OLD.tag_name IS DISTINCT FROM NEW.tag_name)
    EXECUTE FUNCTION enqueue_reconcile_tag_matching_counts_tag_name_update_trigger();


-- Initial one-time backfill.
--
-- The enqueue helper canonicalizes existing alias groups, so this creates one
-- normal initial task per alias group.
SELECT create_reconcile_tag_matching_counts_tasks(
    ARRAY(
        SELECT pk
        FROM tag
    ),
    TRUE,
    TRUE,
    TRUE,
    TRUE
);
