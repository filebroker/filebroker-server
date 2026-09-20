-- Create fulltext tables with search_vector TSVECTOR for post search, collection search and group search

CREATE TABLE post_search_index (
    fk_post BIGINT PRIMARY KEY REFERENCES post(pk) ON DELETE CASCADE,

    search_text TEXT NOT NULL,

    search_vector TSVECTOR
        GENERATED ALWAYS AS (
            to_tsvector('simple', search_text)
        ) STORED
);

CREATE INDEX post_search_index_search_vector_idx
ON post_search_index
USING GIN (search_vector);

CREATE INDEX post_search_index_search_text_gin_idx
ON post_search_index
USING GIN (search_text gin_trgm_ops);


CREATE TABLE post_collection_search_index (
    fk_post_collection BIGINT PRIMARY KEY REFERENCES post_collection(pk) ON DELETE CASCADE,

    search_text TEXT NOT NULL,

    search_vector TSVECTOR
        GENERATED ALWAYS AS (
            to_tsvector('simple', search_text)
        ) STORED
);

CREATE INDEX post_collection_search_index_search_vector_idx
ON post_collection_search_index
USING GIN (search_vector);

CREATE INDEX post_collection_search_index_search_text_gin_idx
ON post_collection_search_index
USING GIN (search_text gin_trgm_ops);


CREATE TABLE user_group_search_index (
    fk_user_group BIGINT PRIMARY KEY REFERENCES user_group(pk) ON DELETE CASCADE,

    search_text TEXT NOT NULL,

    search_vector TSVECTOR
        GENERATED ALWAYS AS (
            to_tsvector('simple', search_text)
        ) STORED
);

CREATE INDEX user_group_search_index_search_vector_idx
ON user_group_search_index
USING GIN (search_vector);

CREATE INDEX user_group_search_index_search_text_gin_idx
ON user_group_search_index
USING GIN (search_text gin_trgm_ops);

-- Create triggers to update fulltext vectors

CREATE FUNCTION update_post_search_index(target_post_pk BIGINT)
RETURNS VOID
LANGUAGE SQL
AS $$
    INSERT INTO post_search_index (
        fk_post,
        search_text
    )
    SELECT
        post.pk,
        concat_ws(
            ' ',
            post.title,
            post.description,
            s3_object_metadata.artist,
            s3_object_metadata.album,
            s3_object_metadata.composer,
            s3_object_metadata.genre
        )
    FROM post
    LEFT JOIN s3_object_metadata
        ON s3_object_metadata.object_key = post.s3_object
    WHERE post.pk = target_post_pk
    ON CONFLICT (fk_post)
    DO UPDATE SET
        search_text = EXCLUDED.search_text;
$$;

CREATE FUNCTION update_post_search_index_from_post()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM update_post_search_index(NEW.pk);
    RETURN NEW;
END;
$$;

CREATE TRIGGER post_update_search_index
AFTER INSERT OR UPDATE OF title, description, s3_object
ON post
FOR EACH ROW
EXECUTE FUNCTION update_post_search_index_from_post();


CREATE FUNCTION update_post_search_index_from_s3_object_metadata()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_post_pk BIGINT;
    target_object_key TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        target_object_key := OLD.object_key;
    ELSE
        target_object_key := NEW.object_key;
    END IF;

    FOR target_post_pk IN
        SELECT post.pk
        FROM post
        WHERE post.s3_object = target_object_key
    LOOP
        PERFORM update_post_search_index(target_post_pk);
    END LOOP;

    RETURN NULL;
END;
$$;

CREATE TRIGGER s3_object_metadata_update_post_search_index
AFTER INSERT
   OR DELETE
   OR UPDATE OF artist, album, composer, genre
ON s3_object_metadata
FOR EACH ROW
EXECUTE FUNCTION update_post_search_index_from_s3_object_metadata();


CREATE FUNCTION update_post_collection_search_index()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO post_collection_search_index (
        fk_post_collection,
        search_text
    )
    VALUES (
        NEW.pk,
        concat_ws(
            ' ',
            NEW.title,
            NEW.description
        )
    )
    ON CONFLICT (fk_post_collection)
    DO UPDATE SET
        search_text = EXCLUDED.search_text;

    RETURN NEW;
END;
$$;

CREATE TRIGGER post_collection_update_search_index
AFTER INSERT OR UPDATE OF title, description
ON post_collection
FOR EACH ROW
EXECUTE FUNCTION update_post_collection_search_index();


CREATE FUNCTION update_user_group_search_index()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO user_group_search_index (
        fk_user_group,
        search_text
    )
    VALUES (
        NEW.pk,
        concat_ws(
            ' ',
            NEW.name,
            NEW.description
        )
    )
    ON CONFLICT (fk_user_group)
    DO UPDATE SET
        search_text = EXCLUDED.search_text;

    RETURN NEW;
END;
$$;

CREATE TRIGGER user_group_update_search_index
AFTER INSERT OR UPDATE OF name, description
ON user_group
FOR EACH ROW
EXECUTE FUNCTION update_user_group_search_index();

-- Insert initial fulltext table data for existing rows

INSERT INTO post_collection_search_index (
    fk_post_collection,
    search_text
)
SELECT
    pk,
    concat_ws(
        ' ',
        title,
        description
    )
FROM post_collection;

INSERT INTO user_group_search_index (
    fk_user_group,
    search_text
)
SELECT
    pk,
    concat_ws(
        ' ',
        name,
        description
    )
FROM user_group;

INSERT INTO post_search_index (
    fk_post,
    search_text
)
SELECT
    post.pk,
    concat_ws(
        ' ',
        post.title,
        post.description,
        s3_object_metadata.artist,
        s3_object_metadata.album,
        s3_object_metadata.composer,
        s3_object_metadata.genre
    )
FROM post
LEFT JOIN s3_object_metadata ON s3_object_metadata.object_key = post.s3_object;

--- Ensure trigram and tsvector indexes exist on relevant columns

CREATE INDEX s3_object_metadata_composer_idx
ON s3_object_metadata (lower(composer));
CREATE INDEX s3_object_metadata_composer_gin_idx
ON s3_object_metadata
USING GIN (composer gin_trgm_ops);
-- post
CREATE INDEX post_title_fts_idx
ON post
USING GIN (
    to_tsvector('simple', coalesce(title, ''))
);

CREATE INDEX post_description_fts_idx
ON post
USING GIN (
    to_tsvector('simple', coalesce(description, ''))
);
-- s3_object_metadata
CREATE INDEX s3_object_metadata_mime_type_fts_idx
ON s3_object_metadata
USING GIN (
    to_tsvector('simple', coalesce(mime_type, ''))
);

CREATE INDEX s3_object_metadata_artist_fts_idx
ON s3_object_metadata
USING GIN (
    to_tsvector('simple', coalesce(artist, ''))
);

CREATE INDEX s3_object_metadata_album_fts_idx
ON s3_object_metadata
USING GIN (
    to_tsvector('simple', coalesce(album, ''))
);

CREATE INDEX s3_object_metadata_composer_fts_idx
ON s3_object_metadata
USING GIN (
    to_tsvector('simple', coalesce(composer, ''))
);

CREATE INDEX s3_object_metadata_genre_fts_idx
ON s3_object_metadata
USING GIN (
    to_tsvector('simple', coalesce(genre, ''))
);
-- post_collection
CREATE INDEX post_collection_title_fts_idx
ON post_collection
USING GIN (
    to_tsvector('simple', coalesce(title, ''))
);

CREATE INDEX post_collection_description_fts_idx
ON post_collection
USING GIN (
    to_tsvector('simple', coalesce(description, ''))
);
-- user_group
CREATE INDEX user_group_name_fts_idx
ON user_group
USING GIN (
    to_tsvector('simple', coalesce(name, ''))
);

CREATE INDEX user_group_description_fts_idx
ON user_group
USING GIN (
    to_tsvector('simple', coalesce(description, ''))
);

-- Replace lower() GIN indexes with normal ones (and use ILIKE instead)

DROP INDEX tag_name_gin_idx;
CREATE INDEX tag_name_gin_idx
    ON tag USING GIN (tag_name gin_trgm_ops);


DROP INDEX post_title_gin_idx;
CREATE INDEX post_title_gin_idx
    ON post USING GIN (title gin_trgm_ops);

DROP INDEX post_description_gin_idx;
CREATE INDEX post_description_gin_idx
    ON post USING GIN (description gin_trgm_ops);


DROP INDEX post_collection_title_gin_idx;
CREATE INDEX post_collection_title_gin_idx
    ON post_collection USING GIN (title gin_trgm_ops);

DROP INDEX post_collection_description_gin_idx;
CREATE INDEX post_collection_description_gin_idx
    ON post_collection USING GIN (description gin_trgm_ops);


DROP INDEX user_group_name_gin_idx;
CREATE INDEX user_group_name_gin_idx
    ON user_group USING GIN (name gin_trgm_ops);

DROP INDEX user_group_description_gin_idx;
CREATE INDEX user_group_description_gin_idx
    ON user_group USING GIN (description gin_trgm_ops);


DROP INDEX s3_object_metadata_artist_gin_idx;
CREATE INDEX s3_object_metadata_artist_gin_idx
    ON s3_object_metadata USING GIN (artist gin_trgm_ops);

DROP INDEX s3_object_metadata_album_gin_idx;
CREATE INDEX s3_object_metadata_album_gin_idx
    ON s3_object_metadata USING GIN (album gin_trgm_ops);

DROP INDEX s3_object_metadata_genre_gin_idx;
CREATE INDEX s3_object_metadata_genre_gin_idx
    ON s3_object_metadata USING GIN (genre gin_trgm_ops);

DROP INDEX s3_object_metadata_mime_type_gin_idx;
CREATE INDEX s3_object_metadata_mime_type_gin_idx
    ON s3_object_metadata USING GIN (mime_type gin_trgm_ops);
