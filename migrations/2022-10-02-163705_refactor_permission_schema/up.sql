CREATE TABLE post_group_access (
    fk_post INTEGER NOT NULL REFERENCES post(pk),
    fk_granted_group INTEGER NOT NULL REFERENCES user_group(pk),
    write BOOLEAN NOT NULL DEFAULT FALSE,
    fk_granted_by INTEGER NOT NULL REFERENCES registered_user(pk),
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY(fk_post, fk_granted_group)
);

ALTER TABLE post ADD COLUMN public BOOLEAN NOT NULL DEFAULT FALSE;

INSERT INTO post_group_access SELECT fk_post, fk_granted_group, administrator, fk_granted_by, creation_timestamp FROM permission_target WHERE fk_post IS NOT NULL AND fk_granted_group IS NOT NULL AND fk_granted_by IS NOT NULL;
UPDATE post SET public = EXISTS(SELECT * FROM permission_target WHERE fk_post = post.pk AND public);

CREATE TABLE post_collection_group_access (
    fk_post_collection INTEGER NOT NULL REFERENCES post_collection(pk),
    fk_granted_group INTEGER NOT NULL REFERENCES user_group(pk),
    write BOOLEAN NOT NULL DEFAULT FALSE,
    fk_granted_by INTEGER NOT NULL REFERENCES registered_user(pk),
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY(fk_post_collection, fk_granted_group)
);

ALTER TABLE post_collection ADD COLUMN public BOOLEAN NOT NULL DEFAULT FALSE;

INSERT INTO post_group_access SELECT fk_post_collection, fk_granted_group, administrator, fk_granted_by, creation_timestamp FROM permission_target WHERE fk_post_collection IS NOT NULL AND fk_granted_group IS NOT NULL AND fk_granted_by IS NOT NULL;
UPDATE post_collection SET public = EXISTS(SELECT * FROM permission_target where fk_post_collection = post_collection.pk AND public);

CREATE TABLE broker_access (
    pk SERIAL PRIMARY KEY,
    fk_broker INTEGER NOT NULL REFERENCES broker(pk),
    fk_granted_group INTEGER REFERENCES user_group(pk),
    write BOOLEAN NOT NULL DEFAULT FALSE,
    public BOOLEAN NOT NULL DEFAULT FALSE,
    quota BIGINT,
    fk_granted_by INTEGER NOT NULL REFERENCES registered_user(pk),
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO broker_access(fk_broker, fk_granted_group, write, public, quota, fk_granted_by, creation_timestamp) SELECT fk_broker, fk_granted_group, administrator, public, quota, fk_granted_by, creation_timestamp FROM permission_target WHERE fk_broker IS NOT NULL AND fk_granted_by IS NOT NULL;

DROP TABLE permission_target;
