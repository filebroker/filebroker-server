CREATE TABLE permission_target (
    pk SERIAL PRIMARY KEY,
    public BOOLEAN NOT NULL DEFAULT false,
    administrator BOOLEAN NOT NULL DEFAULT false,
    fk_granted_group INTEGER REFERENCES user_group(pk),
    quota BIGINT,
    fk_post INTEGER REFERENCES post(pk),
    fk_broker INTEGER REFERENCES broker(pk),
    fk_post_collection INTEGER REFERENCES post_collection(pk),
    fk_granted_by INTEGER REFERENCES registered_user(pk),
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO permission_target(public, administrator, fk_granted_group, quota, fk_broker, fk_granted_by, creation_timestamp) SELECT public, write, fk_granted_group, quota, fk_broker, fk_granted_by, creation_timestamp FROM broker_access;

DROP TABLE broker_access;

INSERT INTO permission_target(public, fk_post_collection, creation_timestamp) SELECT true, pk, creation_timestamp FROM post_collection WHERE public;
ALTER TABLE post_collection DROP COLUMN public;

INSERT INTO permission_target(administrator, fk_granted_group, fk_post_collection, fk_granted_by, creation_timestamp) SELECT write, fk_granted_group, fk_post_collection, fk_granted_by, creation_timestamp FROM post_collection_group_access;
DROP TABLE post_collection_group_access;

INSERT INTO permission_target(public, fk_post, creation_timestamp) SELECT true, pk, creation_timestamp FROM post where public;
ALTER TABLE post DROP COLUMN public;

INSERT INTO permission_target(administrator, fk_granted_group, fk_post, fk_granted_by, creation_timestamp) SELECT write, fk_granted_group, fk_post, fk_granted_by, creation_timestamp FROM post_group_access;
DROP TABLE post_group_access;
