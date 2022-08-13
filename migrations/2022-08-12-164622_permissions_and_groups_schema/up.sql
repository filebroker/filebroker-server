CREATE TABLE user_group (
    pk SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    public BOOLEAN NOT NULL DEFAULT false,
    hidden BOOLEAN NOT NULL DEFAULT false,
    fk_owner INTEGER REFERENCES registered_user(pk) NOT NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE user_group_membership (
    fk_group INTEGER REFERENCES user_group(pk) NOT NULL,
    fk_user INTEGER REFERENCES registered_user(pk) NOT NULL,
    administrator BOOLEAN NOT NULL DEFAULT false,
    revoked BOOLEAN NOT NULL DEFAULT false,
    fk_granted_by INTEGER REFERENCES registered_user(pk) NOT NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY(fk_group, fk_user)
);

CREATE TABLE post_collection (
    pk SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    fk_owner INTEGER REFERENCES registered_user(pk) NOT NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE post_collection_item (
    fk_post INTEGER REFERENCES post(pk) NOT NULL,
    fk_post_collection INTEGER REFERENCES post_collection(pk) NOT NULL,
    fk_added_by INTEGER REFERENCES registered_user(pk) NOT NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY(fk_post, fk_post_collection)
);

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
