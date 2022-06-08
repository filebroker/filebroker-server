CREATE TABLE post (
    pk SERIAL PRIMARY KEY,
    data_url VARCHAR(2048) NOT NULL,
    source_url VARCHAR(2048) NULL,
    title VARCHAR(300) NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    fk_create_user INTEGER REFERENCES registered_user(pk) NOT NULL
);

CREATE TABLE tag (
    pk SERIAL PRIMARY KEY,
    tag_name VARCHAR(50) UNIQUE NOT NULL,
    fk_parent INTEGER REFERENCES tag(pk) NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE post_tag (
    fk_post INTEGER REFERENCES post(pk) NOT NULL,
    fk_tag INTEGER REFERENCES tag(pk) NOT NULL,
    PRIMARY KEY(fk_post, fk_tag)
);

CREATE TABLE tag_alias (
    fk_source INTEGER REFERENCES tag(pk) NOT NULL,
    fk_target INTEGER REFERENCES tag(pk) NOT NULL,
    PRIMARY KEY(fk_source, fk_target)
);
