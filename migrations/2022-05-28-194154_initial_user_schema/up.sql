CREATE TABLE registered_user(
    pk SERIAL PRIMARY KEY,
    user_name VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    email VARCHAR(320) NULL,
    avatar_url VARCHAR(2048) NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE refresh_token(
    pk SERIAL PRIMARY KEY,
    uuid UUID UNIQUE NOT NULL,
    expiry TIMESTAMP WITH TIME ZONE NOT NULL,
    invalidated BOOLEAN NOT NULL,
    fk_registered_user INTEGER REFERENCES registered_user(pk) NOT NULL
);
