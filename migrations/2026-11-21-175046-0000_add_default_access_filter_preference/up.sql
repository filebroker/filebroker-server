CREATE TYPE access_filter AS ENUM ('all', 'shared', 'own');

ALTER TABLE user_preferences ADD COLUMN default_access_filter access_filter NOT NULL DEFAULT 'all';
