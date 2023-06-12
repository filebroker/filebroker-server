CREATE UNIQUE INDEX user_name_unique_idx_case_insensitive ON registered_user(LOWER(user_name));
