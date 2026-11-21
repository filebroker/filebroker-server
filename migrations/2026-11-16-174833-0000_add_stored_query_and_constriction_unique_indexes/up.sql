CREATE UNIQUE INDEX stored_query_unique_name_idx
    ON stored_query (fk_user, scope, lower(name))
    WHERE is_saved;

CREATE UNIQUE INDEX stored_constriction_unique_name_idx
    ON stored_constriction (fk_user, scope, lower(name))
    NULLS NOT DISTINCT;
