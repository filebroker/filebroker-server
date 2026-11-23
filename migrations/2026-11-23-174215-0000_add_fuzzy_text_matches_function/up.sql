-- Word-level typo-tolerant fallback for multiword full-text searches.
-- Requires PostgreSQL's bundled fuzzystrmatch extension.
CREATE EXTENSION fuzzystrmatch;

CREATE FUNCTION fuzzy_text_matches(
    search_term TEXT,
    candidate_text TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    query_words TEXT[];
    candidate_words TEXT[];
    query_word TEXT;
    candidate_word TEXT;
    max_distance INTEGER;
    matched BOOLEAN;
BEGIN
    query_words := array_remove(
        regexp_split_to_array(lower(search_term), '\W+'), ''
    );
    candidate_words := array_remove(
        regexp_split_to_array(lower(candidate_text), '\W+'), ''
    );

    IF cardinality(query_words) = 0 THEN
        RETURN FALSE;
    END IF;

    -- The search is a conjunction of independently typo-tolerant words.
    -- Their order and position within the indexed text do not matter.
    FOREACH query_word IN ARRAY query_words LOOP
        max_distance := CASE
            WHEN length(query_word) <= 2 THEN 0
            WHEN length(query_word) <= 7 THEN 1
            ELSE 2
        END;

        matched := FALSE;

        FOREACH candidate_word IN ARRAY candidate_words LOOP
            IF query_word = candidate_word THEN
                matched := TRUE;
                EXIT;
            END IF;

            -- fuzzystrmatch's levenshtein functions accept strings
            -- of at most 255 characters; long exact matches work above.
            IF abs(length(query_word) - length(candidate_word)) <= max_distance
                AND length(query_word) <= 255
                AND length(candidate_word) <= 255
                AND levenshtein_less_equal(
                    query_word,
                    candidate_word,
                    max_distance
                ) <= max_distance
            THEN
                matched := TRUE;
                EXIT;
            END IF;
        END LOOP;

        IF NOT matched THEN
            RETURN FALSE;
        END IF;
    END LOOP;

    RETURN TRUE;
END;
$$;
