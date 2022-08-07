UPDATE post SET data_url = '' WHERE data_url IS NULL;
ALTER TABLE post ALTER COLUMN data_url SET NOT NULL;
