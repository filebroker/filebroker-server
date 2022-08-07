ALTER TABLE post ALTER COLUMN data_url DROP NOT NULL;
UPDATE post SET data_url = NULL WHERE data_url = '';
