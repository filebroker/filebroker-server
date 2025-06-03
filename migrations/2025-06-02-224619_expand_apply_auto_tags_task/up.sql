ALTER TABLE apply_auto_tags_task ADD COLUMN locked_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE apply_auto_tags_task ADD COLUMN fail_count INTEGER NOT NULL DEFAULT 0;
