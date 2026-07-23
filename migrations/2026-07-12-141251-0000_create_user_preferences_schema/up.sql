CREATE TABLE user_preferences(
    fk_user BIGINT PRIMARY KEY REFERENCES registered_user(pk),
    advanced_query_mode BOOLEAN NOT NULL DEFAULT FALSE,
    auto_play_audio BOOLEAN NOT NULL DEFAULT FALSE,
    auto_play_video BOOLEAN NOT NULL DEFAULT FALSE,
    auto_play_audio_in_collection BOOLEAN NOT NULL DEFAULT TRUE,
    auto_play_video_in_collection BOOLEAN NOT NULL DEFAULT TRUE
);
