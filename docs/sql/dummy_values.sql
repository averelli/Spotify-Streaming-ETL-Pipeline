-- dim_episode
INSERT INTO core.dim_episode (episode_id, spotify_episode_uri, duration_ms, duration_sec, podcast_name, spotify_podcast_uri, release_date)
VALUES (0, 'unknown', NULL, NULL, 'Unknown Podcast', 'unknown', '1900-01-01')
ON CONFLICT DO NOTHING;

-- dim_podcast
INSERT INTO core.dim_podcast (podcast_id, spotify_podcast_uri, podcast_name, description, podcast_cover_art_url)
VALUES (0, 'unknown', 'Unknown Show', 'Unknown podcast show', NULL)
ON CONFLICT DO NOTHING;