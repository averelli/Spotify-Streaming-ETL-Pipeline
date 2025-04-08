INSERT INTO core.dim_reason (reason_type, reason_group)
SELECT DISTINCT reason_start AS reason_type, 'start' AS reason_group FROM staging.streaming_history
UNION ALL
SELECT DISTINCT reason_end, 'end' AS reason_group FROM staging.streaming_history
ON CONFLICT DO NOTHING;