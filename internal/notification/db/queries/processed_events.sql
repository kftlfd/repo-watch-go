-- name: ClaimProcessedEvent :execrows
INSERT INTO processed_events (event_id, event_type)
VALUES (@event_id, @event_type)
ON CONFLICT (event_id) DO NOTHING;
