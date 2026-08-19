CREATE TABLE notification_log (
    id              UUID        PRIMARY KEY,
    event_id        UUID        NOT NULL,
    event_type      VARCHAR(80) NOT NULL,
    recipient       VARCHAR(320) NOT NULL,
    subject         VARCHAR(500) NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    correlation_id  UUID
);

CREATE INDEX idx_notification_log_event_id ON notification_log(event_id);
