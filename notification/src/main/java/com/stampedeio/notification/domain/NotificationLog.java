package com.stampedeio.notification.domain;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "notification_log")
public class NotificationLog {

    @Id
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "event_type", nullable = false, length = 80)
    private String eventType;

    @Column(nullable = false, length = 320)
    private String recipient;

    @Column(nullable = false, length = 500)
    private String subject;

    @Column(name = "sent_at", nullable = false, updatable = false)
    private Instant sentAt = Instant.now();

    @Column(name = "correlation_id")
    private UUID correlationId;

    protected NotificationLog() {
    }

    public NotificationLog(UUID eventId, String eventType, String recipient,
                           String subject, UUID correlationId) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.eventType = eventType;
        this.recipient = recipient;
        this.subject = subject;
        this.correlationId = correlationId;
    }

    public UUID getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getRecipient() {
        return recipient;
    }

    public String getSubject() {
        return subject;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }
}
