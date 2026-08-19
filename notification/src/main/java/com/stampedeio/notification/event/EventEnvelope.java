package com.stampedeio.notification.event;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String eventType,
        int version,
        Instant occurredAt,
        UUID correlationId,
        UUID aggregateId,
        Object payload
) {
}
