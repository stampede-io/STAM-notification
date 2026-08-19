package com.stampedeio.notification.consumer;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.stampedeio.notification.service.IdempotencyService;
import com.stampedeio.notification.service.NotificationService;

@Component
public class BookingNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookingNotificationConsumer.class);

    private final IdempotencyService idempotencyService;
    private final NotificationService notificationService;

    public BookingNotificationConsumer(IdempotencyService idempotencyService,
                                       NotificationService notificationService) {
        this.idempotencyService = idempotencyService;
        this.notificationService = notificationService;
    }

    @KafkaListener(topics = "reservations.events", groupId = "notification-service")
    public void consume(Map<String, Object> message) {
        String eventType = (String) message.get("eventType");
        UUID eventId = UUID.fromString((String) message.get("eventId"));
        UUID correlationId = parseUuid((String) message.get("correlationId"));

        MDC.put("correlationId", correlationId != null ? correlationId.toString() : "unknown");
        MDC.put("service", "notification");
        try {
            if (!isRelevantEvent(eventType)) {
                log.debug("Ignoring event type: {}", eventType);
                return;
            }

            if (!idempotencyService.tryMarkProcessed(eventId, eventType)) {
                log.info("Duplicate event detected, skipping: eventId={} type={}", eventId, eventType);
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) message.get("payload");
            String reservationId = (String) payload.get("reservationId");
            String showId = (String) payload.get("showId");
            @SuppressWarnings("unchecked")
            List<String> seatIds = (List<String>) payload.get("seatIds");

            switch (eventType) {
                case "ReservationConfirmed" ->
                    notificationService.sendConfirmation(eventId, correlationId,
                            reservationId, showId, seatIds);
                case "SeatsReleased" ->
                    notificationService.sendCancellation(eventId, correlationId,
                            reservationId, showId, seatIds);
                case "HoldExpired" ->
                    notificationService.sendExpiry(eventId, correlationId,
                            reservationId, showId, seatIds);
                default -> log.warn("Unhandled event type: {}", eventType);
            }
        } finally {
            MDC.remove("correlationId");
            MDC.remove("service");
        }
    }

    private boolean isRelevantEvent(String eventType) {
        return "ReservationConfirmed".equals(eventType)
                || "SeatsReleased".equals(eventType)
                || "HoldExpired".equals(eventType);
    }

    private static UUID parseUuid(String value) {
        if (value == null) return null;
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
