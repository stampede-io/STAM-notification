package com.stampedeio.notification.consumer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stampedeio.notification.service.IdempotencyService;
import com.stampedeio.notification.service.NotificationService;

@Component
public class BookingNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookingNotificationConsumer.class);

    private final IdempotencyService idempotencyService;
    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    public BookingNotificationConsumer(IdempotencyService idempotencyService,
                                       NotificationService notificationService,
                                       ObjectMapper objectMapper) {
        this.idempotencyService = idempotencyService;
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "reservations.events", groupId = "notification-service")
    public void consume(Map<String, Object> message) {
        String eventType = (String) message.get("eventType");
        String eventIdStr = (String) message.get("eventId");

        if (eventIdStr == null) {
            log.warn("Received message with no eventId, skipping: {}", message);
            return;
        }

        UUID eventId = parseUuid(eventIdStr);
        if (eventId == null) {
            log.warn("Received message with malformed eventId={}, skipping", eventIdStr);
            return;
        }

        UUID correlationId = parseUuid((String) message.get("correlationId"));

        MDC.put("correlationId", correlationId != null ? correlationId.toString() : "unknown");
        MDC.put("service", "notification");
        try {
            if (!isRelevantEvent(eventType)) {
                log.debug("Ignoring event type: {}", eventType);
                return;
            }

            if (idempotencyService.isAlreadyProcessed(eventId)) {
                log.info("Duplicate event detected, skipping: eventId={} type={}", eventId, eventType);
                return;
            }

            Map<String, Object> payload = extractPayload(message.get("payload"));
            if (payload == null) {
                log.warn("Null or unparseable payload for eventId={}, skipping", eventId);
                return;
            }

            String reservationId = (String) payload.get("reservationId");
            String showId = (String) payload.get("showId");
            @SuppressWarnings("unchecked")
            List<String> seatIds = (List<String>) payload.get("seatIds");
            if (seatIds == null) {
                seatIds = Collections.emptyList();
            }

            switch (eventType) {
                case "ReservationConfirmed" ->
                    notificationService.sendNotification(eventId, correlationId,
                            reservationId, showId, seatIds,
                            "email/confirmation", "Booking Confirmed", "ReservationConfirmed");
                case "SeatsReleased" ->
                    notificationService.sendNotification(eventId, correlationId,
                            reservationId, showId, seatIds,
                            "email/cancellation", "Booking Cancelled", "SeatsReleased");
                case "HoldExpired" ->
                    notificationService.sendNotification(eventId, correlationId,
                            reservationId, showId, seatIds,
                            "email/expiry", "Hold Expired", "HoldExpired");
                default -> log.warn("Unhandled event type: {}", eventType);
            }

            idempotencyService.markProcessed(eventId, eventType);
        } catch (Exception e) {
            log.error("Failed to process event eventId={} type={}: {}",
                    eventId, eventType, e.getMessage(), e);
        } finally {
            MDC.remove("correlationId");
            MDC.remove("service");
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractPayload(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof Map) {
            return (Map<String, Object>) raw;
        }
        if (raw instanceof String jsonStr) {
            try {
                return objectMapper.readValue(jsonStr, new TypeReference<>() {});
            } catch (Exception e) {
                log.warn("Failed to parse payload string as JSON: {}", e.getMessage());
                return null;
            }
        }
        log.warn("Unexpected payload type: {}", raw.getClass().getName());
        return null;
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
