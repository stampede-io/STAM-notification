package com.stampedeio.notification.config;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * STAM-406 / AC5: see STAM-booking's identical class (the producer side)
 * for the full header contract. Notification has no producer of its own
 * (CLAUDE.md §13#7), so only the consumer half applies here. Populating
 * MDC from the header, rather than relying solely on BookingNotification-
 * Consumer's own payload-parsing MDC.put, means a malformed-message log
 * line (eventId missing, before the payload is ever read) still carries a
 * correlationId instead of none at all.
 */
public final class KafkaCorrelationIdInterceptor implements RecordInterceptor<String, Map<String, Object>> {

    public static final String HEADER = "correlationId";
    private static final String SERVICE = "notification";

    @Override
    public ConsumerRecord<String, Map<String, Object>> intercept(
            ConsumerRecord<String, Map<String, Object>> record, Consumer<String, Map<String, Object>> consumer) {
        Header header = record.headers().lastHeader(HEADER);
        if (header != null) {
            MDC.put(HEADER, new String(header.value(), StandardCharsets.UTF_8));
        }
        MDC.put("service", SERVICE);
        return record;
    }

    @Override
    public void success(ConsumerRecord<String, Map<String, Object>> record,
                         Consumer<String, Map<String, Object>> consumer) {
        clear();
    }

    @Override
    public void failure(ConsumerRecord<String, Map<String, Object>> record, Exception exception,
                         Consumer<String, Map<String, Object>> consumer) {
        clear();
    }

    private void clear() {
        MDC.remove(HEADER);
        MDC.remove("service");
    }
}
