package com.stampedeio.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stampedeio.notification.repository.ProcessedEventRepository;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Tag("integration")
@SpringBootTest
@Testcontainers
class NotificationConsumerIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> mailhog = new GenericContainer<>(DockerImageName.parse("mailhog/mailhog"))
            .withExposedPorts(1025, 8025);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.mail.host", mailhog::getHost);
        registry.add("spring.mail.port", () -> mailhog.getMappedPort(1025));
        registry.add("eureka.client.enabled", () -> "false");
        registry.add("spring.cloud.config.enabled", () -> "false");
        registry.add("spring.config.import", () -> "");
    }

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    private KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final HttpClient httpClient = HttpClient.newHttpClient();

    @BeforeEach
    void setUp() {
        processedEventRepository.deleteAll();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(producerProps,
                new StringSerializer(), new JsonSerializer<>(mapper));
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        clearMailhog();
    }

    @Test
    void ac1_reservationConfirmed_sendsConfirmationEmail() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        sendEvent(eventId, "ReservationConfirmed", correlationId, reservationId);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JsonNode messages = getMailhogMessages();
            assertThat(messages.get("total").asInt()).isEqualTo(1);
            String subject = messages.get("items").get(0)
                    .get("Content").get("Headers").get("Subject").get(0).asText();
            assertThat(subject).contains("Booking Confirmed");
            assertThat(subject).contains(reservationId.toString());
        });
    }

    @Test
    void ac2_seatsReleased_sendsCancellationEmail() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        sendEvent(eventId, "SeatsReleased", correlationId, reservationId);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JsonNode messages = getMailhogMessages();
            assertThat(messages.get("total").asInt()).isEqualTo(1);
            String subject = messages.get("items").get(0)
                    .get("Content").get("Headers").get("Subject").get(0).asText();
            assertThat(subject).contains("Booking Cancelled");
        });
    }

    @Test
    void ac2_holdExpired_sendsExpiryEmail() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        sendEvent(eventId, "HoldExpired", correlationId, reservationId);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JsonNode messages = getMailhogMessages();
            assertThat(messages.get("total").asInt()).isEqualTo(1);
            String subject = messages.get("items").get(0)
                    .get("Content").get("Headers").get("Subject").get(0).asText();
            assertThat(subject).contains("Hold Expired");
        });
    }

    @Test
    void ac3_duplicateEvent_doesNotSendDuplicateEmail() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        sendEvent(eventId, "ReservationConfirmed", correlationId, reservationId);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JsonNode messages = getMailhogMessages();
            assertThat(messages.get("total").asInt()).isEqualTo(1);
        });

        sendEvent(eventId, "ReservationConfirmed", correlationId, reservationId);

        // Wait a bit to allow any duplicate to arrive
        Thread.sleep(3000);

        JsonNode messages = getMailhogMessages();
        assertThat(messages.get("total").asInt())
                .as("Duplicate event must not produce a second email")
                .isEqualTo(1);

        assertThat(processedEventRepository.count()).isEqualTo(1);
    }

    private void sendEvent(UUID eventId, String eventType, UUID correlationId,
                           UUID reservationId) throws Exception {
        UUID showId = UUID.randomUUID();
        Map<String, Object> payload = Map.of(
                "reservationId", reservationId.toString(),
                "showId", showId.toString(),
                "seatIds", List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                "status", eventType.equals("ReservationConfirmed") ? "CONFIRMED" : "RELEASED",
                "correlationId", correlationId.toString());

        Map<String, Object> envelope = new HashMap<>();
        envelope.put("eventId", eventId.toString());
        envelope.put("eventType", eventType);
        envelope.put("version", 1);
        envelope.put("occurredAt", Instant.now().toString());
        envelope.put("correlationId", correlationId.toString());
        envelope.put("aggregateId", reservationId.toString());
        envelope.put("payload", payload);

        kafkaTemplate.send("reservations.events", reservationId.toString(), envelope)
                .get(10, TimeUnit.SECONDS);
    }

    private JsonNode getMailhogMessages() {
        try {
            String url = "http://" + mailhog.getHost() + ":" + mailhog.getMappedPort(8025)
                    + "/api/v2/messages";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());
            return objectMapper.readTree(response.body());
        } catch (Exception e) {
            throw new RuntimeException("Failed to query Mailhog API", e);
        }
    }

    private void clearMailhog() {
        try {
            String url = "http://" + mailhog.getHost() + ":" + mailhog.getMappedPort(8025)
                    + "/api/v1/messages";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .DELETE()
                    .build();
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Mailhog might not have DELETE endpoint; ignore
        }
    }
}
