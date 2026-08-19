package com.stampedeio.notification.service;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import com.stampedeio.notification.domain.NotificationLog;
import com.stampedeio.notification.repository.NotificationLogRepository;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine;
    private final NotificationLogRepository notificationLogRepository;
    private final String fromAddress;

    public NotificationService(JavaMailSender mailSender,
                               TemplateEngine templateEngine,
                               NotificationLogRepository notificationLogRepository,
                               @Value("${notification.from-address}") String fromAddress) {
        this.mailSender = mailSender;
        this.templateEngine = templateEngine;
        this.notificationLogRepository = notificationLogRepository;
        this.fromAddress = fromAddress;
    }

    public void sendNotification(UUID eventId, UUID correlationId,
                                 String reservationId, String showId,
                                 List<String> seatIds, String templateName,
                                 String subjectPrefix, String eventType) {
        String subject = subjectPrefix + " - " + reservationId;
        String recipient = reservationId + "@notifications.stampede.io";

        Context ctx = new Context();
        ctx.setVariable("reservationId", reservationId);
        ctx.setVariable("showId", showId);
        ctx.setVariable("seats", seatIds.isEmpty() ? "N/A" : String.join(", ", seatIds));

        String body = templateEngine.process(templateName, ctx);
        sendEmail(recipient, subject, body);

        notificationLogRepository.save(
                new NotificationLog(eventId, eventType, recipient, subject, correlationId));
        log.info("{} email sent for reservation={}", subjectPrefix, reservationId);
    }

    private void sendEmail(String to, String subject, String htmlBody) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");
            helper.setFrom(fromAddress);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(htmlBody, true);
            mailSender.send(message);
        } catch (MessagingException e) {
            log.error("Failed to send email to {}: {}", to, e.getMessage());
            throw new EmailSendException("Failed to send email to " + to, e);
        }
    }

    public static class EmailSendException extends RuntimeException {
        public EmailSendException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
