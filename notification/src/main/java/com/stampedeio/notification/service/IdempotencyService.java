package com.stampedeio.notification.service;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.notification.domain.ProcessedEvent;
import com.stampedeio.notification.repository.ProcessedEventRepository;

@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);

    private final ProcessedEventRepository processedEventRepository;

    public IdempotencyService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional(readOnly = true)
    public boolean isAlreadyProcessed(UUID eventId) {
        return processedEventRepository.existsById(eventId);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markProcessed(UUID eventId, String eventType) {
        try {
            processedEventRepository.saveAndFlush(new ProcessedEvent(eventId, eventType));
        } catch (DataIntegrityViolationException e) {
            log.debug("Event already marked processed (concurrent race): eventId={}", eventId);
        }
    }
}
