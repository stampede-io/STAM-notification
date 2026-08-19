package com.stampedeio.notification.service;

import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.stampedeio.notification.domain.ProcessedEvent;
import com.stampedeio.notification.repository.ProcessedEventRepository;

@Service
public class IdempotencyService {

    private final ProcessedEventRepository processedEventRepository;

    public IdempotencyService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryMarkProcessed(UUID eventId, String eventType) {
        try {
            processedEventRepository.saveAndFlush(new ProcessedEvent(eventId, eventType));
            return true;
        } catch (DataIntegrityViolationException e) {
            return false;
        }
    }
}
