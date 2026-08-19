package com.stampedeio.notification.repository;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.stampedeio.notification.domain.ProcessedEvent;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {
}
