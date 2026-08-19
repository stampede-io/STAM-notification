package com.stampedeio.notification.repository;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.stampedeio.notification.domain.NotificationLog;

public interface NotificationLogRepository extends JpaRepository<NotificationLog, UUID> {
}
