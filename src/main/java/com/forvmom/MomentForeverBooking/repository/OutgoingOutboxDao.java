package com.forvmom.MomentForeverBooking.repository;

import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface OutgoingOutboxDao extends JpaRepository<OutgoingOutboxRecord, Long> {

        /**
         * Finds records with status in the given list (PENDING, FAILED etc.)
         * that were last updated before the cutoff time.
         * Used by OutgoingOutboxPublisher for Quartz retry.
         */
        List<OutgoingOutboxRecord> findByStatusInAndUpdatedAtBefore(
                        List<String> statuses,
                        LocalDateTime cutoff);

        /**
         * Finds any existing outgoing record for a specific (bookingId, eventType)
         * pair,
         * regardless of status. Used by OutboxRetryService to detect whether a record
         * already exists (PENDING / FAILED / SENT / DEAD) before creating a new one —
         * prevents duplicate records when retrying a partially-failed transaction.
         */
        Optional<OutgoingOutboxRecord> findByBookingIdAndEventType(String bookingId, String eventType);

        /**
         * Deletes SENT records older than the cutoff. Called by
         * OutgoingOutboxCleanupService.
         */
        @Modifying
        @Transactional
        @Query("DELETE FROM OutgoingOutboxRecord o WHERE o.status = :status AND o.updatedAt < :cutoff")
        int deleteByStatusAndUpdatedAtBefore(
                        @Param("status") String status,
                        @Param("cutoff") LocalDateTime cutoff);
}