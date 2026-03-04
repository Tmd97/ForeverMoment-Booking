package com.forvmom.MomentForeverBooking.repository;

import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

public interface OutgoingOutboxDao extends JpaRepository<OutgoingOutboxRecord, Long> {

    /**
     * Finds records with status in the given list (e.g., PENDING, FAILED)
     * that were last updated before the cutoff time.
     * Used by the publisher to pick events that need to be (re)sent.
     */
    List<OutgoingOutboxRecord> findByStatusInAndUpdatedAtBefore(
            List<String> statuses,
            LocalDateTime cutoff
    );

    /**
     * Deletes records that have been successfully sent (status = 'SENT')
     * and are older than the cutoff. Used for cleanup.
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM OutgoingOutboxRecord o WHERE o.status = :status AND o.updatedAt < :cutoff")
    int deleteByStatusAndUpdatedAtBefore(
            @Param("status") String status,
            @Param("cutoff") LocalDateTime cutoff
    );
}