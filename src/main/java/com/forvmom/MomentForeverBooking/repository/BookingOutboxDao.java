package com.forvmom.MomentForeverBooking.repository;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface BookingOutboxDao extends JpaRepository<BookingOutbox, Long> {
    Optional<BookingOutbox> findByBookingReferenceId(String bookingReferenceId);

    @Modifying
    @Query("DELETE FROM BookingOutbox b WHERE b.status = :status AND b.updatedAt < :cutoff")
    int deleteByStatusAndUpdatedAtBefore(@Param("status") String status,
                                         @Param("cutoff") LocalDateTime cutoff);

    List<BookingOutbox> findByStatusInAndUpdatedAtBefore(List<String> statuses,
                                                         LocalDateTime cutoff);

    Optional<BookingOutbox> findByBookingReferenceIdAndEventType(String bookingReferenceId, String eventType);
}