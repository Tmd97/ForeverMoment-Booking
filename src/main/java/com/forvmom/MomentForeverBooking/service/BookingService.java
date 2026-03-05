package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * Booking service interface.
 *
 * <p>
 * All mutating methods are {@code @Transactional} (DB-only) and return the
 * {@link OutgoingOutboxRecord} that was created for the outgoing Kafka event.
 * The caller publishes to Kafka <em>after</em> the transaction commits so the
 * DB connection is released before any network I/O happens.
 *
 * <p>
 * Methods return {@code null} when they are idempotent no-ops (e.g. booking
 * is already in the target status). The caller
 * ({@link OutgoingOutboxPublisher})
 * handles {@code null} gracefully.
 */
public interface BookingService {

    /** Saves booking (PENDING) + creates PAYMENT_REQUESTED outbox record. */
    OutgoingOutboxRecord processBookingRequest(BookingRequestEvent event);

    /** Marks booking CONFIRMED + creates BOOKING_CONFIRMED outbox record. */
    OutgoingOutboxRecord confirmBooking(String bookingId);

    /** Marks booking FAILED + creates BOOKING_FAILED outbox record. */
    OutgoingOutboxRecord failBooking(String bookingId, String reason);

    /** Marks booking CANCELLED. No outgoing event produced. */
    void cancelBooking(String bookingId);

    Booking getBookingByBookingId(String bookingId);

    Page<Booking> getBookingsByUser(Long userId, Pageable pageable);
}
