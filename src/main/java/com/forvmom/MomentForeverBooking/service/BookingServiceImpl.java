package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.exception.BookingNotFoundException;
import com.forvmom.MomentForeverBooking.exception.BookingStatusConflictException;
import com.forvmom.MomentForeverBooking.mapper.BookingMapper;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Core booking business logic.
 *
 * <p>
 * <b>Transaction boundary design</b>:
 * Each public method is {@code @Transactional} for <em>DB work only</em> —
 * it saves state and enqueues an {@link OutgoingOutboxRecord} (PENDING), then
 * returns. The DB transaction commits and releases the connection immediately.
 *
 * <p>
 * After the method returns, the <em>caller</em> (consumer) triggers a
 * non-transactional Kafka publish via
 * {@link OutgoingOutboxPublisher#trySinglePublish(OutgoingOutboxRecord)}.
 * If the publish fails the record stays PENDING and Quartz retries it — opening
 * zero DB transactions for the retry.
 *
 * <p>
 * <b>Why publish outside the TX?</b>
 * Holding a DB connection while waiting for Kafka unnecessarily lengthens
 * transaction time. Separating the phases means:
 * <ul>
 * <li>DB connection returned to the pool as early as possible</li>
 * <li>Kafka publish is retried by Quartz without opening any transaction</li>
 * </ul>
 */
@Service
public class BookingServiceImpl implements BookingService {

    public static final String EVT_PAYMENT_FAILED = "PAYMENT_FAILED";
    public static final String EVT_PAYMENT_PROCESSED = "PAYMENT_PROCESSED";
    public static final String EVT_BOOKING_REQUESTED = "BOOKING_REQUESTED";

    private static final Logger log = LoggerFactory.getLogger(BookingServiceImpl.class);

    // Event-type constants shared with OutgoingOutboxPublisher for routing
    public static final String EVT_PAYMENT_REQUESTED = "PAYMENT_REQUESTED";
    public static final String EVT_BOOKING_CONFIRMED = "BOOKING_CONFIRMED";
    public static final String EVT_BOOKING_FAILED = "BOOKING_FAILED";

    private final BookingRepository bookingRepository;
    private final BookingMapper bookingMapper;
    private final OutgoingOutboxService outgoingOutboxService;

    public BookingServiceImpl(BookingRepository bookingRepository,
                              BookingMapper bookingMapper,
                              OutgoingOutboxService outgoingOutboxService) {
        this.bookingRepository = bookingRepository;
        this.bookingMapper = bookingMapper;
        this.outgoingOutboxService = outgoingOutboxService;
    }

    // ── Incoming: booking-requested ───────────────────────────────────────────

    /**
     * Persists the booking (PENDING) and creates a PENDING outgoing outbox record
     * for payment-requested. Returns the record so the caller can publish to Kafka
     * outside this transaction.
     */
    @Override
    @Transactional
    public OutgoingOutboxRecord processBookingRequest(BookingRequestEvent event) {
        if (bookingRepository.existsByBookingId(event.getBookingId())) {
            log.warn("Duplicate booking request ignored. bookingId={}", event.getBookingId());
            return null; // idempotent — nothing to publish
        }

        Booking booking = bookingMapper.toEntity(event);
        bookingRepository.save(booking);
        log.info("Persisted PENDING booking: bookingId={}", booking.getBookingId());

        PaymentRequestedEvent paymentEvent = buildPaymentRequestedEvent(booking);
        OutgoingOutboxRecord record = outgoingOutboxService.createRecord(
                booking.getBookingId(), EVT_PAYMENT_REQUESTED, paymentEvent);
        log.debug("Created outgoing outbox record id={} for PAYMENT_REQUESTED bookingId={}",
                record.getId(), booking.getBookingId());
        return record;
        // TX commits here — DB connection released. Caller publishes to Kafka next.
    }

    // ── Payment result: confirm ───────────────────────────────────────────────

    /**
     * Marks booking CONFIRMED and enqueues booking-confirmed for publish.
     * Returns the outbox record; caller publishes outside this TX.
     */
    @Override
    @Transactional
    public OutgoingOutboxRecord confirmBooking(String bookingId) {

        Booking booking = getBookingByBookingId(bookingId);
        if (booking.getStatus() == BookingStatus.CONFIRMED)
            return null; // idempotent

        if (booking.getStatus() == BookingStatus.CANCELLED ||
                booking.getStatus() == BookingStatus.FAILED) {
            throw new BookingStatusConflictException(bookingId, booking.getStatus().name(), "confirm");
        }

        OutgoingOutboxRecord record = null;
        try {
            booking.setStatus(BookingStatus.CONFIRMED);
            booking.setConfirmedAt(LocalDateTime.now());
            bookingRepository.save(booking);

            BookingConfirmedEvent event = new BookingConfirmedEvent();
            event.setBookingId(booking.getBookingId());
            event.setUserId(booking.getUserId());
            event.setUserEmail(booking.getUserEmail());
            event.setExperienceId(booking.getExperienceId());
            event.setTimeSlotMapperId(booking.getTimeSlotMapperId());
            event.setGuestCount(booking.getGuestCount());
            event.setConfirmedAt(booking.getConfirmedAt());

            record = outgoingOutboxService.createRecord(
                    bookingId, EVT_BOOKING_CONFIRMED, event);
            log.info("Booking confirmed (outbox created): {}", bookingId);
        } catch (Exception e) {
            log.warn("Failed to process, so marked as FAILED for bookingId={}: {}", bookingId, e.getMessage());
            outgoingOutboxService.markAsFailed(record);
        }
        return record;
    }

    // ── Payment result: fail ─────────────────────────────────────────────────

    /**
     * Marks booking FAILED and enqueues booking-failed for publish.
     * Returns the outbox record; caller publishes outside this TX.
     */
    @Override
    @Transactional
    public OutgoingOutboxRecord failBooking(String bookingId, String reason) {
        OutgoingOutboxRecord record = null;
        try {
            Booking booking = getBookingByBookingId(bookingId);
            if (booking.getStatus() == BookingStatus.FAILED)
                return null; // idempotent
            booking.setStatus(BookingStatus.FAILED);
            booking.setFailureReason(reason);
            bookingRepository.save(booking);
            BookingFailedEvent bookingFailedEvent = buildBookingFailedEvent(booking, reason);

            record = outgoingOutboxService.createRecord(
                    bookingId, EVT_BOOKING_FAILED, bookingFailedEvent);
            log.warn("Booking failed (outbox created): {} (reason: {})", bookingId, reason);
            return record;
        } catch (Exception e) {
            log.warn("Failed to process, so marked as FAILED for bookingId={}: {}", bookingId, e.getMessage());
            outgoingOutboxService.markAsFailed(record);
        }
        return null;
    }

    // ── Cancel ───────────────────────────────────────────────────────────────

    @Override
    @Transactional
    public void cancelBooking(String bookingId) {
        Booking booking = getBookingByBookingId(bookingId);
        if (booking.getStatus() == BookingStatus.CANCELLED)
            return;
        if (booking.getStatus() == BookingStatus.CONFIRMED) {
            throw new BookingStatusConflictException(bookingId, booking.getStatus().name(), "cancel");
        }
        try {
            booking.setStatus(BookingStatus.CANCELLED);
            bookingRepository.save(booking);
            log.info("Booking cancelled: {}", bookingId);
        } catch (Exception e) {
            log.warn("Failed to cancel bookingId={}: {}", bookingId, e.getMessage());
        }
    }

    // ── Queries ───────────────────────────────────────────────────────────────

    @Override
    public Booking getBookingByBookingId(String bookingId) {
        return bookingRepository.findByBookingId(bookingId)
                .orElseThrow(() -> new BookingNotFoundException("Booking not found: " + bookingId));
    }

    @Override
    public Page<Booking> getBookingsByUser(Long userId, Pageable pageable) {
        return bookingRepository.findAllByUserId(userId, pageable);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private PaymentRequestedEvent buildPaymentRequestedEvent(Booking booking) {
        PaymentRequestedEvent e = new PaymentRequestedEvent();
        e.setBookingId(booking.getBookingId());
        e.setRequestedAt(LocalDateTime.now());
        e.setGrandTotal(booking.getGrandTotal());
        e.setUserId(booking.getUserId());
        e.setUserEmail(booking.getUserEmail());
        e.setExperienceId(booking.getExperienceId());
        e.setExperienceName(booking.getExperienceName());
        e.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        e.setGuestCount(booking.getGuestCount());
        e.setCurrency("INR");
        return e;
    }

    private BookingFailedEvent buildBookingFailedEvent(Booking booking, String reason) {

        BookingFailedEvent bookingFailedEvent = new BookingFailedEvent();
        bookingFailedEvent.setBookingId(booking.getBookingId());
        bookingFailedEvent.setFailedAt(LocalDateTime.now());
        bookingFailedEvent.setFailureReason(reason);
        bookingFailedEvent.setUserId(booking.getUserId());
        bookingFailedEvent.setUserEmail(booking.getUserEmail());
        bookingFailedEvent.setExperienceId(booking.getExperienceId());
        bookingFailedEvent.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        bookingFailedEvent.setGuestCount(booking.getGuestCount());
        return bookingFailedEvent;
    }
}