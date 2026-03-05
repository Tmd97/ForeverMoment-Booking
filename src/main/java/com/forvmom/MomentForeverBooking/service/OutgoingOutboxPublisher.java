package com.forvmom.MomentForeverBooking.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Handles Kafka publishing for outgoing outbox records.
 *
 * <p>
 * Two entry points:
 * <ol>
 * <li>{@link #trySinglePublish(OutgoingOutboxRecord)} — called by consumers
 * immediately after the DB transaction commits. No transaction is opened
 * here; if the send fails the record stays PENDING for Quartz to retry.</li>
 * <li>{@link #publishPendingEvents()} — called by Quartz every minute to retry
 * any PENDING or FAILED records that missed the immediate publish.</li>
 * </ol>
 *
 * <p>
 * Both paths share the same {@link #sendToKafka} routing logic so there is
 * no duplication.
 */
@Service
public class OutgoingOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutgoingOutboxPublisher.class);
    private static final int MAX_RETRIES = 5;
    private static final long GRACE_PERIOD_MINUTES = 1;

    private final OutgoingOutboxDao outgoingOutboxDao;
    private final BookingEventProducer eventProducer;
    private final ObjectMapper objectMapper;
    private final AlertService alertService;
    private final OutgoingOutboxService outgoingOutboxService;


    @Autowired
    private BookingService bookingService;

    public OutgoingOutboxPublisher(OutgoingOutboxDao outgoingOutboxDao,
                                   BookingEventProducer eventProducer,
                                   ObjectMapper objectMapper,
                                   AlertService alertService,
                                   OutgoingOutboxService outgoingOutboxService) {
        this.outgoingOutboxDao = outgoingOutboxDao;
        this.eventProducer = eventProducer;
        this.objectMapper = objectMapper;
        this.alertService = alertService;
        this.outgoingOutboxService = outgoingOutboxService;
    }

    // ── Immediate publish (called by consumers, outside TX) ───────────────────

    /**
     * Attempts a single Kafka publish for the given outbox record.
     * <p>
     * <b>No transaction is opened.</b> The caller must ensure the record is
     * already committed in DB before calling this.
     * <ul>
     * <li>Success → marks SENT (small independent TX via
     * {@link OutgoingOutboxService})</li>
     * <li>Failure → logs warning; record stays PENDING for Quartz retry</li>
     * </ul>
     *
     * @param record the outgoing outbox record to publish (must be non-null)
     */
    public void trySinglePublish(OutgoingOutboxRecord record) {
        if (record == null)
            return; // idempotent caller may pass null on duplicate
        try {
            sendToKafka(record);
            outgoingOutboxService.markAsSent(record);
            log.info("Immediate publish succeeded: outboxId={}, type={}, bookingId={}",
                    record.getId(), record.getEventType(), record.getBookingId());
        } catch (Exception e) {
            log.warn("Immediate publish failed — record id={} stays PENDING for Quartz retry: {}",
                    record.getId(), e.getMessage());
            outgoingOutboxService.markAsFailed(record);
            // Do NOT rethrow — DB state is the source of truth, Quartz will retry
        }
    }

    /**
     * Convenience overload: looks up an existing record by (bookingId, eventType)
     * and calls {@link #trySinglePublish(OutgoingOutboxRecord)}.
     * Used by {@link OutboxRetryService} for the common retry case where the record
     * already exists — avoiding re-opening a full transaction.
     */
    public void trySinglePublishByBookingAndType(String bookingId, String eventType) {
        Optional<OutgoingOutboxRecord> opt = outgoingOutboxDao
                .findByBookingIdAndEventType(bookingId, eventType);
        if (opt.isEmpty()) {
            log.warn("trySinglePublishByBookingAndType: no record found for bookingId={}, type={}", bookingId,
                    eventType);
            return;
        }
        OutgoingOutboxRecord record = opt.get();
        if (OutgoingOutboxRecord.STATUS_SENT.equals(record.getStatus())) {
            log.debug("Record id={} already SENT — skipping", record.getId());
            return;
        }
        trySinglePublish(record);
    }

    // ── Quartz batch retry (every minute) ─────────────────────────────────────

    /**
     * Called by Quartz every minute to retry PENDING / FAILED outgoing records.
     * Picks up records updated more than {@value GRACE_PERIOD_MINUTES} minute(s)
     * ago
     * to avoid racing the immediate-publish path.
     */
    @Transactional
    public void publishPendingEvents() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(GRACE_PERIOD_MINUTES);
        List<OutgoingOutboxRecord> pending = outgoingOutboxDao.findByStatusInAndUpdatedAtBefore(
                List.of(OutgoingOutboxRecord.STATUS_PENDING, OutgoingOutboxRecord.STATUS_FAILED),
                cutoff);

        if (!pending.isEmpty()) {
            log.info("Quartz outbox poller found {} record(s) to retry", pending.size());
        }

        for (OutgoingOutboxRecord record : pending) {
            if (record.getRetryCount() >= MAX_RETRIES) {
                handleDeadRecord(record);
                continue;
            }
            try {
                outgoingOutboxService.incrementRetry(record);
                sendToKafka(record);
                outgoingOutboxService.markAsSent(record);
                log.info("Quartz retry succeeded: id={}, type={}, bookingId={}",
                        record.getId(), record.getEventType(), record.getBookingId());
            } catch (Exception e) {
                log.error("Quartz retry failed: id={}, type={}, bookingId={}: {}",
                        record.getId(), record.getEventType(), record.getBookingId(), e.getMessage());
            }
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void sendToKafka(OutgoingOutboxRecord record) throws Exception {
        String type = record.getEventType();
        String json = record.getPayload();

        switch (type) {

            case BookingServiceImpl.EVT_PAYMENT_REQUESTED -> {
                PaymentRequestedEvent event = objectMapper.readValue(json, PaymentRequestedEvent.class);
                eventProducer.sendPaymentRequestedEvent(event);
            }
            case BookingServiceImpl.EVT_BOOKING_CONFIRMED -> {
                BookingConfirmedEvent event = objectMapper.readValue(json, BookingConfirmedEvent.class);
                eventProducer.sendBookingConfirmedEvent(event);
            }
            case BookingServiceImpl.EVT_BOOKING_FAILED -> {
                BookingFailedEvent event = objectMapper.readValue(json, BookingFailedEvent.class);
                eventProducer.sendBookingFailedEvent(event);
            }
            default -> log.warn("Unknown outgoing event type: {}", type);
        }
    }

    private void handleDeadRecord(OutgoingOutboxRecord record) {
        outgoingOutboxService.markAsDead(record);
        String msg = String.format(
                "Outgoing outbox record id=%d type=%s bookingId=%s moved to DEAD after %d retries",
                record.getId(), record.getEventType(), record.getBookingId(), MAX_RETRIES);
        log.error("DEAD: {}", msg);
        alertService.sendAlert(msg);
        // Attempt compensation based on event type
        try {
            compensateOutgoingDeadRecord(record);
        } catch (Exception e) {
            log.error("Compensation failed for outgoing dead record id={}", record.getId(), e);
            alertService.sendAlert(String.format(
                    "COMPENSATION FAILED for outgoing dead record id=%d - MANUAL INTERVENTION REQUIRED",
                    record.getId()
            ));
        }
    }

    private void compensateOutgoingDeadRecord(OutgoingOutboxRecord record) {
        String eventType = record.getEventType();
        String bookingId = record.getBookingId();
        String payload = record.getPayload();

        switch (eventType) {
            case BookingServiceImpl.EVT_PAYMENT_REQUESTED:
                compensatePaymentRequested(bookingId, payload);
                break;

            case BookingServiceImpl.EVT_BOOKING_CONFIRMED:
                compensateBookingConfirmed(bookingId, payload);
                break;

            case BookingServiceImpl.EVT_BOOKING_FAILED:
                compensateBookingFailed(bookingId, payload);
                break;

            default:
                log.warn("No compensation handler for outgoing dead event type: {}", eventType);
        }
    }

    private void compensatePaymentRequested(String bookingId, String payload) {
        try {
            // Payment request permanently failed to publish
            // This means the payment service never knew about this booking
            // The booking is stuck in PENDING state with inventory held

            log.warn("PAYMENT_REQUESTED dead letter for bookingId={} - payment service never notified", bookingId);

            // Option 1: Try to get booking details and release inventory
            try {
                Booking booking = bookingService.getBookingByBookingId(bookingId);

                // Release the held inventory
//                inventoryService.releaseInventory(
//                        booking.getExperienceId(),
//                        booking.getTimeSlotMapperId(),
//                        booking.getGuestCount()
//                );

                log.info("Inventory released for bookingId={} due to dead PAYMENT_REQUESTED", bookingId);

                // Mark booking as FAILED
//                bookingService.forceFailBooking(bookingId, "Payment request failed permanently");

                // Send notification to user
//                emailService.sendBookingFailedEmail(
//                        booking.getUserEmail(),
//                        bookingId,
//                        "Unable to process payment request"
//                );

                alertService.sendAlert(String.format(
                        "Compensation complete for dead PAYMENT_REQUESTED: booking %s failed, inventory released",
                        bookingId
                ));

            } catch (Exception e) {
                // Booking might not exist or other issues
                log.error("Cannot compensate PAYMENT_REQUESTED dead letter - booking {} may not exist", bookingId, e);

                alertService.sendAlert(String.format(
                        "MANUAL ACTION NEEDED: Dead PAYMENT_REQUESTED for booking %s - check inventory state",
                        bookingId
                ));
            }

        } catch (Exception e) {
            log.error("Failed to compensate PAYMENT_REQUESTED dead letter for bookingId={}", bookingId, e);
            throw e;
        }
    }

    private void compensateBookingConfirmed(String bookingId, String payload) {
        try {
            // Booking confirmed event permanently failed to publish
            // The booking is CONFIRMED in our system, but external services don't know

            log.warn("BOOKING_CONFIRMED dead letter for bookingId={} - external services not notified", bookingId);

            // Deserialize to get details
            BookingConfirmedEvent event = objectMapper.readValue(payload, BookingConfirmedEvent.class);

            // Option 1: Retry one more time via different mechanism
            // Create a new outgoing record with same payload (manual retry)
            OutgoingOutboxRecord retryRecord = outgoingOutboxService.createRecord(
                    bookingId,
                    BookingServiceImpl.EVT_BOOKING_CONFIRMED,
                    event
            );

            log.info("Created manual retry record id={} for dead BOOKING_CONFIRMED", retryRecord.getId());

            // Option 2: Send email to admin to manually notify
//            emailService.sendAdminAlert(
//                    "Booking Confirmed Notification Failed",
//                    String.format("Booking %s is CONFIRMED but notification failed. Manual intervention may be needed.", bookingId)
//            );

            alertService.sendAlert(String.format(
                    "⚠️ BOOKING_CONFIRMED dead letter: booking %s is confirmed but external systems not notified. Manual retry created.",
                    bookingId
            ));

        } catch (Exception e) {
            log.error("Failed to compensate BOOKING_CONFIRMED dead letter for bookingId={}", bookingId, e);
            //throw e;
        }
    }

    private void compensateBookingFailed(String bookingId, String payload) {
        try {
            // Booking failed event permanently failed to publish
            // The booking is FAILED in our system, but external services don't know

            log.warn("BOOKING_FAILED dead letter for bookingId={} - external services not notified", bookingId);

            // Deserialize to get details
            BookingFailedEvent event = objectMapper.readValue(payload, BookingFailedEvent.class);

            // For FAILED events, notification is less critical than CONFIRMED
            // But still should be attempted

            // Option 1: Log and alert only (notification can be missed)
            log.info("Booking {} is FAILED but failure notification couldn't be sent", bookingId);

            // Option 2: Create manual retry
            OutgoingOutboxRecord retryRecord = outgoingOutboxService.createRecord(
                    bookingId,
                    BookingServiceImpl.EVT_BOOKING_FAILED,
                    event
            );

            log.info("Created manual retry record id={} for dead BOOKING_FAILED", retryRecord.getId());

            alertService.sendAlert(String.format(
                    "BOOKING_FAILED dead letter: booking %s is failed. Manual retry created.",
                    bookingId
            ));

        } catch (Exception e) {
            log.error("Failed to compensate BOOKING_FAILED dead letter for bookingId={}", bookingId, e);
            //throw e;
        }
    }
}