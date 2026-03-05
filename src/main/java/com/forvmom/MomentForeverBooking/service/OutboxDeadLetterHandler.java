package com.forvmom.MomentForeverBooking.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class OutboxDeadLetterHandler {

    private static final Logger log = LoggerFactory.getLogger(OutboxDeadLetterHandler.class);

    private final OutboxService outboxService;
    private final BookingEventProducer eventProducer;
    private final AlertService alertService;
    private ObjectMapper objectMapper;
    private OutgoingOutboxService outgoingOutboxService;

    public OutboxDeadLetterHandler(OutboxService outboxService,
                                   BookingEventProducer eventProducer,
                                   AlertService alertService,
                                   ObjectMapper objectMapper,
                                   OutgoingOutboxService outgoingOutboxService) {

        this.outboxService = outboxService;
        this.eventProducer = eventProducer;
        this.alertService = alertService;
        this.objectMapper = objectMapper;
        this.outgoingOutboxService = outgoingOutboxService;
    }

    public void handleDeadRecord(BookingOutbox outbox) {
        outboxService.markAsDead(outbox);
        String bookingId = outbox.getBookingReferenceId();
        String eventType = outbox.getEventType();

        log.error("Record moved to DEAD after max retries: id={}, bookingId={}, eventType={}",
                outbox.getId(), bookingId, eventType);

        // Send alert first (always do this)
        String alertMsg = String.format(
                "Incoming outbox record id=%d for booking=%s eventType=%s moved to DEAD after max retries",
                outbox.getId(), bookingId, eventType);
        alertService.sendAlert(alertMsg);
        // Attempt compensation based on event type

        try {
            sendCompensationEvent(outbox);
        } catch (Exception e) {
            log.error("Failed to send compensation for dead record id={}, bookingId={}, eventType={}",
                    outbox.getId(), bookingId, eventType, e);
            alertService.sendAlert("Compensation failed for dead record " + outbox.getId());
        }
    }

    private void sendCompensationEvent(BookingOutbox outbox) throws Exception {
        String eventType = outbox.getEventType();
        String payload = outbox.getPayload();
        String bookingId = outbox.getBookingReferenceId();

        switch (eventType) {
            case BookingServiceImpl.EVT_BOOKING_REQUESTED:
                // Original booking request failed completely - send booking-failed
                BookingRequestEvent requestEvent = objectMapper.readValue(payload, BookingRequestEvent.class);
                sendBookingFailedEvent(requestEvent);
                log.info("Sent BOOKING_FAILED compensation for dead BOOKING_REQUESTED: bookingId={}", bookingId);
                break;

            case BookingServiceImpl.EVT_PAYMENT_PROCESSED:
                // Payment succeeded but we couldn't confirm the booking
                // Option 1: Send a refund request to payment service
                // Option 2: Just alert (manual intervention required)
                log.warn("Dead PAYMENT_PROCESSED record for bookingId={} - manual intervention may be needed", bookingId);
                // You could create a refund request via outgoing outbox here
                createRefundRequest(bookingId);
                break;

            case BookingServiceImpl.EVT_PAYMENT_FAILED:

                //TODO: send email to user about payment failure and next steps
                // Payment failed and we couldn't record the failure
                // The booking may be stuck in PENDING state - alert is sufficient
                log.warn("Dead PAYMENT_FAILED record for bookingId={} - booking may be stuck in PENDING", bookingId);
                // Maybe mark booking as FAILED directly? Risky - alert is safer
                break;

            default:
                log.warn("No compensation handler for dead event type: {}", eventType);
        }
    }

    private BookingFailedEvent buildFailedEvent(BookingRequestEvent requestEvent) {
        BookingFailedEvent failedEvent = new BookingFailedEvent();
        failedEvent.setBookingId(requestEvent.getBookingId());
        failedEvent.setUserId(requestEvent.getUserId());
        failedEvent.setUserEmail(requestEvent.getUserEmail());
        failedEvent.setExperienceId(requestEvent.getExperienceId());
        failedEvent.setTimeSlotMapperId(requestEvent.getTimeSlotMapperId());
        failedEvent.setGuestCount(requestEvent.getGuestCount());
        failedEvent.setFailureReason("Outbox record moved to DEAD after max retries");
        failedEvent.setFailedAt(LocalDateTime.now());
        return failedEvent;
    }

    private void sendBookingFailedEvent(BookingRequestEvent requestEvent) {
        // Create the failed event
        BookingFailedEvent failedEvent = new BookingFailedEvent();
        failedEvent.setBookingId(requestEvent.getBookingId());
        failedEvent.setUserId(requestEvent.getUserId());
        failedEvent.setUserEmail(requestEvent.getUserEmail());
        failedEvent.setExperienceId(requestEvent.getExperienceId());
        failedEvent.setTimeSlotMapperId(requestEvent.getTimeSlotMapperId());
        failedEvent.setGuestCount(requestEvent.getGuestCount());
        failedEvent.setFailureReason("Booking processing failed after max retries");
        failedEvent.setFailedAt(LocalDateTime.now());

        // Use outgoing outbox for reliability (don't send directly)

        // Try to publish immediately, if it fails, Quartz will retry
        // You'd need to inject OutgoingOutboxPublisher or handle this separately
//        log.info("Created outgoing BOOKING_FAILED record id={} for dead booking", record.getId());
    }

    private void createRefundRequest(String bookingId) {
        // For PAYMENT_PROCESSED dead records, you might want to request a refund
        // This would be sent to payment service via outgoing outbox
//        RefundRequestEvent refundEvent = new RefundRequestEvent();
//        refundEvent.setBookingId(bookingId);
//        refundEvent.setRequestedAt(LocalDateTime.now());
//        refundEvent.setReason("Booking confirmation failed after max retries");
//
//        OutgoingOutboxRecord record = outgoingOutboxService.createRecord(
//                bookingId,
//                "REFUND_REQUESTED", // You'd need to define this constant
//                refundEvent
//        );
//
//        log.info("Created REFUND_REQUESTED record id={} for dead payment-processed", record.getId());
    }
}