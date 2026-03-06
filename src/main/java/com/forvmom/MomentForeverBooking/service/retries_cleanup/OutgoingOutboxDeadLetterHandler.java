package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import com.forvmom.MomentForeverBooking.service.alerts.AlertService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OutgoingOutboxDeadLetterHandler {

    private static final Logger log = LoggerFactory.getLogger(OutgoingOutboxDeadLetterHandler.class);

    private final OutgoingOutboxService outgoingOutboxService;
    private final AlertService alertService;
    private final ObjectMapper objectMapper;

    public OutgoingOutboxDeadLetterHandler(OutgoingOutboxService outgoingOutboxService,
                                           AlertService alertService,
                                           ObjectMapper objectMapper) {
        this.outgoingOutboxService = outgoingOutboxService;
        this.alertService = alertService;
        this.objectMapper = objectMapper;
    }

    public void handleDeadRecord(OutgoingOutboxRecord record) {
        outgoingOutboxService.markAsDead(record);
        String msg = String.format(
                "Outgoing outbox record id=%d type=%s bookingId=%s moved to DEAD after max retries",
                record.getId(), record.getEventType(), record.getBookingId());
        log.error("DEAD: {}", msg);
        alertService.sendAlert(msg);

        try {
            compensate(record);
        } catch (Exception e) {
            log.error("Compensation failed for outgoing dead record id={}", record.getId(), e);
            alertService.sendAlert(String.format(
                    "COMPENSATION FAILED for outgoing dead record id=%d - MANUAL INTERVENTION REQUIRED",
                    record.getId()
            ));
        }
    }

    private void compensate(OutgoingOutboxRecord record) throws Exception {
        String eventType = record.getEventType();
        String bookingId = record.getBookingId();
        String payload = record.getPayload();

        switch (eventType) {
            case EventConstants.PAYMENT_REQUESTED:
                compensatePaymentRequested(bookingId, payload);
                break;
            case EventConstants.BOOKING_CONFIRMED:
                compensateBookingConfirmed(bookingId, payload);
                break;
            case EventConstants.BOOKING_FAILED:
                compensateBookingFailed(bookingId, payload);
                break;
            default:
                log.warn("No compensation handler for outgoing dead event type: {}", eventType);
        }
    }

    private void compensatePaymentRequested(String bookingId, String payload) throws Exception {
        // Payment request permanently failed – the booking is stuck in PENDING with inventory held.
        log.warn("PAYMENT_REQUESTED dead letter for bookingId={} – payment service never notified", bookingId);
        PaymentRequestedEvent event = objectMapper.readValue(payload, PaymentRequestedEvent.class);

        // Option 1: Release inventory (if you have an inventory service)
        // inventoryService.releaseInventory(event.getExperienceId(), event.getTimeSlotMapperId(), event.getGuestCount());

        // Option 2: Mark booking as FAILED directly (risky but possible)
        // bookingService.forceFailBooking(bookingId, "Payment request permanently failed");

        // Option 3: Send an alert for manual intervention (what we currently do)
        alertService.sendAlert(String.format(
                "MANUAL ACTION NEEDED: Dead PAYMENT_REQUESTED for booking %s – check inventory state",
                bookingId
        ));
    }

    private void compensateBookingConfirmed(String bookingId, String payload) throws Exception {
        log.warn("BOOKING_CONFIRMED dead letter for bookingId={} – external services not notified", bookingId);
        BookingConfirmedEvent event = objectMapper.readValue(payload, BookingConfirmedEvent.class);

        // Option: Create a manual retry record (the record itself is dead, so we create a new one)
        // OutgoingOutboxRecord retryRecord = outgoingOutboxService.createRecord(bookingId, EventConstants.BOOKING_CONFIRMED, event);
        // log.info("Created manual retry record id={} for dead BOOKING_CONFIRMED", retryRecord.getId());

        alertService.sendAlert(String.format(
                "⚠️ BOOKING_CONFIRMED dead letter: booking %s is confirmed but external systems not notified. Manual intervention may be needed.",
                bookingId
        ));
    }

    private void compensateBookingFailed(String bookingId, String payload) throws Exception {
        log.warn("BOOKING_FAILED dead letter for bookingId={} – external services not notified", bookingId);
        BookingFailedEvent event = objectMapper.readValue(payload, BookingFailedEvent.class);

        // Less critical; just alert
        alertService.sendAlert(String.format(
                "BOOKING_FAILED dead letter: booking %s is failed. Manual check recommended.",
                bookingId
        ));
    }
}