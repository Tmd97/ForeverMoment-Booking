package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
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

    public OutboxDeadLetterHandler(OutboxService outboxService,
                                   BookingEventProducer eventProducer,
                                   AlertService alertService) {
        this.outboxService = outboxService;
        this.eventProducer = eventProducer;
        this.alertService = alertService;
    }

    public void handleDeadRecord(BookingOutbox outbox) {
        outboxService.markAsDead(outbox);

        try {
            BookingRequestEvent requestEvent = JsonUtils.fromJson(outbox.getPayload(), BookingRequestEvent.class);
            BookingFailedEvent failedEvent = buildFailedEvent(requestEvent);
            eventProducer.sendBookingFailedEvent(failedEvent);
            log.info("BookingFailedEvent sent for dead booking: {}", failedEvent.getBookingId());
        } catch (Exception e) {
            log.error("Failed to send BookingFailedEvent for dead outbox id={}, bookingReferenceId={}",
                    outbox.getId(), outbox.getBookingReferenceId(), e);
            alertService.sendAlert("Failed to send failure event for dead record " + outbox.getId());
        }

        alertService.sendAlert("Outbox record " + outbox.getId() + " for booking " +
                outbox.getBookingReferenceId() + " moved to DEAD after max retries");
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
}