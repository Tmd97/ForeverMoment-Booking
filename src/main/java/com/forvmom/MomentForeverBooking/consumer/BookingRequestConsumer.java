package com.forvmom.MomentForeverBooking.consumer;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.service.BookingService;
import com.forvmom.MomentForeverBooking.service.OutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class BookingRequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookingRequestConsumer.class);

    private final BookingService bookingService;
    private final OutboxService outboxService;

    public BookingRequestConsumer(BookingService bookingService,
                                  OutboxService outboxService) {
        this.bookingService = bookingService;
        this.outboxService = outboxService;
    }

    @KafkaListener(topics = "${kafka.topics.booking-requested}",
            groupId = "booking-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onBookingRequested(@Payload BookingRequestEvent event, Acknowledgment ack) {
        String bookingId = event.getBookingId();
        log.info("Received booking-requested: {}", bookingId);

        BookingOutbox outbox = outboxService.findOrCreateForEvent(event);

        if (BookingOutbox.STATUS_PROCESSED.equals(outbox.getStatus())) {
            log.info("Booking already processed, acknowledging. bookingId={}", bookingId);
            ack.acknowledge();
            return;
        }

        try {
            // Idempotency check: if status is PROCESSING, it means another instance is already working on it
            outboxService.markAsProcessing(outbox);
            //Store in outbox before processing to ensure we have a record of the event and can retry if needed
            bookingService.processBookingRequest(event);
            outboxService.markAsProcessed(outbox);
            ack.acknowledge();
            log.info("Successfully processed booking: {}", bookingId);
        } catch (Exception e) {
            log.error("Failed to process booking: {}", bookingId, e);
            outboxService.markAsFailed(outbox);
            // Do not ack – rely on scheduled retries
            throw e;
        }
    }
}