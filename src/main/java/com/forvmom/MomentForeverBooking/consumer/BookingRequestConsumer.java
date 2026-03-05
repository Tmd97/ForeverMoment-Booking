package com.forvmom.MomentForeverBooking.consumer;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.service.BookingService;
import com.forvmom.MomentForeverBooking.service.OutboxService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code booking-requested} events published by the Platform (Core)
 * service.
 *
 * <p>
 * <b>Ack-first, short-TX design</b>:
 * <ol>
 * <li>Create/find incoming outbox record (idempotency guard)</li>
 * <li>Skip immediately if already PROCESSED (duplicate delivery)</li>
 * <li>ACK the Kafka message right away — no blocking</li>
 * <li>Call {@link BookingService#processBookingRequest} — the
 * {@code @Transactional}
 * method saves the booking and creates a PENDING {@link OutgoingOutboxRecord},
 * then the TX commits and the DB connection is released immediately</li>
 * <li>Immediately try to publish to Kafka (outside any TX) via
 * {@link OutgoingOutboxPublisher#trySinglePublish} — if Kafka is down the
 * record stays PENDING and Quartz retries it without opening a transaction</li>
 * </ol>
 */
@Component
public class BookingRequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookingRequestConsumer.class);

    private final BookingService bookingService;
    private final OutboxService outboxService;
    private final OutgoingOutboxPublisher outgoingOutboxPublisher;

    public BookingRequestConsumer(BookingService bookingService,
            OutboxService outboxService,
            OutgoingOutboxPublisher outgoingOutboxPublisher) {
        this.bookingService = bookingService;
        this.outboxService = outboxService;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
    }

    @KafkaListener(topics = "${kafka.topics.booking-requested}", groupId = "booking-group", containerFactory = "kafkaListenerContainerFactory")
    public void onBookingRequested(@Payload BookingRequestEvent event, Acknowledgment ack) {
        String bookingId = event.getBookingId();
        log.info("Received booking-requested: bookingId={}", bookingId);

        // Step 1: Idempotency guard — find or create incoming outbox record
        BookingOutbox outbox = outboxService.findOrCreateForEvent(event);
        OutgoingOutboxRecord outgoingOutboxRecord = null;
        // Step 2: Already fully processed — nothing to do
        if (BookingOutbox.STATUS_PROCESSED.equals(outbox.getStatus())) {
            log.info("Duplicate booking-requested ignored (already PROCESSED): bookingId={}", bookingId);
            ack.acknowledge();
            return;
        }
        // Step 3: ACK immediately — Quartz handles any downstream failure
        ack.acknowledge();
        try {
            outboxService.markAsProcessing(outbox);
            // Step 4: DB-only TX: saves booking + creates PENDING OutgoingOutboxRecord
            // TX commits here — DB connection released before Kafka is touched
            outgoingOutboxRecord = bookingService.processBookingRequest(event);
            outboxService.markAsProcessed(outbox);
        } catch (Exception e) {
            log.error("Failed to process booking-requested bookingId={}", bookingId, e);
            outboxService.markAsFailed(outbox);
            return; // stop here – do not attempt publish
        }
        // Step 5: Publish to Kafka outside TX — no DB connection held
        // If this fails, record stays PENDING; Quartz retries without opening a TX
        outgoingOutboxPublisher.trySinglePublish(outgoingOutboxRecord);
        log.info("Booking-requested fully processed: bookingId={}", bookingId);
    }
}