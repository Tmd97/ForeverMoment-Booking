package com.forvmom.MomentForeverBooking.consumer;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
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
 * Consumes {@code payment-processed} events from the Payment service.
 *
 * <p>
 * Ack-first. DB TX and Kafka publish are separated to avoid holding the
 * DB connection while waiting for Kafka:
 * <ol>
 * <li>ACK immediately</li>
 * <li>{@link BookingService#confirmBooking} — {@code @Transactional}: updates
 * booking
 * status and creates a PENDING {@link OutgoingOutboxRecord} for
 * {@code booking-confirmed}.
 * TX commits and DB connection released.</li>
 * <li>{@link OutgoingOutboxPublisher#trySinglePublish} — outside TX: publishes
 * to Kafka.
 * On failure record stays PENDING; Quartz retries without opening a
 * transaction.</li>
 * </ol>
 *
 * <p>
 * Idempotency: {@code confirmBooking()} returns {@code null} if already
 * CONFIRMED.
 */
@Component
public class PaymentProcessedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessedConsumer.class);

    private final BookingService bookingService;
    private final OutgoingOutboxPublisher outgoingOutboxPublisher;
    private final OutboxService outboxService;

    public PaymentProcessedConsumer(BookingService bookingService,
                                    OutgoingOutboxPublisher outgoingOutboxPublisher, OutboxService outboxService) {
        this.bookingService = bookingService;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
        this.outboxService = outboxService;
    }

    @KafkaListener(topics = "${kafka.topics.payment-confirmed}", groupId = "booking-group", containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentProcessed(@Payload PaymentProcessedEvent event, Acknowledgment ack) {
        String bookingId = event.getBookingId();
        log.info("Received payment-processed: bookingId={}, transactionId={}", bookingId, event.getTransactionId());

        // Step 1: Idempotency guard — find or create incoming outbox record
        BookingOutbox outbox = outboxService.findOrCreateForEvent(event);
        // ACK immediately
        ack.acknowledge();

        try {
            // DB-only TX: update booking + create PENDING outgoing record
            // TX commits here → DB connection released
            OutgoingOutboxRecord record = bookingService.confirmBooking(bookingId);

            // Publish outside TX — no DB connection held
            outgoingOutboxPublisher.trySinglePublish(record);

        } catch (Exception e) {
            log.error("Failed to confirm booking bookingId={}: {}", bookingId, e.getMessage(), e);
        }
    }
}
