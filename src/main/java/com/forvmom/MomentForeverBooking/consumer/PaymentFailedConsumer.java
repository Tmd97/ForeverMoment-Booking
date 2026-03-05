package com.forvmom.MomentForeverBooking.consumer;

import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.PaymentFailedEvent;
import com.forvmom.MomentForeverBooking.service.BookingService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code payment-failed} events from the Payment service.
 *
 * <p>
 * Same ack-first / split-TX design as {@link PaymentProcessedConsumer}:
 * DB write (fail booking) commits first, then Kafka publish happens outside TX.
 *
 * <p>
 * Idempotency: {@code failBooking()} returns {@code null} if already FAILED.
 */
@Component
public class PaymentFailedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentFailedConsumer.class);

    private final BookingService bookingService;
    private final OutgoingOutboxPublisher outgoingOutboxPublisher;

    public PaymentFailedConsumer(BookingService bookingService,
            OutgoingOutboxPublisher outgoingOutboxPublisher) {
        this.bookingService = bookingService;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
    }

    @KafkaListener(topics = "${kafka.topics.payment-failed}", groupId = "booking-group", containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentFailed(@Payload PaymentFailedEvent event, Acknowledgment ack) {
        String bookingId = event.getBookingId();
        log.info("Received payment-failed: bookingId={}, reason={}", bookingId, event.getFailureReason());

        // ACK immediately
        ack.acknowledge();

        try {
            String reason = event.getFailureReason() != null
                    ? event.getFailureReason()
                    : "Payment failed (code: " + event.getErrorCode() + ")";

            // DB-only TX: update booking + create PENDING outgoing record
            // TX commits → DB connection released before Kafka is touched
            OutgoingOutboxRecord record = bookingService.failBooking(bookingId, reason);

            // Publish outside TX — no DB connection held during Kafka I/O
            outgoingOutboxPublisher.trySinglePublish(record);

        } catch (Exception e) {
            log.error("Failed to process payment-failed for bookingId={}: {}", bookingId, e.getMessage(), e);
        }
    }
}
