package com.forvmom.MomentForeverBooking.consumer;

import com.forvmom.MomentForeverBooking.events.PaymentFailedEvent;
import com.forvmom.MomentForeverBooking.service.inbound.InboundEventProcessorService;
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
    private final InboundEventProcessorService processingService;

    public PaymentFailedConsumer(InboundEventProcessorService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(topics = "${kafka.topics.payment-failed}", groupId = "booking-group", containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentFailed(@Payload PaymentFailedEvent event, Acknowledgment ack) {
        processingService.processEvent(event, ack);
    }
}
