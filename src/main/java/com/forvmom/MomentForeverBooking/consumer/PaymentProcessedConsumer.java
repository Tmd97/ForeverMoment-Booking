package com.forvmom.MomentForeverBooking.consumer;

import com.forvmom.MomentForeverBooking.events.PaymentProcessedEvent;
import com.forvmom.MomentForeverBooking.service.inbound.InboundEventProcessorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PaymentProcessedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessedConsumer.class);

    private final InboundEventProcessorService processingService;

    public PaymentProcessedConsumer(InboundEventProcessorService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(topics = "${kafka.topics.payment-confirmed}", groupId = "booking-group", containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentProcessed(@Payload PaymentProcessedEvent event, Acknowledgment ack) {
        processingService.processEvent(event, ack);
    }
}
