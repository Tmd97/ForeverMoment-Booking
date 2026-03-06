package com.forvmom.MomentForeverBooking.consumer;

import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.service.inbound.InboundEventProcessorService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class BookingRequestConsumer {

    private final InboundEventProcessorService processingService;

    public BookingRequestConsumer(InboundEventProcessorService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(topics = "${kafka.topics.booking-requested}", groupId = "booking-group", containerFactory = "kafkaListenerContainerFactory")
    public void onBookingRequested(@Payload BookingRequestEvent event, Acknowledgment ack) {
        processingService.processEvent(event, ack);
    }
}