package com.forvmom.MomentForeverBooking.producer;

import com.forvmom.MomentForeverBooking.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class BookingEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(BookingEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.booking-confirmed}")
    private String bookingConfirmedTopic;

    @Value("${kafka.topics.booking-failed}")
    private String bookingFailedTopic;

    public BookingEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void sendBookingConfirmed(BookingConfirmedEvent event) {
        kafkaTemplate.send(bookingConfirmedTopic, event.getBookingId(), event);
        logger.info("Sent BookingConfirmedEvent: bookingId={}", event.getBookingId());
    }


    public void sendBookingFailed(BookingFailedEvent event) {
        kafkaTemplate.send(bookingFailedTopic, event.getBookingId(), event);
        logger.info("Sent BookingFailedEvent: bookingId={}", event.getBookingId());
    }

//    @Override
//    public void sendInventoryReleaseRequested(InventoryReleaseRequestedEvent event) {
//        kafkaTemplate.send(inventoryReleaseTopic, event.getBookingId(), event);
//        logger.info("Sent InventoryReleaseRequestedEvent: bookingId={}", event.getBookingId());
//    }
}