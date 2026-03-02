package com.forvmom.MomentForeverBooking.producer;

import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes outbound events from the Booking Service.
 */
@Service
public class BookingEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(BookingEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public BookingEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPaymentRequested(PaymentRequestedEvent event) {
        logger.info("Publishing payment-requested event for bookingId={}", event.getBookingId());
        kafkaTemplate.send("payment-requested", String.valueOf(event.getBookingId()), event);
    }

    public void sendBookingConfirmed(BookingConfirmedEvent event) {
        logger.info("Publishing booking-confirmed event for bookingId={}", event.getBookingId());
        kafkaTemplate.send("booking-confirmed", String.valueOf(event.getBookingId()), event);
    }

    public void sendBookingFailed(BookingFailedEvent event) {
        logger.info("Publishing booking-failed event for bookingId={}", event.getBookingId());
        kafkaTemplate.send("booking-failed", String.valueOf(event.getBookingId()), event);
    }
}
