package com.forvmom.MomentForeverBooking.consumer;

import com.forvmom.MomentForeverBooking.dto.response.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.service.BookingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Listens to {@code booking-requested} from Core Service.
 *
 * <p>
 * Idempotent processing: delegates to {@link BookingService} which checks
 * if the booking ID already exists before persisting. Duplicate events are
 * safely acknowledged and ignored.
 */
@Component
public class BookingRequestConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BookingRequestConsumer.class);

    private final BookingService bookingService;

    public BookingRequestConsumer(BookingService bookingService) {
        this.bookingService = bookingService;
    }

    @KafkaListener(topics = "${kafka.topics.booking-requested:booking-requested}", groupId = "booking-group", containerFactory = "kafkaListenerContainerFactory")
    public void onBookingRequested(@Payload BookingRequestEvent event, Acknowledgment ack) {
        logger.info("Received booking-requested: bookingId={}, userId={}",
                event.getBookingId(), event.getUserId());

        try {
            bookingService.processBookingRequest(event);

            // Acknowledge after successful save + downstream event firing.
            ack.acknowledge();
            logger.info("Successfully processed booking-requested: bookingId={}", event.getBookingId());

        } catch (Exception e) {
            logger.error("Failed to process booking-requested for bookingId={}: {}",
                    event.getBookingId(), e.getMessage(), e);
            // Do NOT ack; let Kafka redeliver based on retry policy (DLQ eventually)
            throw e;
        }
    }
}
