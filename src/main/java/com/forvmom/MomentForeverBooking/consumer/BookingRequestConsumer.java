package com.forvmom.MomentForeverBooking.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.repository.BookingOutboxDao;
import com.forvmom.MomentForeverBooking.service.BookingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class BookingRequestConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BookingRequestConsumer.class);

    private final BookingService bookingService;
    private final BookingOutboxDao outboxDao;
    private final ObjectMapper objectMapper;  // for JSON serialization

    public BookingRequestConsumer(BookingService bookingService,
                                  BookingOutboxDao outboxDao,
                                  ObjectMapper objectMapper) {
        this.bookingService = bookingService;
        this.outboxDao = outboxDao;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${kafka.topics.booking-requested}",
            groupId = "booking-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onBookingRequested(@Payload BookingRequestEvent event, Acknowledgment ack) {
        String bookingId = event.getBookingId();
        logger.info("Received booking-requested: {}", bookingId);

        // 1. Find or create outbox record
        BookingOutbox outbox = outboxDao.findByBookingReferenceId(bookingId)
                .orElseGet(() -> {
                    BookingOutbox newOutbox = new BookingOutbox();
                    newOutbox.setBookingReferenceId(bookingId);
                    newOutbox.setEventType("BOOKING_REQUESTED");
                    newOutbox.setStatus(BookingOutbox.STATUS_NEW);
                    newOutbox.setRetryCount(0);
                    try {
                        newOutbox.setPayload(objectMapper.writeValueAsString(event));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Failed to serialize event payload", e);
                    }
                    return outboxDao.save(newOutbox);
                });

        // Idempotency: if already processed, just ack and return
        if (BookingOutbox.STATUS_PROCESSED.equals(outbox.getStatus())) {
            logger.info("Booking already processed, acking and ignoring: {}", bookingId);
            ack.acknowledge();
            return;
        }
        try {
            // Attempt processing (marked as PROCESSING to prevent concurrent attempts)
            outbox.setStatus(BookingOutbox.STATUS_PROCESSING);
            outbox.setUpdatedAt(LocalDateTime.now());
            outboxDao.save(outbox);
            //process the booking request (idempotent)
            bookingService.processBookingRequest(event);
            // If successful, mark as PROCESSED
            outbox.setStatus(BookingOutbox.STATUS_PROCESSED);
            outbox.setUpdatedAt(LocalDateTime.now());
            outboxDao.save(outbox);
            ack.acknowledge();
            logger.info("Successfully processed booking: {}", bookingId);

        } catch (Exception e) {
            // On failure, log the error, mark the outbox as FAILED, and do NOT ack Quarta will handle retries and DLQ after max attempts
            logger.error("Failed to process booking: {}", bookingId, e);
            outbox.setStatus(BookingOutbox.STATUS_FAILED);
            outbox.setUpdatedAt(LocalDateTime.now());
            outboxDao.save(outbox);
            throw e;
        }
    }
}