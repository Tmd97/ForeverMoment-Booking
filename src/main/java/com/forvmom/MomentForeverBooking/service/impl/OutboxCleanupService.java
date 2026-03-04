package com.forvmom.MomentForeverBooking.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.repository.BookingOutboxDao;
import com.forvmom.MomentForeverBooking.service.BookingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class OutboxCleanupService {

    private static final Logger logger = LoggerFactory.getLogger(OutboxCleanupService.class);
    private static final int MAX_RETRIES = 5;
    private static final long GRACE_PERIOD_MINUTES = 2;
    private static final int HOURS_TO_KEEP = 24;

    private final BookingOutboxDao outboxDao;
    private final BookingService bookingService;
    private final BookingEventProducer eventProducer;
    private final ObjectMapper objectMapper;

    public OutboxCleanupService(BookingOutboxDao outboxDao,
                                BookingService bookingService,
                                BookingEventProducer eventProducer,
                                ObjectMapper objectMapper) {
        this.outboxDao = outboxDao;
        this.bookingService = bookingService;
        this.eventProducer = eventProducer;
        this.objectMapper = objectMapper;
    }

    // Cleanup old PROCESSED records
    public void cleanupPublishedRecords() {
        LocalDateTime cutoff = LocalDateTime.now().minusHours(HOURS_TO_KEEP);
        int deleted = outboxDao.deleteByStatusAndUpdatedAtBefore(BookingOutbox.STATUS_PROCESSED, cutoff);
        if (deleted > 0) {
            logger.info("Cleaned up {} processed records older than {}", deleted, cutoff);
        }
    }

    // Retry stuck (PROCESSING) and failed (FAILED) records
    public void retryStuckAndFailedRecords() {
        LocalDateTime graceCutoff = LocalDateTime.now().minusMinutes(GRACE_PERIOD_MINUTES);
        List<BookingOutbox> stuckRecords = outboxDao.findByStatusInAndUpdatedAtBefore(
                List.of(BookingOutbox.STATUS_PROCESSING, BookingOutbox.STATUS_FAILED),
                graceCutoff);

        for (BookingOutbox outbox : stuckRecords) {
            if (outbox.getRetryCount() >= MAX_RETRIES) {
                handleDeadRecord(outbox);
                continue;
            }

            try {
                // Increment retry count and set status to PROCESSING
                outbox.setRetryCount(outbox.getRetryCount() + 1);
                outbox.setStatus(BookingOutbox.STATUS_PROCESSING);
                outbox.setUpdatedAt(LocalDateTime.now());
                outboxDao.save(outbox);

                // Deserialize payload and reprocess
                reprocessEvent(outbox);

                // If success, mark as PROCESSED
                outbox.setStatus(BookingOutbox.STATUS_PROCESSED);
                outbox.setUpdatedAt(LocalDateTime.now());
                outboxDao.save(outbox);
                logger.info("Retry succeeded for outbox id={}, booking={}",
                        outbox.getId(), outbox.getBookingReferenceId());

            } catch (Exception e) {
                logger.error("Retry failed for outbox id={}, booking={}",
                        outbox.getId(), outbox.getBookingReferenceId(), e);
                outbox.setStatus(BookingOutbox.STATUS_FAILED);
                outbox.setUpdatedAt(LocalDateTime.now());
                outboxDao.save(outbox);
            }
        }
    }

    private void reprocessEvent(BookingOutbox outbox) throws Exception {
        String eventType = outbox.getEventType();
        String payload = outbox.getPayload();

        if ("BOOKING_REQUESTED".equals(eventType)) {
            BookingRequestEvent event = objectMapper.readValue(payload, BookingRequestEvent.class);
            bookingService.processBookingRequest(event);
        } else {
            // Handle other event types if needed (e.g., PAYMENT_CONFIRMED)
            logger.warn("Unknown event type for reprocessing: {}", eventType);
        }
    }

    private void handleDeadRecord(BookingOutbox outbox) {
        outbox.setStatus(BookingOutbox.STATUS_DEAD);
        outbox.setUpdatedAt(LocalDateTime.now());
        outboxDao.save(outbox);

        try {
            // Deserialize the original request event from the payload
            BookingRequestEvent requestEvent = objectMapper.readValue(outbox.getPayload(), BookingRequestEvent.class);

            // Build a BookingFailedEvent from the request data
            BookingFailedEvent failedEvent = new BookingFailedEvent();
            failedEvent.setBookingId(requestEvent.getBookingId());
            failedEvent.setUserId(requestEvent.getUserId());
            failedEvent.setUserEmail(requestEvent.getUserEmail());
            failedEvent.setExperienceId(requestEvent.getExperienceId());
            failedEvent.setTimeSlotMapperId(requestEvent.getTimeSlotMapperId());
            failedEvent.setGuestCount(requestEvent.getGuestCount());
            failedEvent.setFailureReason("Outbox record moved to DEAD after max retries");
            failedEvent.setFailedAt(LocalDateTime.now());

            eventProducer.sendBookingFailed(failedEvent);
            logger.info("BookingFailedEvent sent for dead booking: {}", failedEvent.getBookingId());

        } catch (Exception e) {
            // Log the error but don't rethrow – the record is already marked DEAD
            logger.error("Failed to send BookingFailedEvent for dead outbox id={}, bookingReferenceId={}",
                    outbox.getId(), outbox.getBookingReferenceId(), e);
            // Optionally send an alert with more details
            sendAlert("Failed to send failure event for dead record " + outbox.getId());
        }

        sendAlert("Outbox record " + outbox.getId() + " for booking " +
                outbox.getBookingReferenceId() + " moved to DEAD after max retries");
    }

    private void sendAlert(String message) {
        // Implement actual alerting (email, Slack, PagerDuty, etc.)
        logger.error("ALERT: {}", message);
    }
}