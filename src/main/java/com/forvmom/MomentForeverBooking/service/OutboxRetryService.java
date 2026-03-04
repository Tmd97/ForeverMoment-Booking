package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.repository.BookingOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class OutboxRetryService {

    private static final Logger log = LoggerFactory.getLogger(OutboxRetryService.class);
    private static final int MAX_RETRIES = 5;
    private static final long GRACE_PERIOD_MINUTES = 2;

    private final BookingOutboxDao outboxDao;
    private final BookingService bookingService;
    private final OutboxService outboxService;
    private final OutboxDeadLetterHandler deadLetterHandler;

    public OutboxRetryService(BookingOutboxDao outboxDao,
                              BookingService bookingService,
                              OutboxService outboxService,
                              OutboxDeadLetterHandler deadLetterHandler) {
        this.outboxDao = outboxDao;
        this.bookingService = bookingService;
        this.outboxService = outboxService;
        this.deadLetterHandler = deadLetterHandler;
    }

    @Transactional
    public void retryStuckAndFailedRecords() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(GRACE_PERIOD_MINUTES);
        List<BookingOutbox> stuckRecords = outboxDao.findByStatusInAndUpdatedAtBefore(
                List.of(BookingOutbox.STATUS_PROCESSING, BookingOutbox.STATUS_FAILED),
                cutoff);

        for (BookingOutbox outbox : stuckRecords) {
            if (outbox.getRetryCount() >= MAX_RETRIES) {
                deadLetterHandler.handleDeadRecord(outbox);
                continue;
            }

            try {
                outboxService.incrementRetry(outbox);
                reprocessEvent(outbox);
                outboxService.markAsProcessed(outbox);
                log.info("Retry succeeded for outbox id={}, booking={}",
                        outbox.getId(), outbox.getBookingReferenceId());
            } catch (Exception e) {
                log.error("Retry failed for outbox id={}, booking={}",
                        outbox.getId(), outbox.getBookingReferenceId(), e);
                outboxService.markAsFailed(outbox);
            }
        }
    }

    private void reprocessEvent(BookingOutbox outbox) throws Exception {
        String eventType = outbox.getEventType();
        String payload = outbox.getPayload();

        if ("BOOKING_REQUESTED".equals(eventType)) {
            BookingRequestEvent event = JsonUtils.fromJson(payload, BookingRequestEvent.class);
            bookingService.processBookingRequest(event);
        } else {
            log.warn("Unknown event type for reprocessing: {}", eventType);
        }
    }
}