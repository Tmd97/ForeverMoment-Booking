package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.repository.BookingOutboxDao;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Quartz-triggered retry handler for the <em>incoming</em>
 * {@link BookingOutbox}.
 *
 * <p>
 * Since {@link BookingServiceImpl#processBookingRequest} is
 * {@code @Transactional},
 * partial-state is impossible: either <b>both</b> the Booking and the
 * {@link OutgoingOutboxRecord} were committed, or <b>neither</b> was.
 *
 * <p>
 * Two branches:
 * <ul>
 * <li><b>Branch A — Outgoing record EXISTS</b> (common case: Kafka was down):
 * The transaction already committed. Just call
 * {@link OutgoingOutboxPublisher#trySinglePublish} — <em>zero transaction
 * opened</em>.
 * If the record is already SENT, skip quietly.</li>
 * <li><b>Branch B — Nothing exists</b> (rare: the whole transaction rolled
 * back):
 * Re-run the full {@code processBookingRequest()} — its own
 * {@code existsByBookingId()} guard makes it safe to call multiple times.
 * Then trigger an immediate publish attempt.</li>
 * </ul>
 */
@Service
public class OutboxRetryService {

    private static final Logger log = LoggerFactory.getLogger(OutboxRetryService.class);
    private static final int MAX_RETRIES = 5;
    private static final long GRACE_PERIOD_MINUTES = 2;

    private final BookingOutboxDao outboxDao;
    private final OutboxService outboxService;
    private final OutboxDeadLetterHandler deadLetterHandler;
    private final OutgoingOutboxDao outgoingOutboxDao;
    private final OutgoingOutboxService outgoingOutboxService;
    private final OutgoingOutboxPublisher outgoingOutboxPublisher;
    private final BookingService bookingService;

    public OutboxRetryService(BookingOutboxDao outboxDao,
            OutboxService outboxService,
            OutboxDeadLetterHandler deadLetterHandler,
            OutgoingOutboxDao outgoingOutboxDao,
            OutgoingOutboxService outgoingOutboxService,
            OutgoingOutboxPublisher outgoingOutboxPublisher,
            BookingService bookingService) {
        this.outboxDao = outboxDao;
        this.outboxService = outboxService;
        this.deadLetterHandler = deadLetterHandler;
        this.outgoingOutboxDao = outgoingOutboxDao;
        this.outgoingOutboxService = outgoingOutboxService;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
        this.bookingService = bookingService;
    }

    /**
     * Called by Quartz every 2 minutes.
     * Finds incoming outbox records stuck in PROCESSING or FAILED and retries them.
     */
    @Transactional
    public void retryStuckAndFailedRecords() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(GRACE_PERIOD_MINUTES);

        // check if it is incoming or outgoing record stuck, if incoming, we have to
        // re-run the enrichment and publish flow, if outgoing, we only need to
        // re-publish, no enrichment needed as it is already enriched before

        List<BookingOutbox> stuck = outboxDao.findByStatusInAndUpdatedAtBefore(
                List.of(BookingOutbox.STATUS_PROCESSING, BookingOutbox.STATUS_FAILED),
                cutoff);

        if (!stuck.isEmpty()) {
            log.info("Incoming outbox poller found {} stuck record(s) to retry", stuck.size());
        }

        for (BookingOutbox outbox : stuck) {
            if (outbox.getRetryCount() >= MAX_RETRIES) {
                deadLetterHandler.handleDeadRecord(outbox);
                continue;
            }
            try {
                outboxService.incrementRetry(outbox);
                // inbound request events get processed(payment request event publish)
                // is part of it as whole inbound
                reprocessEvent(outbox);
                outboxService.markAsProcessed(outbox);
                log.info("Retry succeeded for incoming outbox id={}, booking={}",
                        outbox.getId(), outbox.getBookingReferenceId());
            } catch (Exception e) {
                log.error("Retry failed for incoming outbox id={}, booking={}: {}",
                        outbox.getId(), outbox.getBookingReferenceId(), e.getMessage());
                outboxService.markAsFailed(outbox);
            }
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void reprocessEvent(BookingOutbox outbox) throws Exception {
        if (!"BOOKING_REQUESTED".equals(outbox.getEventType())) {
            log.warn("No retry handler for incoming event type: {}", outbox.getEventType());
            return;
        }

        String bookingId = outbox.getBookingReferenceId();

        // ── Branch A: Outgoing record already exists (common case)
        // ────────────────────
        // The @Transactional method committed: booking + outgoing record are both in
        // DB.
        // Only the Kafka publish failed. Re-publish directly — NO transaction opened.
        Optional<OutgoingOutboxRecord> existingOutgoing = outgoingOutboxDao.findByBookingIdAndEventType(
                bookingId, BookingServiceImpl.EVT_PAYMENT_REQUESTED);

        if (existingOutgoing.isPresent()) {
            OutgoingOutboxRecord record = existingOutgoing.get();
            if (OutgoingOutboxRecord.STATUS_SENT.equals(record.getStatus())) {
                log.info("[Branch-A] payment-requested already SENT for bookingId={} — nothing to do", bookingId);
                return;
            }
            log.info("[Branch-A] Re-publishing existing outbox record id={} (status={}) for bookingId={}",
                    record.getId(), record.getStatus(), bookingId);
            outgoingOutboxPublisher.trySinglePublish(record); // zero DB transaction
            return;
        }

        // ── Branch B: Nothing in DB (transaction rolled back entirely) ───────────────
        // Re-run the full processBookingRequest() — it creates booking + outgoing
        // record
        // atomically, then we immediately try the Kafka publish.
        log.info("[Branch-B] No outgoing record for bookingId={} — re-running processBookingRequest()", bookingId);
        BookingRequestEvent event = JsonUtils.fromJson(outbox.getPayload(), BookingRequestEvent.class);
        OutgoingOutboxRecord newRecord = bookingService.processBookingRequest(event);
        // Immediate publish attempt outside the processBookingRequest() transaction
        outgoingOutboxPublisher.trySinglePublish(newRecord);
    }
}