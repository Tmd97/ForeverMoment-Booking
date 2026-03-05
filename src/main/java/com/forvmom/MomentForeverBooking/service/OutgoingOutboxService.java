package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Manages lifecycle of {@link OutgoingOutboxRecord} — the outbox for events
 * this service publishes outward (payment-requested, booking-confirmed,
 * booking-failed).
 *
 * <p>
 * Callers save a record via {@link #createRecord(String, String, Object)},
 * then immediately attempt a Kafka publish. If publish fails the record stays
 * PENDING and Quartz picks it up for retry.
 */
@Service
public class OutgoingOutboxService {

    private static final Logger log = LoggerFactory.getLogger(OutgoingOutboxService.class);

    private final OutgoingOutboxDao outgoingOutboxDao;

    public OutgoingOutboxService(OutgoingOutboxDao outgoingOutboxDao) {
        this.outgoingOutboxDao = outgoingOutboxDao;
    }

    /**
     * Creates a PENDING outbox record for any outgoing event.
     *
     * @param bookingId the booking reference this event belongs to
     * @param eventType e.g. "PAYMENT_REQUESTED", "BOOKING_CONFIRMED",
     *                  "BOOKING_FAILED"
     * @param payload   the event object — will be serialised to JSON
     */
    @Transactional
    public OutgoingOutboxRecord createRecord(String bookingId, String eventType, Object payload) {
        OutgoingOutboxRecord record = new OutgoingOutboxRecord();
        record.setBookingId(bookingId);
        record.setEventType(eventType);
        record.setStatus(OutgoingOutboxRecord.STATUS_PENDING);
        record.setRetryCount(0);
        record.setPayload(JsonUtils.toJson(payload));
        OutgoingOutboxRecord saved = outgoingOutboxDao.save(record);
        log.debug("Created outgoing outbox record id={}, type={}, bookingId={}", saved.getId(), eventType, bookingId);
        return saved;
    }

    @Transactional
    public void markAsSent(OutgoingOutboxRecord record) {
        record.setStatus(OutgoingOutboxRecord.STATUS_SENT);
        outgoingOutboxDao.save(record);
    }

    @Transactional
    public void markAsFailed(OutgoingOutboxRecord record) {
        record.setStatus(OutgoingOutboxRecord.STATUS_FAILED);
        outgoingOutboxDao.save(record);
    }

    @Transactional
    public void markAsDead(OutgoingOutboxRecord record) {
        record.setStatus(OutgoingOutboxRecord.STATUS_DEAD);
        outgoingOutboxDao.save(record);
    }

    @Transactional
    public void incrementRetry(OutgoingOutboxRecord record) {
        record.setRetryCount(record.getRetryCount() + 1);
        record.setStatus(OutgoingOutboxRecord.STATUS_FAILED);
        outgoingOutboxDao.save(record);
    }
}