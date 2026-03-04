package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.repository.BookingOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class OutboxService {
    private final BookingOutboxDao outboxDao;

    public OutboxService(BookingOutboxDao outboxDao) {
        this.outboxDao = outboxDao;
    }

    @Transactional
    public BookingOutbox findOrCreateForEvent(BookingRequestEvent event) {
        String bookingId = event.getBookingId();
        Optional<BookingOutbox> existing = outboxDao.findByBookingReferenceId(bookingId);
        return existing.orElseGet(() -> createNewOutbox(event));
    }

    private BookingOutbox createNewOutbox(BookingRequestEvent event) {
        BookingOutbox outbox = new BookingOutbox();
        outbox.setBookingReferenceId(event.getBookingId());
        outbox.setEventType("BOOKING_REQUESTED");
        outbox.setStatus(BookingOutbox.STATUS_NEW);
        outbox.setRetryCount(0);
        outbox.setPayload(JsonUtils.toJson(event));
        return outboxDao.save(outbox);
    }

    @Transactional
    public void markAsProcessing(BookingOutbox outbox) {
        outbox.setStatus(BookingOutbox.STATUS_PROCESSING);
        outbox.setUpdatedAt(LocalDateTime.now());
        outboxDao.save(outbox);
    }

    @Transactional
    public void markAsProcessed(BookingOutbox outbox) {
        outbox.setStatus(BookingOutbox.STATUS_PROCESSED);
        outbox.setUpdatedAt(LocalDateTime.now());
        outboxDao.save(outbox);
    }

    @Transactional
    public void markAsFailed(BookingOutbox outbox) {
        outbox.setStatus(BookingOutbox.STATUS_FAILED);
        outbox.setUpdatedAt(LocalDateTime.now());
        outboxDao.save(outbox);
    }

    @Transactional
    public void markAsDead(BookingOutbox outbox) {
        outbox.setStatus(BookingOutbox.STATUS_DEAD);
        outbox.setUpdatedAt(LocalDateTime.now());
        outboxDao.save(outbox);
    }

    @Transactional
    public void incrementRetry(BookingOutbox outbox) {
        outbox.setRetryCount(outbox.getRetryCount() + 1);
        outbox.setStatus(BookingOutbox.STATUS_PROCESSING);
        outbox.setUpdatedAt(LocalDateTime.now());
        outboxDao.save(outbox);
    }
}