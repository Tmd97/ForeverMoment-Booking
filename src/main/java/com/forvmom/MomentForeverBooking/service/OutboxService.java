package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.repository.BookingOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class OutboxService {

    private final BookingOutboxDao outboxDao;
    private final Map<String, InboundEvent> creatorMap;

    public OutboxService(BookingOutboxDao outboxDao, List<InboundEvent> inboundEventList) {
        this.outboxDao = outboxDao;
        this.creatorMap = inboundEventList.stream()
                .collect(Collectors.toMap(InboundEvent::getEventType, e -> e));
    }

    @Transactional
    public BookingOutbox findOrCreateForEvent(InboundEvent event) {
        Optional<BookingOutbox> existing = outboxDao.findByBookingReferenceIdAndEventType(event.getBookingId(), event.getEventType());
        if (existing.isPresent()) {
            return existing.get();
        } else {
            return createNewOutbox(event);
        }

    }

    private BookingOutbox createNewOutbox(InboundEvent event) {
        BookingOutbox outbox = new BookingOutbox();
        outbox.setBookingReferenceId(event.getBookingId());
        outbox.setEventType(event.getEventType());
        outbox.setStatus(BookingOutbox.PENDING);
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