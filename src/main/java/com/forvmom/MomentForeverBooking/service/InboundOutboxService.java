package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class InboundOutboxService {

    private final InboundOutboxDao inboundOutboxDao;
    private final Map<String, InboundEvent> creatorMap;

    public InboundOutboxService(InboundOutboxDao inboundOutboxDao, List<InboundEvent> inboundEventList) {
        this.inboundOutboxDao = inboundOutboxDao;
        this.creatorMap = inboundEventList.stream()
                .collect(Collectors.toMap(InboundEvent::getEventType, e -> e));
    }

    @Transactional
    public InboundOutbox findOrCreateForEvent(InboundEvent event) {
        Optional<InboundOutbox> existing = inboundOutboxDao.findByBookingReferenceIdAndEventType(event.getBookingId(), event.getEventType());
        if (existing.isPresent()) {
            return existing.get();
        } else {
            return createNewOutbox(event);
        }

    }

    private InboundOutbox createNewOutbox(InboundEvent event) {
        InboundOutbox outbox = new InboundOutbox();
        outbox.setBookingReferenceId(event.getBookingId());
        outbox.setEventType(event.getEventType());
        outbox.setStatus(EventConstants.PENDING);
        outbox.setRetryCount(0);
        outbox.setPayload(JsonUtils.toJson(event));
        return inboundOutboxDao.save(outbox);
    }

    @Transactional
    public void markAsProcessing(InboundOutbox outbox) {
        outbox.setStatus(EventConstants.PENDING);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }

    @Transactional
    public void markAsProcessed(InboundOutbox outbox) {
        outbox.setStatus(EventConstants.PROCESSED);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }

    @Transactional
    public void markAsFailed(InboundOutbox outbox) {
        outbox.setStatus(EventConstants.FAILED);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }

    @Transactional
    public void markAsDead(InboundOutbox outbox) {
        outbox.setStatus(EventConstants.DEAD);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }

    @Transactional
    public void incrementRetry(InboundOutbox outbox) {
        outbox.setRetryCount(outbox.getRetryCount() + 1);
        outbox.setStatus(EventConstants.PENDING);
        outbox.setUpdatedAt(LocalDateTime.now());
        inboundOutboxDao.save(outbox);
    }
}