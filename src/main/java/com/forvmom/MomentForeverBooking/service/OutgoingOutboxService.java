package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.BookingOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.repository.BookingOutboxDao;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.utils.JsonUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class OutgoingOutboxService {
    private final OutgoingOutboxDao outgoingOutboxDao;

    public OutgoingOutboxService(OutgoingOutboxDao outgoingOutboxDao) {
        this.outgoingOutboxDao = outgoingOutboxDao;
    }

//    @Transactional
//    public BookingOutbox findOrCreateForEvent(PaymentRequestedEvent event) {
//        String bookingId = event.getBookingId();
//        Optional<BookingOutbox> existing = outgoingOutboxDao.findByStatusInAndUpdatedAtBefore(bookingId);
//        return existing.orElseGet(() -> createNewOutbox(event));
//    }

    public OutgoingOutboxRecord createOutgoingOutBox(PaymentRequestedEvent event) {
        OutgoingOutboxRecord outgoingOutboxRecord = new OutgoingOutboxRecord();
        outgoingOutboxRecord.setBookingId(event.getBookingId());
        outgoingOutboxRecord.setEventType("PAYMENT_REQUESTED");
        outgoingOutboxRecord.setStatus(OutgoingOutboxRecord.STATUS_PENDING);
        outgoingOutboxRecord.setRetryCount(0);
        outgoingOutboxRecord.setPayload(JsonUtils.toJson(event));
        return outgoingOutboxDao.save(outgoingOutboxRecord);
    }

    @Transactional
    public void markAsProcessing(OutgoingOutboxRecord outgoingOutboxRecord) {
        outgoingOutboxRecord.setStatus(BookingOutbox.STATUS_PROCESSING);
        outgoingOutboxRecord.setUpdatedAt(LocalDateTime.now());
        outgoingOutboxDao.save(outgoingOutboxRecord);
    }

    @Transactional
    public void markAsProcessed(OutgoingOutboxRecord outgoingOutboxRecord) {
        outgoingOutboxRecord.setStatus(OutgoingOutboxRecord.STATUS_SENT);
        outgoingOutboxRecord.setUpdatedAt(LocalDateTime.now());
        outgoingOutboxDao.save(outgoingOutboxRecord);
    }

    @Transactional
    public void markAsFailed(OutgoingOutboxRecord outgoingOutboxRecord) {
        outgoingOutboxRecord.setStatus(BookingOutbox.STATUS_FAILED);
        outgoingOutboxRecord.setUpdatedAt(LocalDateTime.now());
        outgoingOutboxDao.save(outgoingOutboxRecord);
    }

    @Transactional
    public void markAsDead(OutgoingOutboxRecord outgoingOutboxRecord) {
        outgoingOutboxRecord.setStatus(BookingOutbox.STATUS_DEAD);
        outgoingOutboxRecord.setUpdatedAt(LocalDateTime.now());
        outgoingOutboxDao.save(outgoingOutboxRecord);
    }

    @Transactional
    public void incrementRetry(OutgoingOutboxRecord outgoingOutboxRecord) {
        outgoingOutboxRecord.setRetryCount(outgoingOutboxRecord.getRetryCount() + 1);
        outgoingOutboxRecord.setStatus(BookingOutbox.STATUS_PROCESSING);
        outgoingOutboxRecord.setUpdatedAt(LocalDateTime.now());
        outgoingOutboxDao.save(outgoingOutboxRecord);
    }
}