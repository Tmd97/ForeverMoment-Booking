package com.forvmom.MomentForeverBooking.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class OutgoingOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutgoingOutboxPublisher.class);
    private static final int MAX_RETRIES = 5;
    private static final long GRACE_PERIOD_MINUTES = 1;

    private final OutgoingOutboxDao outgoingOutboxDao;
    private final BookingEventProducer eventProducer;
    private final ObjectMapper objectMapper;
    private final AlertService alertService;
    private final OutgoingOutboxService outboxService;

    public OutgoingOutboxPublisher(OutgoingOutboxDao outgoingOutboxDao,
                                   BookingEventProducer eventProducer,
                                   ObjectMapper objectMapper,
                                   AlertService alertService,
                                   OutgoingOutboxService outboxService) {
        this.outgoingOutboxDao = outgoingOutboxDao;
        this.eventProducer = eventProducer;
        this.objectMapper = objectMapper;
        this.alertService = alertService;
        this.outboxService = outboxService;
    }

    @Transactional
    public void publishPendingEvents() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(GRACE_PERIOD_MINUTES);
        List<OutgoingOutboxRecord> pending = outgoingOutboxDao.findByStatusInAndUpdatedAtBefore(
                List.of(OutgoingOutboxRecord.STATUS_PENDING, OutgoingOutboxRecord.STATUS_FAILED),
                cutoff);

        for (OutgoingOutboxRecord record : pending) {
            if (record.getRetryCount() >= MAX_RETRIES) {
                handleDeadRecord(record);
                continue;
            }

            try {
                sendToKafka(record);
                outboxService.markAsProcessed(record);
                log.info("Outgoing outbox record sent: id={}, type={}, bookingId={}",
                        record.getId(), record.getEventType(), record.getBookingId());
            } catch (Exception e) {
                log.error("Failed to send outgoing outbox record id={}, type={}, bookingId={}",
                        record.getId(), record.getEventType(), record.getBookingId(), e);
                outboxService.incrementRetry(record);
                outboxService.markAsFailed(record);
            }
        }
    }

    private void sendToKafka(OutgoingOutboxRecord record) throws Exception {
        String eventType = record.getEventType();
        String payload = record.getPayload();

        if ("PAYMENT_REQUESTED".equals(eventType)) {
            PaymentRequestedEvent event = objectMapper.readValue(payload, PaymentRequestedEvent.class);
            eventProducer.sendPaymentRequestedEvent(event);
        } else {
            log.warn("Unknown outgoing outbox event type: {}", eventType);
        }
    }

    private void handleDeadRecord(OutgoingOutboxRecord record) {
        outboxService.markAsDead(record);
        //TODO: might be revert the inventory if the event is not sent after max retries, but currently just send alert to ops team
        alertService.sendAlert("Outgoing outbox record " + record.getId() + " for booking " +
                record.getBookingId() + " moved to DEAD after max retries");
    }
}