package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.retries_cleanup.OutgoingOutboxPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InboundEventProcessorService {

    private static final Logger log = LoggerFactory.getLogger(InboundEventProcessorService.class);
    private final InboundEventProcessorRegistry registry;
    private final InboundOutboxService inboundOutboxService;
    private final OutgoingOutboxPublisher outgoingOutboxPublisher;

    public InboundEventProcessorService(InboundEventProcessorRegistry registry,
                                        InboundOutboxService inboundOutboxService,
                                         OutgoingOutboxPublisher outgoingOutboxPublisher) {
        this.registry = registry;
        this.inboundOutboxService = inboundOutboxService;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
    }

    @Transactional
    public void processEvent(InboundEvent event, Acknowledgment ack) {
        String bookingId = event.getBookingId();
        String eventType = event.getEventType();
        log.info("Processing inbound event: {} for bookingId={}", eventType, bookingId);

        // Idempotency guard
        InboundOutbox inboundOutbox = inboundOutboxService.findOrCreateForEvent(event);
        if (EventConstants.PROCESSED.equals(inboundOutbox.getStatus())) {
            log.info("Duplicate event ignored (already PROCESSED): {} for bookingId={}", eventType, bookingId);
            ack.acknowledge();
            return;
        }

        // ACK immediately – we'll rely on retry mechanism if processing fails
        ack.acknowledge();

        try {
            inboundOutboxService.markAsProcessing(inboundOutbox);
            InboundBookingEventProcessor processor = registry.getProcessor(eventType);
            OutgoingOutboxRecord outgoingRecord = processor.process(event);
            inboundOutboxService.markAsProcessed(inboundOutbox);

            // Publish outside the transaction (no DB connection held)
            if (outgoingRecord != null) {
                outgoingOutboxPublisher.trySinglePublish(outgoingRecord);
            }
            log.info("Successfully processed inbound event: {} for bookingId={}", eventType, bookingId);
        } catch (Exception e) {
            log.error("Failed to process inbound event: {} for bookingId={}", eventType, bookingId, e);
            inboundOutboxService.markAsFailed(inboundOutbox);
        }
    }
}