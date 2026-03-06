package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.domain.entity.InboundOutbox;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.*;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import com.forvmom.MomentForeverBooking.service.BookingService;
import com.forvmom.MomentForeverBooking.service.BookingServiceImpl;
import com.forvmom.MomentForeverBooking.service.InboundOutboxService;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import com.forvmom.MomentForeverBooking.service.inbound.InboundBookingEventProcessor;
import com.forvmom.MomentForeverBooking.service.inbound.InboundEventProcessorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Quartz-triggered retry handler for the <em>incoming</em>
 * {@link InboundOutbox}.
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
public class InboundOutboxRetryService {

    private static final Logger log = LoggerFactory.getLogger(InboundOutboxRetryService.class);
    private static final int MAX_RETRIES = 5;
    private static final long GRACE_PERIOD_MINUTES = 2;


    // Mapping from inbound event type to the expected outgoing event type
    private static final Map<String, String> OUTGOING_EVENT_TYPE_MAP = Map.of(
            EventConstants.BOOKING_REQUESTED, EventConstants.PAYMENT_REQUESTED,
            EventConstants.PAYMENT_PROCESSED, EventConstants.BOOKING_CONFIRMED,
            EventConstants.PAYMENT_FAILED, EventConstants.BOOKING_FAILED
    );

    // Mapping from inbound event type to the corresponding event class for deserialization
    private static final Map<String, Class<? extends InboundEvent>> EVENT_CLASS_MAP = Map.of(
            EventConstants.BOOKING_REQUESTED, BookingRequestEvent.class,
            EventConstants.PAYMENT_PROCESSED, PaymentProcessedEvent.class,
            EventConstants.PAYMENT_FAILED, PaymentFailedEvent.class
    );

    private final InboundOutboxDao inboundOutboxDao;
    private final InboundOutboxService inboundOutboxService;
    private final OutboxDeadLetterHandler deadLetterHandler;
    private final OutgoingOutboxDao outgoingOutboxDao;
    private final OutgoingOutboxService outgoingOutboxService;
    private final OutgoingOutboxPublisher outgoingOutboxPublisher;
    private final BookingService bookingService;
    private final BookingEventProducer bookingEventProducer;
    private final ObjectMapper objectMapper;
    private final InboundEventProcessorRegistry inboundEventProcessorRegistry;

    public InboundOutboxRetryService(InboundOutboxDao inboundOutboxDao,
                                     InboundOutboxService inboundOutboxService,
                                     OutboxDeadLetterHandler deadLetterHandler,
                                     OutgoingOutboxDao outgoingOutboxDao,
                                     OutgoingOutboxService outgoingOutboxService,
                                     OutgoingOutboxPublisher outgoingOutboxPublisher,
                                     BookingService bookingService,
                                     BookingEventProducer bookingEventProducer,
                                     ObjectMapper objectMapper, InboundEventProcessorRegistry inboundEventProcessorRegistry) {

        this.inboundOutboxDao = inboundOutboxDao;
        this.inboundOutboxService = inboundOutboxService;
        this.deadLetterHandler = deadLetterHandler;
        this.outgoingOutboxDao = outgoingOutboxDao;
        this.outgoingOutboxService = outgoingOutboxService;
        this.outgoingOutboxPublisher = outgoingOutboxPublisher;
        this.bookingService = bookingService;
        this.bookingEventProducer = bookingEventProducer;
        this.objectMapper = objectMapper;
        this.inboundEventProcessorRegistry = inboundEventProcessorRegistry;
    }

    /**
     * Called by Quartz every 2 minutes.
     * Finds incoming inboundOutbox records stuck in PROCESSING or FAILED and retries them.
     */
    @Transactional
    public void retryStuckAndFailedRecords() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(GRACE_PERIOD_MINUTES);

        // check if it is incoming or outgoing record stuck, if incoming, we have to
        // re-run the enrichment and publish flow, if outgoing, we only need to
        // re-publish, no enrichment needed as it is already enriched before

        List<InboundOutbox> stuck = inboundOutboxDao.findByStatusInAndUpdatedAtBefore(
                List.of(EventConstants.PENDING, EventConstants.FAILED),
                cutoff);

        if (!stuck.isEmpty()) {
            log.info("Incoming inboundOutbox poller found {} stuck record(s) to retry", stuck.size());
        }

        for (InboundOutbox inboundOutbox : stuck) {
            if (inboundOutbox.getRetryCount() >= MAX_RETRIES) {
                deadLetterHandler.handleDeadRecord(inboundOutbox);
                continue;
            }
            inboundOutboxService.incrementRetry(inboundOutbox);
            String bookingId = inboundOutbox.getBookingReferenceId();
            String eventType = inboundOutbox.getEventType();

            // Determine expected outgoing event type
            String outgoingEventType = OUTGOING_EVENT_TYPE_MAP.get(eventType);
            if (outgoingEventType == null) {
                log.warn("No outgoing event mapping for event type: {}", eventType);
                inboundOutboxService.markAsFailed(inboundOutbox);
                continue;
            }
            try {
                OutgoingOutboxRecord outgoingOutboxRecord = handleIncomingEventRetry(inboundOutbox, bookingId, eventType, outgoingEventType);
                if (outgoingOutboxRecord != null) {
                    outgoingOutboxPublisher.trySinglePublish(outgoingOutboxRecord);
                    outgoingOutboxService.markAsSent(outgoingOutboxRecord);
                }
            } catch (Exception e) {
                log.warn("Retry failed for bookingId={}, eventType={}, error: {}", bookingId, eventType, e.getMessage());
                inboundOutboxService.markAsFailed(inboundOutbox);
            }
        }
    }

    private OutgoingOutboxRecord handleIncomingEventRetry(InboundOutbox inboundOutbox, String bookingId, String eventType, String outgoingEventType) throws JsonProcessingException {
        // Branch A: Check if outgoing record already exists
        Optional<OutgoingOutboxRecord> existingOutgoing = outgoingOutboxDao.findByBookingIdAndEventType(
                bookingId, outgoingEventType);
        if (existingOutgoing.isPresent()) {
            return existingOutgoing.get();
        } else {
            // Branch B: No outgoing record, re-run the full process
            log.info("[Branch-B] No outgoing record for bookingId={} — re-running processBookingRequest()", bookingId);
            InboundBookingEventProcessor inboundBookingEventProcessor = inboundEventProcessorRegistry.getProcessor(eventType);
            InboundEvent inboundEvent = objectMapper.readValue(inboundOutbox.getPayload(), EVENT_CLASS_MAP.get(eventType));
            return inboundBookingEventProcessor.process(inboundEvent);
        }
    }
}