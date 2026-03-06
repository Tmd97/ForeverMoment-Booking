package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.commons.OutboundEventGenerator;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.*;
import com.forvmom.MomentForeverBooking.mapper.BookingMapper;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BookingRequestProcessor implements InboundBookingEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(BookingRequestProcessor.class);

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private OutgoingOutboxService outgoingOutboxService;

    /**
     * Process incoming booking request:
     * 1) Check for duplicate bookingId
     * 2) Persist new PENDING booking
     * 3) Create PAYMENT_REQUESTED event in outbox
     */

    @Override
    public OutgoingOutboxRecord process(InboundEvent event) {
        if (bookingRepository.existsByBookingId(event.getBookingId())) {
            log.warn("Duplicate booking request ignored. bookingId={}", event.getBookingId());
            return null;
        }

        BookingRequestEvent bookingRequestEvent = (BookingRequestEvent) event;
        Booking booking = BookingMapper.fromBookingRequestEvent(bookingRequestEvent);
        bookingRepository.save(booking);
        log.info("Persisted PENDING booking: bookingId={}", booking.getBookingId());

        OutboundEvent paymentEvent = OutboundEventGenerator.buildOutboundEvent(booking,EventConstants.PAYMENT_REQUESTED,event);
        OutgoingOutboxRecord record = outgoingOutboxService.createRecord(
                booking.getBookingId(), EventConstants.PAYMENT_REQUESTED, paymentEvent);
        log.debug("Created outgoing outbox record id={} for PAYMENT_REQUESTED bookingId={}",
                record.getId(), booking.getBookingId());
        return record;
    }


    @Override
    public String getSupportedEventType() {
        return EventConstants.BOOKING_REQUESTED;
    }
}
