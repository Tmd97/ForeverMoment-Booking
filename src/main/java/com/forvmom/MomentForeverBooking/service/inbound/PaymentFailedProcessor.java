package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.commons.OutboundEventGenerator;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.events.OutboundEvent;
import com.forvmom.MomentForeverBooking.exception.BookingNotFoundException;
import com.forvmom.MomentForeverBooking.exception.BookingStatusConflictException;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedProcessor  implements InboundBookingEventProcessor {
    private static final Logger log = LoggerFactory.getLogger(PaymentFailedProcessor.class);

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private OutgoingOutboxService outgoingOutboxService;


    @Override
    public OutgoingOutboxRecord process(InboundEvent event) {
        OutgoingOutboxRecord record = null;
        try {
            Booking booking = bookingRepository.findByBookingId(event.getBookingId()).orElseThrow(() -> new BookingNotFoundException("Booking not found for id: " + event.getBookingId()));
            if (booking.getStatus() == BookingStatus.CONFIRMED)
                return null; // idempotent

            if (booking.getStatus() == BookingStatus.CANCELLED ||
                    booking.getStatus() == BookingStatus.FAILED) {
                throw new BookingStatusConflictException(event.getBookingId(), booking.getStatus().name(), "confirm");
            }
            if (booking.getStatus() == BookingStatus.FAILED)
                return null; // idempotent
            booking.setStatus(BookingStatus.FAILED);
            booking.setFailureReason("Payment failed");
            bookingRepository.save(booking);
            OutboundEvent bookingFailedEvent = OutboundEventGenerator.buildOutboundEvent(booking, event.getEventType(),event);

            record = outgoingOutboxService.createRecord(
                    event.getBookingId(), EventConstants.BOOKING_FAILED, bookingFailedEvent);
            log.warn("Booking failed (outbox created): {} (reason: {})", event.getBookingId(), "Payment failed");
            return record;
        } catch (Exception e) {
            log.warn("Failed to process, so marked as FAILED for bookingId={}: {}", event.getBookingId(), e.getMessage());
            outgoingOutboxService.markAsFailed(record);
        }
        return null;
    }


    @Override
    public String getSupportedEventType() {
        return EventConstants.PAYMENT_FAILED;
    }
}
