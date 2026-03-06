package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.commons.OutboundEventGenerator;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import com.forvmom.MomentForeverBooking.exception.BookingNotFoundException;
import com.forvmom.MomentForeverBooking.exception.BookingStatusConflictException;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import static com.forvmom.MomentForeverBooking.domain.enums.BookingStatus.CONFIRMED;

@Component
public class PaymentProcessedProcessor  implements InboundBookingEventProcessor  {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessedProcessor.class);


    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private OutgoingOutboxService outgoingOutboxService;

    @Override
    public OutgoingOutboxRecord process(InboundEvent event) {

        Booking booking = bookingRepository.findByBookingId(event.getBookingId()).orElseThrow(() -> new BookingNotFoundException("Booking not found for id: " + event.getBookingId()));
        if (booking.getStatus() == CONFIRMED)
            return null; // idempotent

        if (booking.getStatus() == BookingStatus.CANCELLED ||
                booking.getStatus() == BookingStatus.FAILED) {
            throw new BookingStatusConflictException(event.getBookingId(), booking.getStatus().name(), "confirm");
        }

        OutgoingOutboxRecord record = null;
        try {
            booking.setStatus(CONFIRMED);
            booking.setConfirmedAt(LocalDateTime.now());
            bookingRepository.save(booking);

            OutboundEventGenerator.buildOutboundEvent(booking,EventConstants.BOOKING_CONFIRMED,event);

            BookingConfirmedEvent bookingConfirmedEvent = new BookingConfirmedEvent();
            bookingConfirmedEvent.setBookingId(booking.getBookingId());
            bookingConfirmedEvent.setUserId(booking.getUserId());
            bookingConfirmedEvent.setUserEmail(booking.getUserEmail());
            bookingConfirmedEvent.setExperienceId(booking.getExperienceId());
            bookingConfirmedEvent.setTimeSlotMapperId(booking.getTimeSlotMapperId());
            bookingConfirmedEvent.setGuestCount(booking.getGuestCount());
            bookingConfirmedEvent.setConfirmedAt(booking.getConfirmedAt());

            record = outgoingOutboxService.createRecord(
                    event.getBookingId(), EventConstants.BOOKING_CONFIRMED, event);
            log.info("Booking confirmed (outbox created): {}", event.getBookingId());
        } catch (Exception e) {
            log.warn("Failed to process, so marked as FAILED for bookingId={}: {}", event.getBookingId(), e.getMessage());
            outgoingOutboxService.markAsFailed(record);
        }
        return record;
    }

    @Override
    public String getSupportedEventType() {
        return EventConstants.PAYMENT_PROCESSED;
    }
}
