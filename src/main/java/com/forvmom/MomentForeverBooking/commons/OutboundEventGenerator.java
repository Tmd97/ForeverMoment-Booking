package com.forvmom.MomentForeverBooking.commons;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.events.*;

import java.time.LocalDateTime;

public class OutboundEventGenerator {

    public static OutboundEvent buildOutboundEvent(Booking booking, String eventType, InboundEvent inboundEvent) {

        if (eventType != null) {
            if (eventType.equals(EventConstants.BOOKING_FAILED)) {
                return buildBookingFailedEvent(booking, inboundEvent);
            }

            if (eventType.equals(EventConstants.BOOKING_CONFIRMED)) {
                return buildBookingConfirmedEvent(booking, inboundEvent);
            }

            if (eventType.equals(EventConstants.PAYMENT_REQUESTED)) {
                return buildPaymentRequestedEvent(booking, inboundEvent);
            }
        }
        throw new IllegalArgumentException("Unsupported event type: " + eventType);
    }

    private static OutboundEvent buildBookingConfirmedEvent(Booking booking, InboundEvent inboundEvent) {
        BookingConfirmedEvent e = new BookingConfirmedEvent();
        e.setBookingId(booking.getBookingId());
        e.setConfirmedAt(LocalDateTime.now());
        e.setUserId(booking.getUserId());
        e.setUserEmail(booking.getUserEmail());
        e.setExperienceId(booking.getExperienceId());
        e.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        e.setGuestCount(booking.getGuestCount());
        return e;
    }

    private static PaymentRequestedEvent buildPaymentRequestedEvent(Booking booking, InboundEvent inboundEvent) {
        PaymentRequestedEvent e = new PaymentRequestedEvent();
        e.setBookingId(booking.getBookingId());
        e.setRequestedAt(LocalDateTime.now());
        e.setGrandTotal(booking.getGrandTotal());
        e.setUserId(booking.getUserId());
        e.setUserEmail(booking.getUserEmail());
        e.setExperienceId(booking.getExperienceId());
        e.setExperienceName(booking.getExperienceName());
        e.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        e.setGuestCount(booking.getGuestCount());
        e.setCurrency("INR");
        return e;
    }

    private static BookingFailedEvent buildBookingFailedEvent(Booking booking, InboundEvent inboundEvent) {

        if(booking==null){
            //create minimal event with bookingId and failure reason
                BookingFailedEvent bookingFailedEvent = new BookingFailedEvent();
                bookingFailedEvent.setBookingId(inboundEvent.getBookingId());
                bookingFailedEvent.setFailedAt(LocalDateTime.now());
                bookingFailedEvent.setFailureReason("Booking not found for id: " + inboundEvent.getBookingId());
                return bookingFailedEvent;
        }
        BookingFailedEvent bookingFailedEvent = new BookingFailedEvent();
        bookingFailedEvent.setBookingId(booking.getBookingId());
        bookingFailedEvent.setFailedAt(LocalDateTime.now());
        bookingFailedEvent.setFailureReason("TO BE FILLED BY CALLER");
        bookingFailedEvent.setUserId(booking.getUserId());
        bookingFailedEvent.setUserEmail(booking.getUserEmail());
        bookingFailedEvent.setExperienceId(booking.getExperienceId());
        bookingFailedEvent.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        bookingFailedEvent.setGuestCount(booking.getGuestCount());
        return bookingFailedEvent;
    }


}
