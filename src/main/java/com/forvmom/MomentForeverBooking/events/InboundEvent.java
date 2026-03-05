package com.forvmom.MomentForeverBooking.events;

public interface InboundEvent {
    String getBookingId();
    String getEventType();
}
