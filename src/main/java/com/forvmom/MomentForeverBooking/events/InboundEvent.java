package com.forvmom.MomentForeverBooking.events;

public interface InboundEvent {
    String getBookingId();
    String getEventType();

    // Common fields present in all inbound events (add as needed)
//    Long getUserId();
//    String getUserEmail();
//    Long getExperienceId();
//    Long getTimeSlotMapperId();
//    Integer getGuestCount();
    // ... add others if they exist in all events
}