package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.InboundEvent;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface BookingService {

    // Inbound event processing (already exists)
    OutgoingOutboxRecord processBookingRequest(InboundEvent event);

    // Query methods
    Booking getBookingByBookingId(String bookingId);
    Page<Booking> getBookingsByUser(Long userId, Pageable pageable);

    // Update methods (admin triggered)
    void cancelBooking(String bookingId);
}