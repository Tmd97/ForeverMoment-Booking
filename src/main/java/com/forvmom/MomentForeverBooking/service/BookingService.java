package com.forvmom.MomentForeverBooking.service;

import com.forvmom.common.dto.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface BookingService {

    /**
     * Processes the incoming booking request mapped from Core:
     * Saves the booking locally (PENDING), then triggers payment request.
     */
    void processBookingRequest(BookingRequestEvent event);

    Booking getBookingByBookingId(String bookingId);

    Page<Booking> getBookingsByUser(Long userId, Pageable pageable);

    void confirmBooking(String bookingId);

    void failBooking(String bookingId, String reason);

    void cancelBooking(String bookingId);
}
