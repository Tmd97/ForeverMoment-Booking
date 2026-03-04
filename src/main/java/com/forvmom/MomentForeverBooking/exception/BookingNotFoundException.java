package com.forvmom.MomentForeverBooking.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Thrown when a booking record cannot be found by ID.
 * Maps to HTTP 404 NOT_FOUND.
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
public class BookingNotFoundException extends RuntimeException {

    private final String bookingId;

    public BookingNotFoundException(String bookingId) {
        super("Booking not found: " + bookingId);
        this.bookingId = bookingId;
    }

    public String getBookingId() {
        return bookingId;
    }
}
