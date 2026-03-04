package com.forvmom.MomentForeverBooking.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Thrown when an illegal or impossible status transition is attempted on a
 * booking.
 * E.g. trying to confirm an already-cancelled booking.
 * Maps to HTTP 409 CONFLICT.
 */
@ResponseStatus(HttpStatus.CONFLICT)
public class BookingStatusConflictException extends RuntimeException {

    private final String bookingId;
    private final String currentStatus;
    private final String attemptedAction;

    public BookingStatusConflictException(String bookingId, String currentStatus, String attemptedAction) {
        super(String.format(
                "Cannot %s booking '%s' — current status is %s.",
                attemptedAction, bookingId, currentStatus));
        this.bookingId = bookingId;
        this.currentStatus = currentStatus;
        this.attemptedAction = attemptedAction;
    }

    public String getBookingId() {
        return bookingId;
    }

    public String getCurrentStatus() {
        return currentStatus;
    }

    public String getAttemptedAction() {
        return attemptedAction;
    }
}
