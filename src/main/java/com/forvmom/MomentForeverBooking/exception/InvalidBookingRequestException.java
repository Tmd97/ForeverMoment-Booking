package com.forvmom.MomentForeverBooking.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Thrown when a booking event payload is invalid or missing required fields.
 * Maps to HTTP 400 BAD_REQUEST.
 */
@ResponseStatus(HttpStatus.BAD_REQUEST)
public class InvalidBookingRequestException extends RuntimeException {

    public InvalidBookingRequestException(String message) {
        super(message);
    }

    public InvalidBookingRequestException(String field, String reason) {
        super(String.format("Invalid booking request — field '%s': %s", field, reason));
    }
}
