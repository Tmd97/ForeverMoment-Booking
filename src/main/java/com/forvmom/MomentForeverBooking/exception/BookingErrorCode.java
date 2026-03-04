package com.forvmom.MomentForeverBooking.exception;

/**
 * Application-level error codes for the Booking service.
 * Format: BOOKING_<DOMAIN>_<REASON>
 */
public enum BookingErrorCode {

    // ---- Booking ----
    BOOKING_NOT_FOUND,
    BOOKING_ALREADY_EXISTS,
    BOOKING_ALREADY_CANCELLED,
    BOOKING_ALREADY_CONFIRMED,
    BOOKING_ALREADY_FAILED,
    BOOKING_INVALID_STATUS_TRANSITION,

    // ---- Validation ----
    VALIDATION_FAILED,
    INVALID_REQUEST,

    // ---- Kafka / Event Processing ----
    KAFKA_EVENT_PROCESSING_FAILED,
    DUPLICATE_EVENT_IGNORED,

    // ---- Generic ----
    INTERNAL_SERVER_ERROR,
    SERVICE_UNAVAILABLE
}
