package com.forvmom.MomentForeverBooking.domain.enums;

// PENDING → CONFIRMED → (CANCELLED)
// PENDING → FAILED
public enum BookingStatus {
    PENDING,
    CONFIRMED,
    FAILED,
    CANCELLED
}
