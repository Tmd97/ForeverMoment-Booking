package com.forvmom.MomentForeverBooking.events;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class PaymentRequestedEvent {

    private String bookingId;
    private Long userId;
    private String userEmail;
    private BigDecimal grandTotal;
    private String currency;
    private LocalDateTime requestedAt;

    public PaymentRequestedEvent() {
    }

    public PaymentRequestedEvent(String bookingId,
                                 Long userId,
                                 String userEmail,
                                 BigDecimal grandTotal,
                                 String currency,
                                 LocalDateTime requestedAt) {
        this.bookingId = bookingId;
        this.userId = userId;
        this.userEmail = userEmail;
        this.grandTotal = grandTotal;
        this.currency = currency;
        this.requestedAt = requestedAt;
    }

    public String getBookingId() {
        return bookingId;
    }

    public void setBookingId(String bookingId) {
        this.bookingId = bookingId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public BigDecimal getGrandTotal() {
        return grandTotal;
    }

    public void setGrandTotal(BigDecimal grandTotal) {
        this.grandTotal = grandTotal;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public LocalDateTime getRequestedAt() {
        return requestedAt;
    }

    public void setRequestedAt(LocalDateTime requestedAt) {
        this.requestedAt = requestedAt;
    }
}