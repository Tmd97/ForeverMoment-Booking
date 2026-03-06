package com.forvmom.MomentForeverBooking.domain.entity;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "inbound_outbox", indexes = {
        @Index(name = "idx_inbound_outbox_status_updated", columnList = "status, updated_at"),
        @Index(name = "idx_inbound_outbox_ref", columnList = "booking_ref_id", unique = true)
})
public class InboundOutbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** e.g. MFB-1735000000000-A3F2 — idempotency key for incoming events */
    @Column(name = "booking_ref_id", nullable = false, unique = true)
    private String bookingReferenceId;

    /** e.g. "BOOKING_REQUESTED" */
    @Column(name = "event_type", nullable = false)
    private String eventType;

    /** Full JSON payload of the incoming event — used by Quartz for retry */
    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    @Column(name = "status", nullable = false)
    private String status = EventConstants.PENDING;

    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = this.createdAt;
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    // ── Getters & Setters ──────────────────────────────────────────────────────

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBookingReferenceId() {
        return bookingReferenceId;
    }

    public void setBookingReferenceId(String v) {
        this.bookingReferenceId = v;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String v) {
        this.eventType = v;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String v) {
        this.payload = v;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String v) {
        this.status = v;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int v) {
        this.retryCount = v;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime v) {
        this.createdAt = v;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime v) {
        this.updatedAt = v;
    }
}