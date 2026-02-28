package com.forvmom.MomentForeverBooking.domain.entity;

import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.domain.enums.PricingLevel;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Booking aggregate root — persisted in the Booking service's dedicated
 * database.
 * This entity is created when a booking-requested event is consumed.
 */
@Entity
@Table(name = "bookings", indexes = {
        @Index(name = "idx_booking_user", columnList = "user_id"),
        @Index(name = "idx_booking_status", columnList = "status"),
        @Index(name = "idx_booking_experience", columnList = "experience_id")
})
@Getter
@Setter
public class Booking {

    @Id
    @Column(name = "booking_id", nullable = false, unique = true, length = 60)
    private String bookingId; // e.g., MFB-1735000000000-A3F2

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "user_email", nullable = false, length = 255)
    private String userEmail;

    @Column(name = "user_full_name", length = 255)
    private String userFullName;

    @Column(name = "experience_id", nullable = false)
    private Long experienceId;

    @Column(name = "experience_name", nullable = false, length = 255)
    private String experienceName;

    @Column(name = "experience_slug", nullable = false, length = 255)
    private String experienceSlug;

    @Column(name = "location_id")
    private Long locationId;

    @Column(name = "location_name", length = 255)
    private String locationName;

    @Column(name = "time_slot_mapper_id", nullable = false)
    private Long timeSlotMapperId;

    @Column(name = "time_slot_id", nullable = false)
    private Long timeSlotId;

    @Column(name = "time_slot_label", length = 100)
    private String timeSlotLabel;

    @Column(name = "start_time", length = 20)
    private String startTime;

    @Column(name = "end_time", length = 20)
    private String endTime;

    @Column(name = "guest_count", nullable = false)
    private Integer guestCount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private BookingStatus status = BookingStatus.PENDING;

    @Column(name = "resolved_price_per_person", nullable = false, precision = 10, scale = 2)
    private BigDecimal resolvedPricePerPerson;

    @Enumerated(EnumType.STRING)
    @Column(name = "pricing_level", nullable = false, length = 20)
    private PricingLevel pricingLevel;

    @Column(name = "total_amount", nullable = false, precision = 10, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "addons_total", nullable = false, precision = 10, scale = 2)
    private BigDecimal addonsTotal = BigDecimal.ZERO;

    @Column(name = "grand_total", nullable = false, precision = 10, scale = 2)
    private BigDecimal grandTotal;

    @Column(name = "pincode", length = 20)
    private String pincode;

    @Column(name = "failure_reason", length = 500)
    private String failureReason;

    @Column(name = "requested_at")
    private LocalDateTime requestedAt;

    @Column(name = "confirmed_at")
    private LocalDateTime confirmedAt;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @OneToMany(mappedBy = "booking", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<BookingAddon> addons = new ArrayList<>();

    public void addAddon(BookingAddon addon) {
        addons.add(addon);
        addon.setBooking(this);
    }
}
