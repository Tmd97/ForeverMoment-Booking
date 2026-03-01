package com.forvmom.MomentForeverBooking.domain.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * Add-on line items for a Booking.
 */
@Entity
@Table(name = "booking_addons")
public class BookingAddon {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "booking_id", nullable = false)
    private Booking booking;

    @Column(name = "addon_mapper_id", nullable = false)
    private Long addonMapperId;

    @Column(name = "addon_name", nullable = false, length = 255)
    private String addonName;

    @Column(name = "effective_price", nullable = false, precision = 10, scale = 2)
    private BigDecimal effectivePrice;

    @Column(name = "is_free", nullable = false)
    private boolean free;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Booking getBooking() {
        return booking;
    }

    public void setBooking(Booking booking) {
        this.booking = booking;
    }

    public Long getAddonMapperId() {
        return addonMapperId;
    }

    public void setAddonMapperId(Long addonMapperId) {
        this.addonMapperId = addonMapperId;
    }

    public String getAddonName() {
        return addonName;
    }

    public void setAddonName(String addonName) {
        this.addonName = addonName;
    }

    public BigDecimal getEffectivePrice() {
        return effectivePrice;
    }

    public void setEffectivePrice(BigDecimal effectivePrice) {
        this.effectivePrice = effectivePrice;
    }

    public boolean isFree() {
        return free;
    }

    public void setFree(boolean free) {
        this.free = free;
    }
}
