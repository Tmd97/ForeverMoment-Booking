package com.forvmom.MomentForeverBooking.service.impl;

import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.BookingAddon;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.domain.enums.PricingLevel;
import com.forvmom.MomentForeverBooking.exception.BookingNotFoundException;
import com.forvmom.MomentForeverBooking.exception.BookingStatusConflictException;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import com.forvmom.MomentForeverBooking.service.BookingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class BookingServiceImpl implements BookingService {

    private static final Logger logger = LoggerFactory.getLogger(BookingServiceImpl.class);

    private final BookingRepository bookingRepository;
    private final BookingEventProducer bookingEventProducer;

    public BookingServiceImpl(BookingRepository bookingRepository, BookingEventProducer bookingEventProducer) {
        this.bookingRepository = bookingRepository;
        this.bookingEventProducer = bookingEventProducer;
    }

    @Override
    @Transactional
    public void processBookingRequest(BookingRequestEvent event) {
        // Idempotency DB check
        if (bookingRepository.existsByBookingId(event.getBookingId())) {
            logger.warn("Booking already exists. Ignored duplicate event. bookingId={}", event.getBookingId());
            return;
        }

        // 1. Map event to Booking entity

        Booking booking = new Booking();
        booking.setBookingId(event.getBookingId());
        booking.setUserId(event.getUserId());
        booking.setUserEmail(event.getUserEmail());
        booking.setUserFullName(event.getUserFullName());
        booking.setExperienceId(event.getExperienceId());
        booking.setExperienceName(event.getExperienceName());
        // booking.setExperienceSlug(event.getExperienceSlug());
        booking.setLocationId(event.getLocationId());
        booking.setLocationName(event.getLocationName());
        booking.setTimeSlotMapperId(event.getTimeSlotMapperId());
        booking.setTimeSlotId(event.getTimeSlotId());
        booking.setTimeSlotLabel(event.getTimeSlotLabel());
        booking.setStartTime(event.getStartTime());
        booking.setEndTime(event.getEndTime());
        booking.setGuestCount(event.getGuestCount());
        booking.setStatus(BookingStatus.PENDING);
        booking.setResolvedPricePerPerson(event.getResolvedPricePerPerson());
        booking.setPricingLevel(PricingLevel.valueOf(event.getPricingLevel()));
        booking.setTotalAmount(event.getTotalAmount());
        booking.setAddonsTotal(event.getAddonsTotal());
        booking.setGrandTotal(event.getGrandTotal());
        booking.setPincode(event.getPincode());
        booking.setRequestedAt(event.getRequestedAt() != null ? event.getRequestedAt() : LocalDateTime.now());

        // Map Add-ons
        if (event.getAddons() != null) {
            for (var addonSnap : event.getAddons()) {
                BookingAddon addon = new BookingAddon();
                addon.setAddonMapperId(addonSnap.getAddonMapperId());
                addon.setAddonName(addonSnap.getAddonName());
                addon.setEffectivePrice(addonSnap.getEffectivePrice());
                addon.setFree(addonSnap.isFree());
                booking.addAddon(addon);
            }
        }

        // 2. Persist booking locally
        bookingRepository.save(booking);
        logger.info("Persisted PENDING booking: bookingId={}", booking.getBookingId());

        // 3. Publish payment requested event
        PaymentRequestedEvent paymentEvent = new PaymentRequestedEvent();
        paymentEvent.setBookingId(booking.getBookingId());
        paymentEvent.setUserId(booking.getUserId());
        paymentEvent.setUserEmail(booking.getUserEmail());
        paymentEvent.setGrandTotal(booking.getGrandTotal());
        paymentEvent.setCurrency("INR");
        paymentEvent.setRequestedAt(LocalDateTime.now());

        logger.info("Publishing payment-requested for bookingId={}, amount={}, currency={} ",
                paymentEvent.getBookingId(), paymentEvent.getGrandTotal(), paymentEvent.getCurrency());


        bookingEventProducer.sendPaymentRequested(paymentEvent);
    }

    @Override
    public Booking getBookingByBookingId(String bookingId) {
        return bookingRepository.findByBookingId(bookingId)
                .orElseThrow(() -> new BookingNotFoundException("Booking not found: " + bookingId));
    }

    @Override
    public Page<Booking> getBookingsByUser(Long userId, Pageable pageable) {
        return bookingRepository.findAllByUserId(userId, pageable);
    }

    @Override
    @Transactional
    public void confirmBooking(String bookingId) {
        Booking booking = getBookingByBookingId(bookingId);
        if (booking.getStatus() == BookingStatus.CONFIRMED)
            return;

        if (booking.getStatus() == BookingStatus.CANCELLED) {
            throw new BookingStatusConflictException(bookingId, booking.getStatus().name(), "confirm");
        }
        if (booking.getStatus() == BookingStatus.FAILED) {
            throw new BookingStatusConflictException(bookingId, booking.getStatus().name(), "confirm");
        }

        booking.setStatus(BookingStatus.CONFIRMED);
        booking.setConfirmedAt(LocalDateTime.now());
        bookingRepository.save(booking);

        BookingConfirmedEvent event = new BookingConfirmedEvent();
        event.setBookingId(booking.getBookingId());
        event.setUserId(booking.getUserId());
        event.setUserEmail(booking.getUserEmail());
        event.setExperienceId(booking.getExperienceId());
        event.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        event.setGuestCount(booking.getGuestCount());
        event.setConfirmedAt(booking.getConfirmedAt());

        bookingEventProducer.sendBookingConfirmed(event);
        logger.info("Booking confirmed: {}", bookingId);
    }

    @Override
    @Transactional
    public void failBooking(String bookingId, String reason) {
        Booking booking = getBookingByBookingId(bookingId);
        if (booking.getStatus() == BookingStatus.FAILED)
            return;

        booking.setStatus(BookingStatus.FAILED);
        booking.setFailureReason(reason);
        bookingRepository.save(booking);

        BookingFailedEvent event = new BookingFailedEvent();
        event.setBookingId(booking.getBookingId());
        event.setUserId(booking.getUserId());
        event.setUserEmail(booking.getUserEmail());
        event.setExperienceId(booking.getExperienceId());
        event.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        event.setGuestCount(booking.getGuestCount());
        event.setFailureReason(reason);
        event.setFailedAt(LocalDateTime.now());

        bookingEventProducer.sendBookingFailed(event);
        logger.warn("Booking failed: {} (reason: {})", bookingId, reason);
    }

    @Override
    @Transactional
    public void cancelBooking(String bookingId) {
        Booking booking = getBookingByBookingId(bookingId);
        if (booking.getStatus() == BookingStatus.CANCELLED)
            return; // idempotent — already cancelled, ignore gracefully
        if (booking.getStatus() == BookingStatus.CONFIRMED) {
            throw new BookingStatusConflictException(bookingId, booking.getStatus().name(), "cancel");
        }

        // Cancellation policies could be evaluated here before updating
        booking.setStatus(BookingStatus.CANCELLED);
        bookingRepository.save(booking);
        logger.info("Booking cancelled: {}", bookingId);

        // Optionally publish a BookingCancelledEvent if Core needs to release inventory
        // on cancel
    }
}
