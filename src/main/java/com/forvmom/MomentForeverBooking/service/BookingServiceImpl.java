package com.forvmom.MomentForeverBooking.service;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.events.BookingConfirmedEvent;
import com.forvmom.MomentForeverBooking.events.BookingFailedEvent;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.events.PaymentRequestedEvent;
import com.forvmom.MomentForeverBooking.exception.BookingNotFoundException;
import com.forvmom.MomentForeverBooking.exception.BookingStatusConflictException;
import com.forvmom.MomentForeverBooking.mapper.BookingMapper;
import com.forvmom.MomentForeverBooking.producer.BookingEventProducer;
import com.forvmom.MomentForeverBooking.repository.BookingRepository;
import com.forvmom.MomentForeverBooking.service.BookingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class BookingServiceImpl implements BookingService {

    private static final Logger log = LoggerFactory.getLogger(BookingServiceImpl.class);

    private final BookingRepository bookingRepository;
    private final BookingEventProducer eventProducer;
    private final BookingMapper bookingMapper;

    @Autowired
    private OutgoingOutboxService outgoingOutboxService;

    public BookingServiceImpl(BookingRepository bookingRepository,
                              BookingEventProducer eventProducer,
                              BookingMapper bookingMapper) {
        this.bookingRepository = bookingRepository;
        this.eventProducer = eventProducer;
        this.bookingMapper = bookingMapper;
    }

    @Override
    @Transactional
    public void processBookingRequest(BookingRequestEvent event) {
        if (bookingRepository.existsByBookingId(event.getBookingId())) {
            log.warn("Duplicate booking request ignored. bookingId={}", event.getBookingId());
            return;
        }
        Booking booking = bookingMapper.toEntity(event);
        bookingRepository.save(booking);
        log.info("Persisted PENDING booking: bookingId={}", booking.getBookingId());

        // Trigger payment request
        PaymentRequestedEvent paymentRequestedEvent = buildPaymentRequestedEvent(booking);
        // save to outbox for async processing
        outgoingOutboxService.createOutgoingOutBox(paymentRequestedEvent);
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
        if (booking.getStatus() == BookingStatus.CONFIRMED) return;

        if (booking.getStatus() == BookingStatus.CANCELLED ||
                booking.getStatus() == BookingStatus.FAILED) {
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

        eventProducer.sendBookingConfirmedEvent(event);
        log.info("Booking confirmed: {}", bookingId);
    }

    @Override
    @Transactional
    public void failBooking(String bookingId, String reason) {
        Booking booking = getBookingByBookingId(bookingId);
        if (booking.getStatus() == BookingStatus.FAILED) return;

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

        eventProducer.sendBookingFailedEvent(event);
        log.warn("Booking failed: {} (reason: {})", bookingId, reason);
    }

    @Override
    @Transactional
    public void cancelBooking(String bookingId) {
        Booking booking = getBookingByBookingId(bookingId);
        if (booking.getStatus() == BookingStatus.CANCELLED) return;
        if (booking.getStatus() == BookingStatus.CONFIRMED) {
            throw new BookingStatusConflictException(bookingId, booking.getStatus().name(), "cancel");
        }

        booking.setStatus(BookingStatus.CANCELLED);
        bookingRepository.save(booking);
        log.info("Booking cancelled: {}", bookingId);
        // Optionally publish BookingCancelledEvent if needed
    }

    @Transactional
    public PaymentRequestedEvent buildPaymentRequestedEvent(Booking booking) {
        PaymentRequestedEvent paymentRequestedEvent = new PaymentRequestedEvent();
        paymentRequestedEvent.setBookingId(booking.getBookingId());
        paymentRequestedEvent.setRequestedAt(LocalDateTime.now());
        paymentRequestedEvent.setGrandTotal(booking.getGrandTotal());
        paymentRequestedEvent.setUserId(booking.getUserId());
        paymentRequestedEvent.setUserEmail(booking.getUserEmail());
        paymentRequestedEvent.setExperienceId(booking.getExperienceId());
        paymentRequestedEvent.setExperienceName(booking.getExperienceName());
        paymentRequestedEvent.setTimeSlotMapperId(booking.getTimeSlotMapperId());
        paymentRequestedEvent.setGuestCount(booking.getGuestCount());
        return paymentRequestedEvent;
    }


}