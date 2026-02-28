package com.forvmom.MomentForeverBooking.repository;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface BookingRepository extends JpaRepository<Booking, String> {

    Optional<Booking> findByBookingId(String bookingId);

    boolean existsByBookingId(String bookingId);

    Page<Booking> findAllByUserId(Long userId, Pageable pageable);

    Page<Booking> findAllByStatus(BookingStatus status, Pageable pageable);
}
