package com.forvmom.MomentForeverBooking.controller;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.service.BookingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/bookings")
@Tag(name = "Booking Queries", description = "Endpoints for fetching booking history and details")
public class BookingController {

    private final BookingService bookingService;

    public BookingController(BookingService bookingService) {
        this.bookingService = bookingService;
    }

    @GetMapping("/{bookingId}")
    @Operation(summary = "Get Booking by ID")
    public ResponseEntity<Booking> getBookingById(@PathVariable String bookingId) {
        return ResponseEntity.ok(bookingService.getBookingByBookingId(bookingId));
    }

    @GetMapping("/user/{userId}")
    @Operation(summary = "Get Bookings for User")
    public ResponseEntity<Page<Booking>> getBookingsByUser(
            @PathVariable Long userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {

        // Sorting by newest first
        PageRequest pageRequest = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt"));
        return ResponseEntity.ok(bookingService.getBookingsByUser(userId, pageRequest));
    }

    @PutMapping("/{bookingId}/cancel")
    @Operation(summary = "Cancel Booking")
    public ResponseEntity<?> cancelBooking(@PathVariable String bookingId) {
        bookingService.cancelBooking(bookingId);
        return ResponseEntity.ok(Map.of(
                "bookingId", bookingId,
                "status", "CANCELLED",
                "message", "Booking has been cancelled successfully."));
    }
}
