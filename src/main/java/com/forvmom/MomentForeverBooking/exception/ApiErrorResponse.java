package com.forvmom.MomentForeverBooking.exception;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Unified error response body returned by {@link BookingGlobalExceptionHandler}
 * for every error that occurs in the Booking microservice.
 *
 * <p>
 * Constructed via {@link ApiErrorResponseBuilder}:
 * 
 * <pre>
 * ApiErrorResponse response = ApiErrorResponse.builder()
 *         .status(404)
 *         .error("NOT_FOUND")
 *         .code(BookingErrorCode.BOOKING_NOT_FOUND)
 *         .message("Booking not found: BK-12345")
 *         .path("/booking/admin/BK-12345")
 *         .build();
 * </pre>
 *
 * <p>
 * JSON shape:
 * 
 * <pre>
 * {
 *   "timestamp":   "2026-03-04 11:30:00",
 *   "status":      404,
 *   "error":       "NOT_FOUND",
 *   "code":        "BOOKING_NOT_FOUND",
 *   "message":     "Booking not found: BK-12345",
 *   "path":        "/booking/admin/BK-12345",
 *   "fieldErrors": [ ... ]    // present only for validation failures
 * }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiErrorResponse {

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private final LocalDateTime timestamp;

    /** HTTP status code, e.g. 404 */
    private final int status;

    /** HTTP reason phrase in UPPER_SNAKE, e.g. "NOT_FOUND" */
    private final String error;

    /** Application-level error code, e.g. "BOOKING_NOT_FOUND" */
    private final String code;

    /** Human-readable error message safe to show clients */
    private final String message;

    /** Request URI that triggered the error */
    private final String path;

    /** Field-level validation errors (only for 400 validation failures) */
    private final List<FieldError> fieldErrors;

    // Private constructor — only ApiErrorResponseBuilder can create instances
    private ApiErrorResponse(ApiErrorResponseBuilder builder) {
        this.timestamp = LocalDateTime.now();
        this.status = builder.status;
        this.error = builder.error;
        this.code = builder.code;
        this.message = builder.message;
        this.path = builder.path;
        this.fieldErrors = builder.fieldErrors;
    }

    // -------------------------------------------------------------------------
    // Getters (immutable — no setters)
    // -------------------------------------------------------------------------

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public int getStatus() {
        return status;
    }

    public String getError() {
        return error;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public String getPath() {
        return path;
    }

    public List<FieldError> getFieldErrors() {
        return fieldErrors;
    }

    // -------------------------------------------------------------------------
    // Static factory to obtain a builder
    // -------------------------------------------------------------------------

    public static ApiErrorResponseBuilder builder() {
        return new ApiErrorResponseBuilder();
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static class ApiErrorResponseBuilder {

        private int status;
        private String error;
        private String code;
        private String message;
        private String path;
        private List<FieldError> fieldErrors;

        // Private constructor — use ApiErrorResponse.builder()
        private ApiErrorResponseBuilder() {
        }

        public ApiErrorResponseBuilder status(int status) {
            this.status = status;
            return this;
        }

        public ApiErrorResponseBuilder error(String error) {
            this.error = error;
            return this;
        }

        /** Accepts a {@link BookingErrorCode} enum and converts to its name string. */
        public ApiErrorResponseBuilder code(BookingErrorCode code) {
            this.code = code.name();
            return this;
        }

        public ApiErrorResponseBuilder message(String message) {
            this.message = message;
            return this;
        }

        public ApiErrorResponseBuilder path(String path) {
            this.path = path;
            return this;
        }

        public ApiErrorResponseBuilder fieldErrors(List<FieldError> fieldErrors) {
            this.fieldErrors = fieldErrors;
            return this;
        }

        public ApiErrorResponse build() {
            return new ApiErrorResponse(this);
        }
    }
}
