package com.forvmom.MomentForeverBooking.exception;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Represents a single field-level validation error.
 * Constructed via {@link FieldErrorBuilder}.
 *
 * <pre>
 * {
 *   "field":         "guestCount",
 *   "rejectedValue": 0,
 *   "message":       "must be greater than 0"
 * }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldError {

    private final String field;
    private final Object rejectedValue;
    private final String message;

    // Private constructor — only FieldErrorBuilder can create instances
    private FieldError(FieldErrorBuilder builder) {
        this.field = builder.field;
        this.rejectedValue = builder.rejectedValue;
        this.message = builder.message;
    }

    // -------------------------------------------------------------------------
    // Getters (immutable — no setters)
    // -------------------------------------------------------------------------

    public String getField() {
        return field;
    }

    public Object getRejectedValue() {
        return rejectedValue;
    }

    public String getMessage() {
        return message;
    }

    // -------------------------------------------------------------------------
    // Static factory to obtain a builder
    // -------------------------------------------------------------------------

    public static FieldErrorBuilder builder() {
        return new FieldErrorBuilder();
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static class FieldErrorBuilder {

        private String field;
        private Object rejectedValue;
        private String message;

        // Private constructor — use FieldError.builder()
        private FieldErrorBuilder() {
        }

        public FieldErrorBuilder field(String field) {
            this.field = field;
            return this;
        }

        public FieldErrorBuilder rejectedValue(Object rejectedValue) {
            this.rejectedValue = rejectedValue;
            return this;
        }

        public FieldErrorBuilder message(String message) {
            this.message = message;
            return this;
        }

        public FieldError build() {
            return new FieldError(this);
        }
    }
}
