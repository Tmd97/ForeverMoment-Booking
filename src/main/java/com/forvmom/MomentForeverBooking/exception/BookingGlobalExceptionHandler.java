package com.forvmom.MomentForeverBooking.exception;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindingResult;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Centralized exception handler for the Booking microservice.
 * All unhandled exceptions propagate here and are mapped to a
 * consistent {@link ApiErrorResponse} built via
 * {@link ApiErrorResponse.ApiErrorResponseBuilder}.
 *
 * <p>
 * Response shape:
 * 
 * <pre>
 * {
 *   "timestamp": "2026-03-04 11:30:00",
 *   "status":    404,
 *   "error":     "NOT_FOUND",
 *   "code":      "BOOKING_NOT_FOUND",
 *   "message":   "Booking not found: BK-12345",
 *   "path":      "/booking/admin/BK-12345"
 * }
 * </pre>
 */
@RestControllerAdvice
public class BookingGlobalExceptionHandler {

        private static final Logger log = LoggerFactory.getLogger(BookingGlobalExceptionHandler.class);

        // =========================================================================
        // DOMAIN EXCEPTIONS
        // =========================================================================

        /**
         * Booking record not found → 404 NOT FOUND
         */
        @ExceptionHandler(BookingNotFoundException.class)
        public ResponseEntity<ApiErrorResponse> handleBookingNotFound(
                        BookingNotFoundException ex,
                        HttpServletRequest request) {

                log.warn("Booking not found — bookingId={}, path={}", ex.getBookingId(), request.getRequestURI());

                return buildResponse(
                                HttpStatus.NOT_FOUND,
                                BookingErrorCode.BOOKING_NOT_FOUND,
                                ex.getMessage(),
                                request.getRequestURI());
        }

        /**
         * Illegal status transition (e.g. cancelling a confirmed booking) → 409
         * CONFLICT
         */
        @ExceptionHandler(BookingStatusConflictException.class)
        public ResponseEntity<ApiErrorResponse> handleBookingStatusConflict(
                        BookingStatusConflictException ex,
                        HttpServletRequest request) {

                log.warn("Booking status conflict — bookingId={}, currentStatus={}, action={}, path={}",
                                ex.getBookingId(), ex.getCurrentStatus(), ex.getAttemptedAction(),
                                request.getRequestURI());

                return buildResponse(
                                HttpStatus.CONFLICT,
                                BookingErrorCode.BOOKING_INVALID_STATUS_TRANSITION,
                                ex.getMessage(),
                                request.getRequestURI());
        }

        /**
         * Semantically invalid booking request → 400 BAD REQUEST
         */
        @ExceptionHandler(InvalidBookingRequestException.class)
        public ResponseEntity<ApiErrorResponse> handleInvalidBookingRequest(
                        InvalidBookingRequestException ex,
                        HttpServletRequest request) {

                log.warn("Invalid booking request — path={}, reason={}", request.getRequestURI(), ex.getMessage());

                return buildResponse(
                                HttpStatus.BAD_REQUEST,
                                BookingErrorCode.INVALID_REQUEST,
                                ex.getMessage(),
                                request.getRequestURI());
        }

        // =========================================================================
        // VALIDATION EXCEPTIONS
        // =========================================================================

        /**
         * @Valid / @Validated constraint violations on request body fields → 400 BAD
         *        REQUEST.
         *        Returns a list of field-level {@link FieldError} entries.
         */
        @ExceptionHandler(MethodArgumentNotValidException.class)
        public ResponseEntity<ApiErrorResponse> handleValidationErrors(
                        MethodArgumentNotValidException ex,
                        HttpServletRequest request) {

                BindingResult bindingResult = ex.getBindingResult();

                List<FieldError> fieldErrors = bindingResult.getFieldErrors().stream()
                                .map(fe -> FieldError.builder()
                                                .field(fe.getField())
                                                .rejectedValue(fe.getRejectedValue())
                                                .message(fe.getDefaultMessage())
                                                .build())
                                .collect(Collectors.toList());

                log.warn("Validation failed — path={}, errors={}", request.getRequestURI(), fieldErrors.size());

                ApiErrorResponse body = ApiErrorResponse.builder()
                                .status(HttpStatus.BAD_REQUEST.value())
                                .error("BAD_REQUEST")
                                .code(BookingErrorCode.VALIDATION_FAILED)
                                .message("Request validation failed. Check 'fieldErrors' for details.")
                                .path(request.getRequestURI())
                                .fieldErrors(fieldErrors)
                                .build();

                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
        }

        /**
         * Missing required @RequestParam → 400 BAD REQUEST
         */
        @ExceptionHandler(MissingServletRequestParameterException.class)
        public ResponseEntity<ApiErrorResponse> handleMissingParam(
                        MissingServletRequestParameterException ex,
                        HttpServletRequest request) {

                String message = String.format("Required query parameter '%s' of type '%s' is missing.",
                                ex.getParameterName(), ex.getParameterType());
                log.warn("Missing request param — path={}, param={}", request.getRequestURI(), ex.getParameterName());

                return buildResponse(HttpStatus.BAD_REQUEST, BookingErrorCode.INVALID_REQUEST, message,
                                request.getRequestURI());
        }

        /**
         * Path/query variable type mismatch → 400 BAD REQUEST
         */
        @ExceptionHandler(MethodArgumentTypeMismatchException.class)
        public ResponseEntity<ApiErrorResponse> handleTypeMismatch(
                        MethodArgumentTypeMismatchException ex,
                        HttpServletRequest request) {

                String expectedType = ex.getRequiredType() != null ? ex.getRequiredType().getSimpleName() : "unknown";
                String message = String.format("Parameter '%s' must be of type '%s'. Received: '%s'.",
                                ex.getName(), expectedType, ex.getValue());
                log.warn("Type mismatch — path={}, param={}", request.getRequestURI(), ex.getName());

                return buildResponse(HttpStatus.BAD_REQUEST, BookingErrorCode.INVALID_REQUEST, message,
                                request.getRequestURI());
        }

        /**
         * Malformed JSON or unreadable request body → 400 BAD REQUEST
         */
        @ExceptionHandler(HttpMessageNotReadableException.class)
        public ResponseEntity<ApiErrorResponse> handleUnreadableBody(
                        HttpMessageNotReadableException ex,
                        HttpServletRequest request) {

                log.warn("Unreadable request body — path={}", request.getRequestURI());
                return buildResponse(
                                HttpStatus.BAD_REQUEST,
                                BookingErrorCode.INVALID_REQUEST,
                                "Request body is malformed or missing. Please provide valid JSON.",
                                request.getRequestURI());
        }

        // =========================================================================
        // HTTP METHOD / MEDIA TYPE EXCEPTIONS
        // =========================================================================

        /**
         * Wrong HTTP method → 405 METHOD NOT ALLOWED
         */
        @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
        public ResponseEntity<ApiErrorResponse> handleMethodNotSupported(
                        HttpRequestMethodNotSupportedException ex,
                        HttpServletRequest request) {

                String message = String.format("HTTP method '%s' is not supported for this endpoint. Supported: %s",
                                ex.getMethod(), ex.getSupportedHttpMethods());
                log.warn("Method not allowed — path={}, method={}", request.getRequestURI(), ex.getMethod());

                return buildResponse(HttpStatus.METHOD_NOT_ALLOWED, BookingErrorCode.INVALID_REQUEST, message,
                                request.getRequestURI());
        }

        /**
         * Unsupported Content-Type → 415 UNSUPPORTED MEDIA TYPE
         */
        @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
        public ResponseEntity<ApiErrorResponse> handleUnsupportedMediaType(
                        HttpMediaTypeNotSupportedException ex,
                        HttpServletRequest request) {

                String message = String.format("Content-Type '%s' is not supported. Use 'application/json'.",
                                ex.getContentType());
                log.warn("Unsupported media type — path={}, contentType={}", request.getRequestURI(),
                                ex.getContentType());

                return buildResponse(HttpStatus.UNSUPPORTED_MEDIA_TYPE, BookingErrorCode.INVALID_REQUEST, message,
                                request.getRequestURI());
        }

        /**
         * No handler found for the request URL → 404 NOT FOUND
         */
        @ExceptionHandler(NoHandlerFoundException.class)
        public ResponseEntity<ApiErrorResponse> handleNoHandlerFound(
                        NoHandlerFoundException ex,
                        HttpServletRequest request) {

                String message = String.format("No endpoint found for [%s] %s", ex.getHttpMethod(), ex.getRequestURL());
                log.warn("No handler found — path={}", request.getRequestURI());

                return buildResponse(HttpStatus.NOT_FOUND, BookingErrorCode.BOOKING_NOT_FOUND, message,
                                request.getRequestURI());
        }

        // =========================================================================
        // STANDARD JAVA EXCEPTIONS
        // =========================================================================

        /**
         * Illegal state or argument violations from the service layer → 400 BAD REQUEST
         */
        @ExceptionHandler({ IllegalStateException.class, IllegalArgumentException.class })
        public ResponseEntity<ApiErrorResponse> handleIllegalState(
                        RuntimeException ex,
                        HttpServletRequest request) {

                log.warn("Illegal state/argument — path={}, message={}", request.getRequestURI(), ex.getMessage());
                return buildResponse(HttpStatus.BAD_REQUEST, BookingErrorCode.INVALID_REQUEST, ex.getMessage(),
                                request.getRequestURI());
        }

        // =========================================================================
        // FALLBACK — CATCH-ALL
        // =========================================================================

        /**
         * Catch-all for any unhandled exception → 500 INTERNAL SERVER ERROR.
         * Logs the full stack trace but returns a safe generic message to the client.
         */
        @ExceptionHandler(Exception.class)
        public ResponseEntity<ApiErrorResponse> handleGenericException(
                        Exception ex,
                        HttpServletRequest request) {

                log.error("Unhandled exception — path={}, error={}", request.getRequestURI(), ex.getMessage(), ex);

                return buildResponse(
                                HttpStatus.INTERNAL_SERVER_ERROR,
                                BookingErrorCode.INTERNAL_SERVER_ERROR,
                                "An unexpected error occurred. Please try again later or contact support.",
                                request.getRequestURI());
        }

        // =========================================================================
        // PRIVATE HELPERS
        // =========================================================================

        /**
         * Builds a {@link ResponseEntity} wrapping an {@link ApiErrorResponse} via its
         * builder.
         */
        private ResponseEntity<ApiErrorResponse> buildResponse(
                        HttpStatus status,
                        BookingErrorCode code,
                        String message,
                        String path) {

                ApiErrorResponse body = ApiErrorResponse.builder()
                                .status(status.value())
                                .error(status.getReasonPhrase().toUpperCase().replace(" ", "_"))
                                .code(code)
                                .message(message)
                                .path(path)
                                .build();

                return ResponseEntity.status(status).body(body);
        }
}
