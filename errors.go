package storage

// errors.go centralizes the error vocabulary used across storage
// backends. Re-exporting xerr constructors here keeps adapter authors
// from having to import kernel/xerr directly for the common case;
// they can still reach for xerr directly when they need a category
// this file does not wrap.
//
// Every error that escapes a Backend or Instance method MUST be a
// *xerr.AppError. Bare errors.New / fmt.Errorf are forbidden across
// package boundaries per flow/AGENTS.md §5.

import (
	"fmt"

	"github.com/nexssp/kernel/xerr"
)

// BadRequest wraps xerr.BadRequest. Use for invalid user-supplied
// options (unknown key, malformed value, missing required field).
// msg is treated as a final message; pre-format with fmt.Sprintf
// if interpolation is needed.
func BadRequest(msg string) error {
	return xerr.BadRequest(msg)
}

// BadRequestf is the printf-style variant.
func BadRequestf(format string, args ...any) error {
	return xerr.BadRequest(fmt.Sprintf(format, args...))
}

// NotFound wraps xerr.NotFound. Use for missing declarations,
// missing backends, missing keys (when "missing" is not the
// expected hit path).
func NotFound(msg string) error {
	return xerr.NotFound(msg)
}

// NotFoundf is the printf-style variant.
func NotFoundf(format string, args ...any) error {
	return xerr.NotFound(fmt.Sprintf(format, args...))
}

// Conflict wraps xerr.Conflict. Use for duplicate registrations,
// fence-token mismatches, idempotency-key payload conflicts.
func Conflict(msg string) error {
	return xerr.Conflict(msg)
}

// Conflictf is the printf-style variant.
func Conflictf(format string, args ...any) error {
	return xerr.Conflict(fmt.Sprintf(format, args...))
}

// Unavailable wraps xerr.Unavailable. Use for infrastructure
// failures: connection refused, timeout, network partition, disk
// full. Always wrap the underlying cause.
func Unavailable(msg string, cause error) error {
	return xerr.Unavailable(msg, cause)
}

// Unavailablef is the printf-style variant.
func Unavailablef(cause error, format string, args ...any) error {
	return xerr.Unavailable(fmt.Sprintf(format, args...), cause)
}

// Internal wraps xerr.Internal. Use for programmer bugs that are not
// the user's fault: a Backend that does not implement its declared
// Kind's interface, a corrupted in-memory cache entry.
func Internal(msg string) error {
	return xerr.Internal(msg)
}

// Internalf is the printf-style variant.
func Internalf(format string, args ...any) error {
	return xerr.Internal(fmt.Sprintf(format, args...))
}

// TooManyRequests wraps xerr.TooManyRequests. Use from RateLimiter
// backends when the underlying store returns its own rate-limit
// signal distinct from the token-bucket decision.
func TooManyRequests(msg string) error {
	return xerr.TooManyRequests(msg)
}
