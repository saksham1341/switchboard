class PermanentError(Exception):
    """Raised by a handler for a failure that cannot succeed on retry.
    The consumer loop maps it to Outcome.DEAD."""


class RetryableError(Exception):
    """Raised by a handler for a failure that *should* be retried, optionally
    carrying the delay the source itself told us to wait.

    The symmetric partner of PermanentError: PermanentError says "never retry",
    RetryableError says "retry, and here is when". If `retry_after` is None the
    Bus falls back to its own exponential backoff — so any plain exception and a
    RetryableError() with no delay behave identically. The point of the class is
    the *override*: a provider that returns a 429 with `retry-after: 2` knows the
    real delay far better than a guessed curve, and blind backoff can fire
    retries so fast they re-exhaust the very budget they are waiting on.
    """

    def __init__(self, message: str = "", retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after
