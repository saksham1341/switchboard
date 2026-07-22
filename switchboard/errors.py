class PermanentError(Exception):
    """Raised by a handler for a failure that cannot succeed on retry.
    The consumer loop maps it to Outcome.DEAD."""
