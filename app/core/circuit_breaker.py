"""
Lightweight in-process circuit breaker for external dependencies.

Usage
-----
    cb = CircuitBreaker(name="groq-llm", failure_threshold=5, recovery_timeout=30)

    async with cb:
        result = await some_async_call()

    # Or for synchronous calls:
    with cb:
        result = some_sync_call()

When the circuit is OPEN, a CircuitBreakerOpen exception is raised
immediately — no attempt is made to call the dependency.  After
*recovery_timeout* seconds the circuit transitions to HALF_OPEN and
lets one request through to probe for recovery.
"""
import enum
import logging
import time
from functools import wraps

logger = logging.getLogger("neuro_platform.circuit_breaker")


class CircuitState(enum.Enum):
    CLOSED = "closed"       # Normal — requests pass through
    OPEN = "open"           # Failing — requests fast-fail
    HALF_OPEN = "half_open"  # Probing — one request allowed through


class CircuitBreakerOpenError(Exception):
    """Raised when a call is rejected because the circuit is OPEN."""

    def __init__(self, name: str, retry_after_seconds: float) -> None:
        self.name = name
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"Circuit breaker '{name}' is OPEN — "
            f"retry in {retry_after_seconds:.0f}s"
        )


class CircuitBreaker:
    """In-process circuit breaker with half-open recovery."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        expected_exceptions: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        self.name = name
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._expected_exceptions = expected_exceptions

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._half_open_lock = False  # prevent concurrent HALF_OPEN probes

    # ── Public properties ──────────────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN and self._is_recovery_due():
            logger.info(
                "Circuit '%s' transitioning OPEN → HALF_OPEN (recovery timeout elapsed)",
                self.name,
            )
            self._state = CircuitState.HALF_OPEN
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    # ── Context manager (async) ────────────────────────────────────────

    async def __aenter__(self) -> "CircuitBreaker":
        self._check()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool:
        if exc_type is None:
            self._on_success()
        elif exc_type is CircuitBreakerOpenError:
            return False  # don't swallow
        elif issubclass(exc_type, self._expected_exceptions):
            self._on_failure()
        return False  # don't swallow — let caller handle

    # ── Context manager (sync) ─────────────────────────────────────────

    def __enter__(self) -> "CircuitBreaker":
        self._check()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool:
        if exc_type is None:
            self._on_success()
        elif exc_type is CircuitBreakerOpenError:
            return False
        elif issubclass(exc_type, self._expected_exceptions):
            self._on_failure()
        return False

    # ── Decorator (sync) ───────────────────────────────────────────────

    def __call__(self, func):
        """Use as @circuit_breaker decorator on sync functions."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper

    # ── Async decorator ────────────────────────────────────────────────

    def async_call(self, func):
        """Use as @circuit_breaker.async_call on async functions."""
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with self:
                return await func(*args, **kwargs)
        return wrapper

    # ── Internal helpers ───────────────────────────────────────────────

    def _check(self) -> None:
        st = self.state  # triggers OPEN → HALF_OPEN if recovery due
        if st == CircuitState.OPEN:
            raise CircuitBreakerOpenError(self.name, self._recovery_timeout)
        if st == CircuitState.HALF_OPEN:
            if self._half_open_lock:
                raise CircuitBreakerOpenError(self.name, self._recovery_timeout)
            self._half_open_lock = True  # acquire probe lock

    def _on_success(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            logger.info(
                "Circuit '%s' recovered — HALF_OPEN → CLOSED",
                self.name,
            )
            self._state = CircuitState.CLOSED
            self._half_open_lock = False
        self._failure_count = 0

    def _on_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._state == CircuitState.HALF_OPEN:
            logger.warning(
                "Circuit '%s' probe failed — HALF_OPEN → OPEN",
                self.name,
            )
            self._state = CircuitState.OPEN
            self._half_open_lock = False
        elif self._failure_count >= self._failure_threshold:
            logger.warning(
                "Circuit '%s' OPENED (%d failures)",
                self.name,
                self._failure_count,
            )
            self._state = CircuitState.OPEN

    def _is_recovery_due(self) -> bool:
        if self._last_failure_time is None:
            return True
        elapsed = time.monotonic() - self._last_failure_time
        return elapsed >= self._recovery_timeout
