"""Watch orders ("观察单") for reproducing a single trace id.

A duty operator opens an order on the home page for a trace id whose archived
trace cannot be found.  An order only observes the *first sampling window that
opens for that id after the order was created* and moves through a small state
machine:

    WAITING ──window opens──▶ COLLECTING ──window closes──▶ AWAITING_WRITE
       │                          │                             │
       │ 30s without a window     │ no ERROR span inside window  │ commit
       ▼                          ▼                             ▼
    TIMEOUT                    DROPPED                       SUCCESS

Every non-terminal order is marked ``INTERRUPTED`` when the process restarts,
because in-memory window state cannot survive a restart.  The manager never
influences ingest: the sampler calls these hooks after applying its normal
window and capacity rules, so ``/v1/traces`` behaves exactly as before.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from .storage import (
    WATCH_AWAITING_WRITE,
    WATCH_COLLECTING,
    WATCH_DROPPED,
    WATCH_INTERRUPTED,
    WATCH_SUCCESS,
    WATCH_TIMEOUT,
    WATCH_WAITING,
    ArchiveStore,
)

log = logging.getLogger("tailsampler.watch")

_HEX32 = re.compile(r"^[0-9a-fA-F]{32}$")
DEFAULT_WATCH_TIMEOUT_SECONDS = 30.0


class WatchError(ValueError):
    """A watch order request is invalid (HTTP 400)."""


class WatchConflict(RuntimeError):
    """Another order is still in progress (HTTP 409)."""


@dataclass
class WatchOrder:
    order_id: str
    trace_id: str
    state: str = WATCH_WAITING
    created_at_ns: int = 0
    updated_at_ns: int = 0
    window_opened_at_ns: Optional[int] = None
    closed_at_ns: Optional[int] = None
    timeout_seconds: float = DEFAULT_WATCH_TIMEOUT_SECONDS
    capacity_rejected: int = 0
    span_count: int = 0
    write_attempts: int = 0
    last_write_error: Optional[str] = None
    archived_trace_id: Optional[str] = None
    # Monotonic deadline used for the 30s wait timeout; wall-clock timestamps
    # are only stored for display.
    created_at_mono: float = 0.0
    # The active trace this order bound itself to.  Only the first window
    # opened after creation is ever tracked.
    bound_trace_id: Optional[str] = None
    # Pending trace whose archive carries this order to SUCCESS.
    bound_pending_id: Optional[str] = None
    dirty: bool = False

    @property
    def terminal(self) -> bool:
        return self.state in (
            WATCH_SUCCESS,
            WATCH_DROPPED,
            WATCH_TIMEOUT,
            WATCH_INTERRUPTED,
        )

    def snapshot(self) -> dict:
        return {
            "orderId": self.order_id,
            "traceId": self.trace_id,
            "state": self.state,
            "createdAtNs": self.created_at_ns,
            "updatedAtNs": self.updated_at_ns,
            "windowOpenedAtNs": self.window_opened_at_ns,
            "closedAtNs": self.closed_at_ns,
            "timeoutSeconds": self.timeout_seconds,
            "capacityRejected": self.capacity_rejected,
            "spanCount": self.span_count,
            "writeAttempts": self.write_attempts,
            "lastWriteError": self.last_write_error,
            "archivedTraceId": self.archived_trace_id,
        }


class WatchManager:
    def __init__(
        self,
        store: ArchiveStore,
        timeout_seconds: float = DEFAULT_WATCH_TIMEOUT_SECONDS,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("watch timeout must be positive")
        self.store = store
        self.timeout_seconds = float(timeout_seconds)
        self.clock = clock or time.monotonic
        self._lock = threading.RLock()
        # Only non-terminal orders live in memory, keyed by order id.  The
        # invariant "at most one order in progress" is enforced on creation.
        self._active: dict[str, WatchOrder] = {}

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def validate_trace_id(trace_id: str) -> str:
        text = (trace_id or "").strip().lower()
        if not _HEX32.match(text) or text == "0" * 32:
            raise WatchError("traceId must be 32 hex characters")
        return text

    def _persist_locked(self, order: WatchOrder, only_active: bool = True) -> None:
        fields = {
            "state": order.state,
            "updated_at_ns": order.updated_at_ns,
            "window_opened_at_ns": order.window_opened_at_ns,
            "closed_at_ns": order.closed_at_ns,
            "capacity_rejected": order.capacity_rejected,
            "span_count": order.span_count,
            "write_attempts": order.write_attempts,
            "last_write_error": order.last_write_error,
            "archived_trace_id": order.archived_trace_id,
        }
        self.store.update_watch_order(
            order.order_id, fields, only_active=only_active
        )
        order.dirty = False

    def _finish_locked(self, order: WatchOrder, state: str) -> None:
        if order.terminal:
            return
        order.state = state
        now_ns = time.time_ns()
        order.updated_at_ns = now_ns
        if order.closed_at_ns is None:
            order.closed_at_ns = now_ns
        self._persist_locked(order, only_active=False)
        self._active.pop(order.order_id, None)
        log.info("watch order %s finished as %s", order.order_id, state)

    # ------------------------------------------------------------- lifecycle
    def recover_interrupted(self) -> list[str]:
        """Mark unfinished orders INTERRUPTED after a restart.

        In-memory window bindings are gone, so any order that had not reached a
        terminal conclusion can no longer observe a window.
        """

        interrupted = self.store.mark_interrupted_watch_orders()
        with self._lock:
            self._active.clear()
        if interrupted:
            log.warning("marked %d watch order(s) interrupted", len(interrupted))
        return interrupted

    def create_watch(self, trace_id: str) -> WatchOrder:
        trace_id = self.validate_trace_id(trace_id)
        with self._lock:
            existing = self.store.get_active_watch_order()
            if existing is not None:
                raise WatchConflict(
                    f"order {existing['order_id']} is already in progress"
                )
            seq = self.store.next_watch_seq()
            now_ns = time.time_ns()
            order = WatchOrder(
                order_id=f"WO-{seq:06d}",
                trace_id=trace_id,
                state=WATCH_WAITING,
                created_at_ns=now_ns,
                updated_at_ns=now_ns,
                timeout_seconds=self.timeout_seconds,
                created_at_mono=self.clock(),
            )
            self.store.create_watch_order(
                {
                    "order_id": order.order_id,
                    "trace_id": order.trace_id,
                    "state": order.state,
                    "created_at_ns": order.created_at_ns,
                    "updated_at_ns": order.updated_at_ns,
                    "window_opened_at_ns": None,
                    "closed_at_ns": None,
                    "timeout_seconds": order.timeout_seconds,
                    "capacity_rejected": 0,
                    "span_count": 0,
                    "write_attempts": 0,
                    "last_write_error": None,
                    "archived_trace_id": None,
                }
            )
            self._active[order.order_id] = order
            log.info("opened watch order %s for trace %s", order.order_id, trace_id)
            return order

    # -------------------------------------------------- sampler event hooks
    def on_capacity_reject(self, trace_id: str) -> None:
        """A new window for ``trace_id`` was refused because slots were full."""

        with self._lock:
            for order in self._active.values():
                if (
                    order.trace_id == trace_id
                    and order.state == WATCH_WAITING
                ):
                    order.capacity_rejected += 1
                    order.updated_at_ns = time.time_ns()
                    order.dirty = True

    def on_trace_admitted(self, trace_id: str) -> None:
        """A brand-new sampling window opened for ``trace_id``."""

        with self._lock:
            for order in self._active.values():
                # Only the first window opened after creation is tracked.
                if (
                    order.trace_id == trace_id
                    and order.state == WATCH_WAITING
                    and order.bound_trace_id is None
                ):
                    order.state = WATCH_COLLECTING
                    order.bound_trace_id = trace_id
                    now_ns = time.time_ns()
                    order.window_opened_at_ns = now_ns
                    order.updated_at_ns = now_ns
                    order.span_count = 0
                    order.dirty = True
                    log.info(
                        "watch order %s entered a window for %s",
                        order.order_id,
                        trace_id,
                    )

    def on_span_accepted(self, trace_id: str, span_count: int) -> None:
        """An admitted window now holds ``span_count`` distinct spans."""

        with self._lock:
            for order in self._active.values():
                if (
                    order.bound_trace_id == trace_id
                    and order.state == WATCH_COLLECTING
                ):
                    order.span_count = span_count
                    order.updated_at_ns = time.time_ns()
                    order.dirty = True

    def on_trace_discarded(self, trace_id: str) -> None:
        """The bound window closed without an ERROR span."""

        with self._lock:
            for order in list(self._active.values()):
                if order.bound_trace_id == trace_id:
                    self._finish_locked(order, WATCH_DROPPED)

    def on_trace_pending(self, trace_id: str) -> None:
        """The bound window closed with an ERROR span and awaits archiving."""

        with self._lock:
            for order in self._active.values():
                if (
                    order.bound_trace_id == trace_id
                    and order.state == WATCH_COLLECTING
                ):
                    order.state = WATCH_AWAITING_WRITE
                    order.bound_pending_id = trace_id
                    order.updated_at_ns = time.time_ns()
                    order.dirty = True

    def on_write_failed(self, trace_id: str, error: str) -> None:
        """An archive attempt for a bound pending trace failed; it stays pending."""

        with self._lock:
            for order in self._active.values():
                if order.bound_pending_id == trace_id:
                    order.write_attempts += 1
                    order.last_write_error = error
                    order.updated_at_ns = time.time_ns()
                    order.dirty = True

    def order_for_pending(self, trace_id: str) -> Optional[WatchOrder]:
        """Return the order whose SUCCESS must commit with this archive."""

        with self._lock:
            for order in self._active.values():
                if (
                    order.bound_pending_id == trace_id
                    and order.state == WATCH_AWAITING_WRITE
                ):
                    return order
            return None

    def on_archive_committed(self, trace_id: str) -> None:
        """A plain (non-order) archive committed: drop the in-memory binding."""

        with self._lock:
            order = self._find_bound_locked(trace_id)
            if order is not None and not order.terminal:
                # Combined write would have finished the order; reaching here
                # means the archive landed without this order's conclusion.
                self._active.pop(order.order_id, None)

    def _find_bound_locked(self, trace_id: str) -> Optional[WatchOrder]:
        for order in self._active.values():
            if order.bound_trace_id == trace_id or order.bound_pending_id == trace_id:
                return order
        return None

    # ------------------------------------------------------------- scheduler
    def tick(self, active_span_counts: Optional[dict[str, int]] = None) -> None:
        """Apply the 30s wait timeout and flush dirty progress rows."""

        active_span_counts = active_span_counts or {}
        with self._lock:
            now = self.clock()
            for order in list(self._active.values()):
                if (
                    order.state == WATCH_WAITING
                    and now - order.created_at_mono >= order.timeout_seconds
                ):
                    self._finish_locked(order, WATCH_TIMEOUT)
                    continue
                if order.state == WATCH_COLLECTING:
                    count = active_span_counts.get(order.bound_trace_id or "")
                    if count is not None and count != order.span_count:
                        order.span_count = count
                        order.updated_at_ns = time.time_ns()
                        order.dirty = True
                if order.dirty and not order.terminal:
                    self._persist_locked(order)

    # ----------------------------------------------------------------- query
    @staticmethod
    def _row_to_snapshot(row: dict) -> dict:
        """Map a snake_case database row onto the camelCase API shape."""

        return {
            "orderId": row["order_id"],
            "traceId": row["trace_id"],
            "state": row["state"],
            "createdAtNs": row["created_at_ns"],
            "updatedAtNs": row["updated_at_ns"],
            "windowOpenedAtNs": row["window_opened_at_ns"],
            "closedAtNs": row["closed_at_ns"],
            "timeoutSeconds": row["timeout_seconds"],
            "capacityRejected": row["capacity_rejected"],
            "spanCount": row["span_count"],
            "writeAttempts": row["write_attempts"],
            "lastWriteError": row["last_write_error"],
            "archivedTraceId": row["archived_trace_id"],
        }

    def get(self, order_id: str) -> Optional[dict]:
        order_id = order_id.strip()
        with self._lock:
            order = self._active.get(order_id)
            if order is not None:
                return order.snapshot()
        row = self.store.get_watch_order(order_id)
        return self._row_to_snapshot(row) if row is not None else None

    def list(self, limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
        rows, total = self.store.list_watch_orders(limit=limit, offset=offset)
        return [self._row_to_snapshot(row) for row in rows], total

    def active_order(self) -> Optional[dict]:
        with self._lock:
            if self._active:
                # At most one active order is allowed by construction.
                order = next(iter(self._active.values()))
                return order.snapshot()
        row = self.store.get_active_watch_order()
        return self._row_to_snapshot(row) if row is not None else None

    def snapshot(self) -> dict:
        with self._lock:
            active = [order.snapshot() for order in self._active.values()]
        return {
            "timeoutSeconds": self.timeout_seconds,
            "activeCount": len(active),
            "active": active,
        }
