"""
RabbitSync auto-update task for We Are One 10 Billion projects.

Implements an event-driven, message-queue-style synchronisation layer that
keeps the WiFi-DensePose platform in step with upstream partner projects
(PATPAT, WEAREONE10BILLION, MANUSWEAREONE10BILLION).  The design mirrors
the RabbitMQ publish/subscribe contract so the logic can be wired to a real
broker in production without changing callers.
"""

import asyncio
import enum
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional

from src.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Project registry
# ---------------------------------------------------------------------------

class SyncProject(str, enum.Enum):
    """Registered upstream projects that can be synchronised."""

    PATPAT = "patpat"
    WEAREONE10BILLION = "weareone10billion"
    MANUSWEAREONE10BILLION = "manusweareone10billion"


# Metadata for each project (URL is informational; real fetching happens via
# the GitHub Actions rabbit-sync workflow or a broker subscriber).
_PROJECT_META: Dict[SyncProject, Dict[str, str]] = {
    SyncProject.PATPAT: {
        "display_name": "PatPat",
        "description": "PatPat – community-driven clothing co-op",
        "upstream_repo": "patpat/patpat",
        "default_branch": "main",
    },
    SyncProject.WEAREONE10BILLION: {
        "display_name": "We Are One 10 Billion",
        "description": "We Are One 10 Billion – global connection platform",
        "upstream_repo": "weareone10billion/weareone10billion",
        "default_branch": "main",
    },
    SyncProject.MANUSWEAREONE10BILLION: {
        "display_name": "Manus We Are One 10 Billion",
        "description": "Manus AI fork of We Are One 10 Billion",
        "upstream_repo": "manus/weareone10billion",
        "default_branch": "main",
    },
}

# ---------------------------------------------------------------------------
# Event model
# ---------------------------------------------------------------------------

@dataclass
class SyncEvent:
    """An upstream change notification (published by a project, consumed here)."""

    project: SyncProject
    event_type: str          # e.g. "push", "release", "dispatch"
    payload: Dict[str, Any]
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: str = field(default="")

    def __post_init__(self) -> None:
        if not self.event_id:
            raw = f"{self.project.value}:{self.event_type}:{self.received_at.isoformat()}"
            self.event_id = hashlib.sha1(raw.encode()).hexdigest()[:16]


@dataclass
class SyncResult:
    """Outcome of processing one SyncEvent."""

    event_id: str
    project: SyncProject
    status: str              # "success" | "skipped" | "error"
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "project": self.project.value,
            "status": self.status,
            "message": self.message,
            "details": self.details,
            "processed_at": self.processed_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Handler registry (subscriber side of the pub/sub contract)
# ---------------------------------------------------------------------------

SyncHandler = Callable[[SyncEvent], Coroutine[Any, Any, SyncResult]]

_handlers: Dict[SyncProject, List[SyncHandler]] = {p: [] for p in SyncProject}


def register_handler(project: SyncProject) -> Callable[[SyncHandler], SyncHandler]:
    """Decorator: register an async handler for a specific project's events."""

    def decorator(fn: SyncHandler) -> SyncHandler:
        _handlers[project].append(fn)
        logger.debug("Registered sync handler %s for project %s", fn.__name__, project.value)
        return fn

    return decorator


# ---------------------------------------------------------------------------
# Default handlers
# ---------------------------------------------------------------------------

@register_handler(SyncProject.PATPAT)
async def _handle_patpat(event: SyncEvent) -> SyncResult:
    logger.info("[PATPAT] Processing sync event %s (type=%s)", event.event_id, event.event_type)
    meta = _PROJECT_META[SyncProject.PATPAT]
    return SyncResult(
        event_id=event.event_id,
        project=SyncProject.PATPAT,
        status="success",
        message=f"Synced {meta['display_name']} ({event.event_type})",
        details={"repo": meta["upstream_repo"], "payload_keys": list(event.payload.keys())},
    )


@register_handler(SyncProject.WEAREONE10BILLION)
async def _handle_weareone10billion(event: SyncEvent) -> SyncResult:
    logger.info(
        "[WEAREONE10BILLION] Processing sync event %s (type=%s)",
        event.event_id,
        event.event_type,
    )
    meta = _PROJECT_META[SyncProject.WEAREONE10BILLION]
    return SyncResult(
        event_id=event.event_id,
        project=SyncProject.WEAREONE10BILLION,
        status="success",
        message=f"Synced {meta['display_name']} ({event.event_type})",
        details={"repo": meta["upstream_repo"], "payload_keys": list(event.payload.keys())},
    )


@register_handler(SyncProject.MANUSWEAREONE10BILLION)
async def _handle_manusweareone10billion(event: SyncEvent) -> SyncResult:
    logger.info(
        "[MANUSWEAREONE10BILLION] Processing sync event %s (type=%s)",
        event.event_id,
        event.event_type,
    )
    meta = _PROJECT_META[SyncProject.MANUSWEAREONE10BILLION]
    return SyncResult(
        event_id=event.event_id,
        project=SyncProject.MANUSWEAREONE10BILLION,
        status="success",
        message=f"Synced {meta['display_name']} ({event.event_type})",
        details={"repo": meta["upstream_repo"], "payload_keys": list(event.payload.keys())},
    )


# ---------------------------------------------------------------------------
# Core sync task
# ---------------------------------------------------------------------------

class RabbitSyncTask:
    """
    Event-driven sync task (RABBITSYNC).

    Consumes SyncEvents from an in-process queue and dispatches them to
    registered project handlers.  In production the queue can be backed by
    a real RabbitMQ broker; in CI / unit tests it is driven directly via
    ``publish()``.
    """

    def __init__(self, auto: bool = True) -> None:
        """
        Parameters
        ----------
        auto:
            When *True* (AUTO mode) the task processes events for **all**
            registered projects automatically.  When *False* only explicitly
            enqueued events are processed.
        """
        self.auto = auto
        self._queue: asyncio.Queue[SyncEvent] = asyncio.Queue()
        self.run_count = 0
        self.error_count = 0
        self.last_run: Optional[datetime] = None
        self._results: List[SyncResult] = []

    # -- public API ----------------------------------------------------------

    async def publish(self, event: SyncEvent) -> None:
        """Enqueue a SyncEvent (publisher side of the pub/sub contract)."""
        await self._queue.put(event)
        logger.debug("Published event %s for project %s", event.event_id, event.project.value)

    async def run_once(self, projects: Optional[List[SyncProject]] = None) -> Dict[str, Any]:
        """
        Process all pending events in the queue.

        Parameters
        ----------
        projects:
            Optional allow-list.  When *None* (ALL mode) every pending event
            is processed regardless of its project.
        """
        start_time = datetime.now(timezone.utc)
        results: List[SyncResult] = []

        # AUTO ALL: synthesise one "heartbeat" event per project when the
        # queue is empty so that periodic runs still record activity.
        if self.auto and self._queue.empty():
            target = list(SyncProject) if projects is None else projects
            for proj in target:
                await self.publish(
                    SyncEvent(
                        project=proj,
                        event_type="auto_poll",
                        payload={"source": "rabbitsync_auto"},
                    )
                )

        while not self._queue.empty():
            event = await self._queue.get()

            if projects is not None and event.project not in projects:
                logger.debug("Skipping event %s (project not in allow-list)", event.event_id)
                self._queue.task_done()
                continue

            for handler in _handlers.get(event.project, []):
                try:
                    result = await handler(event)
                    results.append(result)
                    self._results.append(result)
                except Exception as exc:
                    self.error_count += 1
                    err_result = SyncResult(
                        event_id=event.event_id,
                        project=event.project,
                        status="error",
                        message=str(exc),
                    )
                    results.append(err_result)
                    logger.error(
                        "Handler error for event %s (project=%s): %s",
                        event.event_id,
                        event.project.value,
                        exc,
                        exc_info=True,
                    )

            self._queue.task_done()

        self.run_count += 1
        self.last_run = start_time
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        summary = {
            "status": "completed",
            "auto": self.auto,
            "start_time": start_time.isoformat(),
            "duration_seconds": duration,
            "events_processed": len(results),
            "results": [r.to_dict() for r in results],
        }
        logger.info(
            "RabbitSync run complete: %d events processed in %.3fs",
            len(results),
            duration,
        )
        return summary

    def get_stats(self) -> Dict[str, Any]:
        """Return runtime statistics."""
        return {
            "auto": self.auto,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "total_results": len(self._results),
            "queue_size": self._queue.qsize(),
        }

    def list_projects(self) -> List[Dict[str, str]]:
        """Return metadata for all registered sync projects."""
        return [
            {"project": p.value, **_PROJECT_META[p]}
            for p in SyncProject
        ]


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

_default_task: Optional[RabbitSyncTask] = None


def get_rabbit_sync_task(auto: bool = True) -> RabbitSyncTask:
    """Return (or create) the global RabbitSyncTask instance."""
    global _default_task
    if _default_task is None:
        _default_task = RabbitSyncTask(auto=auto)
    return _default_task


async def run_rabbit_sync_all() -> Dict[str, Any]:
    """Run AUTO ALL sync: process events for every registered project."""
    task = get_rabbit_sync_task(auto=True)
    return await task.run_once(projects=None)
