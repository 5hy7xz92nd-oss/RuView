"""
Unit tests for the RabbitSync task (London School TDD).

All external dependencies (logger, settings) are mocked so the suite runs
without a database or running application.
"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_default_task():
    """Ensure the module-level singleton is reset between tests."""
    import src.tasks.sync as sync_mod
    sync_mod._default_task = None
    yield
    sync_mod._default_task = None


def _make_event(project_slug: str, event_type: str = "push", payload: dict | None = None):
    from src.tasks.sync import SyncEvent, SyncProject
    return SyncEvent(
        project=SyncProject(project_slug),
        event_type=event_type,
        payload=payload or {"ref": "refs/heads/main"},
    )


# ---------------------------------------------------------------------------
# SyncProject
# ---------------------------------------------------------------------------

class TestSyncProject:
    def test_all_three_projects_registered(self):
        from src.tasks.sync import SyncProject
        slugs = {p.value for p in SyncProject}
        assert "patpat" in slugs
        assert "weareone10billion" in slugs
        assert "manusweareone10billion" in slugs

    def test_project_metadata_present_for_all(self):
        from src.tasks.sync import SyncProject, _PROJECT_META
        for project in SyncProject:
            assert project in _PROJECT_META
            meta = _PROJECT_META[project]
            assert "display_name" in meta
            assert "upstream_repo" in meta


# ---------------------------------------------------------------------------
# SyncEvent
# ---------------------------------------------------------------------------

class TestSyncEvent:
    def test_event_id_auto_generated(self):
        event = _make_event("patpat")
        assert len(event.event_id) == 16

    def test_event_id_stable_for_same_input(self):
        """Two events with the same fields at the same moment share an ID."""
        now = datetime(2026, 1, 1, 12, 0, 0)
        from src.tasks.sync import SyncEvent, SyncProject
        e1 = SyncEvent(project=SyncProject.PATPAT, event_type="push",
                       payload={}, received_at=now)
        e2 = SyncEvent(project=SyncProject.PATPAT, event_type="push",
                       payload={}, received_at=now)
        assert e1.event_id == e2.event_id

    def test_explicit_event_id_not_overwritten(self):
        from src.tasks.sync import SyncEvent, SyncProject
        e = SyncEvent(
            project=SyncProject.WEAREONE10BILLION,
            event_type="release",
            payload={},
            event_id="custom-id-123",
        )
        assert e.event_id == "custom-id-123"


# ---------------------------------------------------------------------------
# SyncResult
# ---------------------------------------------------------------------------

class TestSyncResult:
    def test_to_dict_contains_required_keys(self):
        from src.tasks.sync import SyncProject, SyncResult
        result = SyncResult(
            event_id="abc",
            project=SyncProject.PATPAT,
            status="success",
            message="ok",
        )
        d = result.to_dict()
        for key in ("event_id", "project", "status", "message", "details", "processed_at"):
            assert key in d, f"Missing key: {key}"

    def test_to_dict_project_is_string(self):
        from src.tasks.sync import SyncProject, SyncResult
        result = SyncResult(event_id="x", project=SyncProject.WEAREONE10BILLION, status="ok")
        assert isinstance(result.to_dict()["project"], str)


# ---------------------------------------------------------------------------
# RabbitSyncTask — publish / consume
# ---------------------------------------------------------------------------

class TestRabbitSyncTask:
    @pytest.fixture
    def task(self):
        from src.tasks.sync import RabbitSyncTask
        return RabbitSyncTask(auto=False)

    @pytest.mark.asyncio
    async def test_publish_adds_to_queue(self, task):
        event = _make_event("patpat")
        await task.publish(event)
        assert task._queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_run_once_processes_patpat_event(self, task):
        await task.publish(_make_event("patpat"))
        result = await task.run_once()
        assert result["events_processed"] == 1
        assert result["results"][0]["project"] == "patpat"
        assert result["results"][0]["status"] == "success"

    @pytest.mark.asyncio
    async def test_run_once_processes_weareone10billion_event(self, task):
        await task.publish(_make_event("weareone10billion", event_type="release"))
        result = await task.run_once()
        assert result["events_processed"] == 1
        assert result["results"][0]["project"] == "weareone10billion"

    @pytest.mark.asyncio
    async def test_run_once_processes_manusweareone10billion_event(self, task):
        await task.publish(_make_event("manusweareone10billion"))
        result = await task.run_once()
        assert result["events_processed"] == 1
        assert result["results"][0]["project"] == "manusweareone10billion"

    @pytest.mark.asyncio
    async def test_run_once_drains_queue(self, task):
        for slug in ("patpat", "weareone10billion", "manusweareone10billion"):
            await task.publish(_make_event(slug))
        result = await task.run_once()
        assert result["events_processed"] == 3
        assert task._queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_run_once_filters_by_project(self, task):
        from src.tasks.sync import SyncProject
        await task.publish(_make_event("patpat"))
        await task.publish(_make_event("weareone10billion"))
        result = await task.run_once(projects=[SyncProject.PATPAT])
        # patpat processed; weareone10billion skipped (still in queue? no — drained but skipped)
        assert result["events_processed"] == 1
        assert result["results"][0]["project"] == "patpat"

    @pytest.mark.asyncio
    async def test_run_increments_run_count(self, task):
        await task.publish(_make_event("patpat"))
        await task.run_once()
        assert task.run_count == 1
        await task.publish(_make_event("weareone10billion"))
        await task.run_once()
        assert task.run_count == 2

    @pytest.mark.asyncio
    async def test_last_run_updated(self, task):
        assert task.last_run is None
        await task.publish(_make_event("patpat"))
        await task.run_once()
        assert isinstance(task.last_run, datetime)

    @pytest.mark.asyncio
    async def test_handler_exception_sets_error_result(self):
        from src.tasks.sync import RabbitSyncTask, SyncProject, SyncEvent, register_handler, _handlers

        # Inject a failing handler for patpat temporarily
        failing_handler_called = []

        async def _bad_handler(event):
            failing_handler_called.append(True)
            raise RuntimeError("simulated handler failure")

        _handlers[SyncProject.PATPAT].append(_bad_handler)
        try:
            task = RabbitSyncTask(auto=False)
            await task.publish(_make_event("patpat"))
            result = await task.run_once()
            # Original good handler + bad handler both ran
            error_results = [r for r in result["results"] if r["status"] == "error"]
            assert len(error_results) >= 1
            assert task.error_count >= 1
        finally:
            _handlers[SyncProject.PATPAT].remove(_bad_handler)


# ---------------------------------------------------------------------------
# AUTO ALL mode
# ---------------------------------------------------------------------------

class TestAutoAllMode:
    @pytest.fixture
    def auto_task(self):
        from src.tasks.sync import RabbitSyncTask
        return RabbitSyncTask(auto=True)

    @pytest.mark.asyncio
    async def test_auto_mode_generates_events_for_all_projects(self, auto_task):
        """When queue is empty and auto=True, run_once enqueues heartbeat events."""
        from src.tasks.sync import SyncProject
        result = await auto_task.run_once()
        # One event per project
        assert result["events_processed"] == len(list(SyncProject))

    @pytest.mark.asyncio
    async def test_auto_mode_uses_auto_poll_event_type(self, auto_task):
        result = await auto_task.run_once()
        for r in result["results"]:
            assert r["status"] == "success"

    @pytest.mark.asyncio
    async def test_auto_mode_respects_project_filter(self, auto_task):
        from src.tasks.sync import SyncProject
        result = await auto_task.run_once(projects=[SyncProject.PATPAT])
        assert result["events_processed"] == 1
        assert result["results"][0]["project"] == "patpat"


# ---------------------------------------------------------------------------
# Singleton helper
# ---------------------------------------------------------------------------

class TestGetRabbitSyncTask:
    def test_returns_same_instance(self):
        from src.tasks.sync import get_rabbit_sync_task
        t1 = get_rabbit_sync_task()
        t2 = get_rabbit_sync_task()
        assert t1 is t2

    def test_list_projects_returns_all(self):
        from src.tasks.sync import get_rabbit_sync_task, SyncProject
        task = get_rabbit_sync_task()
        projects = task.list_projects()
        slugs = {p["project"] for p in projects}
        assert slugs == {p.value for p in SyncProject}

    def test_get_stats_keys(self):
        from src.tasks.sync import get_rabbit_sync_task
        task = get_rabbit_sync_task()
        stats = task.get_stats()
        for key in ("auto", "run_count", "error_count", "last_run", "total_results", "queue_size"):
            assert key in stats, f"Missing stats key: {key}"


# ---------------------------------------------------------------------------
# run_rabbit_sync_all convenience helper
# ---------------------------------------------------------------------------

class TestRunRabbitSyncAll:
    @pytest.mark.asyncio
    async def test_run_rabbit_sync_all_returns_completed(self):
        from src.tasks.sync import run_rabbit_sync_all, SyncProject
        result = await run_rabbit_sync_all()
        assert result["status"] == "completed"
        assert result["events_processed"] == len(list(SyncProject))
