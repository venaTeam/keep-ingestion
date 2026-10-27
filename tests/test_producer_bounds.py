"""
Tests that the produce path is bounded and that its retries can survive a leader
election.

* **Bounded** — an unbounded send holds a worker slot for aiokafka's 40 s default
  and hammers an already-unhealthy cluster with metadata requests.
* **Backed off** — attempts fired back-to-back all land in the same millisecond
  and hit the same broken state; a leader election takes seconds.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.producers.base_event_handler import DLQ_TASK_NAME, MAIN_TASK_NAME
from src.services.producers.kafka_producer import KafkaEventProducer


def _producer(**overrides):
    with patch("src.services.producers.kafka_producer.AIOKafkaProducer"):
        producer = KafkaEventProducer()
    producer._started = True
    producer.producer = MagicMock()
    producer.dlq_producer = MagicMock()
    producer.dlq_producer.start = AsyncMock()
    # Keep the tests fast; the ratios are what matter, not the absolute values.
    producer.send_timeout = 0.2
    producer.produce_timeout = 2.0
    producer.retry_backoff = 0.05
    producer.retry_backoff_max = 0.2
    for key, value in overrides.items():
        setattr(producer, key, value)
    return producer


def test_request_timeout_is_passed_to_the_client():
    """The 40 s default is what made a failed send so expensive."""
    with patch("src.services.producers.kafka_producer.AIOKafkaProducer") as mock_cls:
        producer = KafkaEventProducer()

    assert producer.request_timeout_ms < 40000
    for call in mock_cls.call_args_list:
        assert call.kwargs["request_timeout_ms"] == producer.request_timeout_ms


@pytest.mark.asyncio
async def test_a_hanging_send_is_bounded_not_left_to_run_40s():
    producer = _producer(max_retries=1)

    async def never_returns(*args, **kwargs):
        await asyncio.sleep(30)

    producer.producer.send_and_wait = never_returns

    started = time.monotonic()
    result = await producer._produce_with_retry(b"payload", "trace-1")
    elapsed = time.monotonic() - started

    assert result is None
    assert elapsed < 1.0  # bounded by send_timeout, not the 30 s sleep


@pytest.mark.asyncio
async def test_retries_are_spaced_so_a_leader_election_can_resolve():
    """Without backoff all attempts land inside the same millisecond and the
    loop cannot outlast a seconds-long leader election."""
    producer = _producer(max_retries=4)
    attempt_times = []

    async def failing(*args, **kwargs):
        attempt_times.append(time.monotonic())
        raise RuntimeError("leader unavailable")

    producer.producer.send_and_wait = failing

    await producer._produce_with_retry(b"payload", "trace-1")

    assert len(attempt_times) == 4
    gaps = [b - a for a, b in zip(attempt_times, attempt_times[1:])]
    assert all(g > 0 for g in gaps), "attempts must not be back-to-back"
    # ...and the spacing grows rather than staying flat.
    assert gaps[-1] > gaps[0]


@pytest.mark.asyncio
async def test_a_later_attempt_succeeding_is_reported_as_success():
    """The whole point: the blip resolves and the sender never sees an error."""
    producer = _producer(max_retries=4)
    calls = []

    async def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("leader unavailable")

    producer.producer.send_and_wait = flaky

    result = await producer._produce_with_retry(b"payload", "trace-1")

    assert result == MAIN_TASK_NAME
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_total_time_is_capped_by_the_shared_budget():
    """retries × per-send timeout must not add up to something worse than the
    40 s stall this replaces."""
    producer = _producer(max_retries=50, produce_timeout=0.6, send_timeout=0.1)

    async def never_returns(*args, **kwargs):
        await asyncio.sleep(30)

    producer.producer.send_and_wait = never_returns

    started = time.monotonic()
    result = await producer._produce_with_retry(b"payload", "trace-1")
    elapsed = time.monotonic() - started

    assert result is None
    assert elapsed < 2.0  # nowhere near 50 × 0.1 + backoff


@pytest.mark.asyncio
async def test_dlq_send_is_bounded_too():
    """The DLQ topic doesn't exist, so this send can only ever spend the timeout
    and fail — it must fail fast."""
    producer = _producer()

    async def never_returns(*args, **kwargs):
        await asyncio.sleep(30)

    producer.dlq_producer.send_and_wait = never_returns

    started = time.monotonic()
    with pytest.raises(asyncio.TimeoutError):
        await producer._send_to_dlq(b"payload", "trace-1")
    elapsed = time.monotonic() - started

    assert elapsed < 1.0


@pytest.mark.asyncio
async def test_successful_first_attempt_does_not_sleep():
    """No latency added to the happy path."""
    producer = _producer(retry_backoff=5.0)
    producer.producer.send_and_wait = AsyncMock()

    started = time.monotonic()
    result = await producer._produce_with_retry(b"payload", "trace-1")
    elapsed = time.monotonic() - started

    assert result == MAIN_TASK_NAME
    assert elapsed < 0.5


@pytest.mark.asyncio
async def test_exhausted_main_topic_still_falls_back_to_the_dlq():
    """Bounding must not change which sink is chosen — only how long it takes to
    get there."""
    producer = _producer(max_retries=2)
    producer.producer.send_and_wait = AsyncMock(side_effect=RuntimeError("no broker"))
    producer.dlq_producer.send_and_wait = AsyncMock()

    task_name = await producer.produce(event={"a": 1}, trace_id="t-1")

    assert task_name == DLQ_TASK_NAME
