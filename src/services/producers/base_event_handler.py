import abc
from enum import Enum

class EventType(str, Enum):
    ALERT = "alert"
    INCIDENT = "incident"
    ENRICH = "enrich"
    BATCH_ENRICH = "batch_enrich"
    DELETE = "delete"


class ProduceResult(str, Enum):
    """Where an event actually landed — a `produce()` that didn't raise may still
    have fallen back to the DLQ topic, which nothing consumes."""

    MAIN = "main"
    DLQ = "dlq"


# Task names returned by `produce()`. The `-dlq` suffix is the per-request signal
# that an event was diverted; `result_from_task_name` is the only reader.
MAIN_TASK_NAME = "kafka-async-task"
DLQ_TASK_NAME = "kafka-async-task-dlq"


def result_from_task_name(task_name) -> ProduceResult:
    """Classify a `produce()` return value. Per-request, unlike
    `last_produce_result()`, which is racy shared state on the producer."""
    if isinstance(task_name, str) and task_name.endswith("-dlq"):
        return ProduceResult.DLQ
    return ProduceResult.MAIN


class EventProducer(abc.ABC):
    @abc.abstractmethod
    async def produce(self, event: dict, event_type: EventType = EventType.ALERT, **kwargs):
        pass

    async def start(self) -> None:
        """Eagerly establish connections (called once on app startup). No-op by
        default so connectionless producers and test doubles needn't implement
        it."""
        return None

    async def stop(self) -> None:
        """Release connections on app shutdown. No-op by default, as `start`."""
        return None

    def last_produce_result(self) -> ProduceResult | None:
        """Sink used by the most recent successful `produce()`, if tracked."""
        return None

    async def health(self, attempt_reconnect: bool = False) -> tuple[bool, dict]:
        """(healthy, detail) for the /readyz probe. Default: healthy."""
        return True, {"producer": type(self).__name__, "checked": False}
