import asyncio
import json
import logging
import time
from typing import Optional
import ssl

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from src.config.core import config
from src.services.producers.base_event_handler import (
    DLQ_TASK_NAME,
    MAIN_TASK_NAME,
    EventProducer,
    EventType,
    ProduceResult,
)

logger = logging.getLogger(__name__)


def _parse_bootstrap_servers(value: str) -> list[str]:
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
        return [str(parsed)]
    except json.JSONDecodeError:
        return [s.strip() for s in value.split(",") if s.strip()]


def _create_ssl_context(security_protocol: str, cafile: Optional[str], certfile: Optional[str],
                        keyfile: Optional[str]) -> Optional[ssl.SSLContext]:
    if security_protocol not in ["SSL", "SASL_SSL"]:
        return None

    ssl_context = ssl.create_default_context(cafile=cafile)

    if certfile and keyfile:
        ssl_context.load_cert_chain(certfile=certfile, keyfile=keyfile)

    return ssl_context


class KafkaEventProducer(EventProducer):
    """Publishes alerts to the ingestion topic, with every wait bounded.

    **The produce path is bounded in three layers, and all three are needed.**
    aiokafka defaults `request_timeout_ms` to 40 s, and a send that cannot
    resolve topic metadata polls the brokers for that entire time — roughly 400
    metadata requests aimed at a cluster that is by definition already unhealthy,
    while holding an async slot. So: an explicit `request_timeout_ms`, a per-send
    ceiling (`send_timeout`), and one deadline shared across all attempts
    (`produce_timeout`). Without that last one the fix backfires — `max_retries`
    x `send_timeout` would exceed the 40 s it replaced.

    **Retries are backed off** (`retry_backoff` doubling to `retry_backoff_max`).
    The failure this loop exists to survive is a partition-leader election, which
    takes seconds; attempts fired back-to-back all hit the same broken state, so
    the original three-in-a-millisecond loop was a retry in name only.

    **Connections are established eagerly** via `start()`, so the first alert to
    a fresh pod does not pay the bootstrap cost, and closed by `stop()`, each
    bounded by `stop_timeout` so shutdown cannot hang on a broker that has
    already gone away. `_start_lock` serializes concurrent first-sends and
    probe-driven reconnects, so a burst on a cold producer costs one bootstrap
    rather than N; it is rebound per event loop, because this class is a
    module-level singleton and a Lock reused across loops raises.

    **The DLQ fallback is retained but is not a delivery.** Nothing consumes that
    topic, so `produce()` marks a diverted event via `DLQ_TASK_NAME` and the
    route answers 503 rather than reporting success.
    """

    def __init__(self):
        self._started = False
        self._start_lock: Optional[asyncio.Lock] = None
        self._start_lock_loop = None
        self._last_start_error: Optional[str] = None
        self._last_result: Optional[ProduceResult] = None

        bootstrap_servers = config("KAFKA_BOOTSTRAP_SERVERS", default="localhost:9092")
        self.bootstrap_servers = _parse_bootstrap_servers(bootstrap_servers)
        self.topic = config("KAFKA_TOPIC", default="keep-events")
        self.max_retries = int(config("KAFKA_MAX_RETRIES", default="5"))

        self.request_timeout_ms = int(
            config("KAFKA_REQUEST_TIMEOUT_MS", default="10000")
        )
        self.send_timeout = float(config("KAFKA_SEND_TIMEOUT_SECONDS", default="5"))
        self.produce_timeout = float(
            config("KAFKA_PRODUCE_TIMEOUT_SECONDS", default="15")
        )
        self.retry_backoff = float(
            config("KAFKA_PRODUCE_RETRY_BACKOFF_SECONDS", default="0.5")
        )
        self.retry_backoff_max = float(
            config("KAFKA_PRODUCE_RETRY_BACKOFF_MAX_SECONDS", default="4")
        )
        self.stop_timeout = float(config("KAFKA_STOP_TIMEOUT_SECONDS", default="5"))

        # DLQ config
        dlq_bootstrap_servers_str = config("KAFKA_DLQ_BOOTSTRAP_SERVERS", default=bootstrap_servers)
        self.dlq_topic = config("KAFKA_DLQ_TOPIC", default="keep-events-dlq")
        self.dlq_bootstrap_servers = _parse_bootstrap_servers(dlq_bootstrap_servers_str)

        # SASL config
        self.security_protocol = config("KAFKA_SECURITY_PROTOCOL", default="PLAINTEXT")
        self.sasl_mechanism = config("KAFKA_SASL_MECHANISM", default="PLAIN")
        self.sasl_plain_username = config("KAFKA_SASL_USERNAME", default=None)
        self.sasl_plain_password = config("KAFKA_SASL_PASSWORD", default=None)
        self.ssl_cafile = config("KAFKA_SSL_CAFILE", default=None)
        self.ssl_certfile = config("KAFKA_SSL_CERTFILE", default=None)
        self.ssl_keyfile = config("KAFKA_SSL_KEYFILE", default=None)

        self.ssl_context = _create_ssl_context(
            self.security_protocol,
            self.ssl_cafile,
            self.ssl_certfile,
            self.ssl_keyfile
        )

        self.producer = self._create_producer(self.bootstrap_servers)
        self.dlq_producer = self._create_producer(self.dlq_bootstrap_servers, is_dlq=True)

    def _create_producer(self, bootstrap_servers: list[str], is_dlq: bool = False) -> AIOKafkaProducer:
        if is_dlq:
            username = config("KAFKA_DLQ_SASL_USERNAME", default=self.sasl_plain_username)
            password = config("KAFKA_DLQ_SASL_PASSWORD", default=self.sasl_plain_password)
        else:
            username = self.sasl_plain_username
            password = self.sasl_plain_password

        return AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=username,
            sasl_plain_password=password,
            ssl_context=self.ssl_context,
            api_version="auto",
            request_timeout_ms=self.request_timeout_ms,
        )

    def _get_start_lock(self) -> asyncio.Lock:
        """A lock bound to the currently running loop. This producer is a
        module-level singleton, and a Lock reused across loops raises
        "attached to a different loop"."""
        loop = asyncio.get_running_loop()
        if self._start_lock is None or self._start_lock_loop is not loop:
            self._start_lock = asyncio.Lock()
            self._start_lock_loop = loop
        return self._start_lock

    async def _ensure_started(self):
        if self._started:
            return
        async with self._get_start_lock():
            if self._started:
                return
            try:
                await self.producer.start()
            except Exception as exc:
                self._last_start_error = f"main topic '{self.topic}': {type(exc).__name__}: {exc}"
                logger.error(
                    "Failed to connect Kafka main producer (topic: %s, servers: %s): %s",
                    self.topic,
                    self.bootstrap_servers,
                    exc,
                )
                raise

            try:
                await self.dlq_producer.start()
            except Exception as exc:
                self._last_start_error = f"DLQ topic '{self.dlq_topic}': {type(exc).__name__}: {exc}"
                logger.error(
                    "Failed to connect Kafka DLQ producer (topic: %s, servers: %s): %s",
                    self.dlq_topic,
                    self.dlq_bootstrap_servers,
                    exc,
                )
                raise

            self._started = True
            self._last_start_error = None

    async def start(self) -> None:
        """Eagerly connect at app startup, moving the bootstrap cost (and a
        possible DLQ diversion) off the first request's path.

        Failures are logged, not raised: the pod still starts, /readyz reports
        the producer unhealthy, and `produce()` retries the connection.
        """
        try:
            await self._ensure_started()
            logger.info(
                "Kafka producer connected at startup",
                extra={
                    "topic": self.topic,
                    "bootstrap_servers": self.bootstrap_servers,
                    "dlq_topic": self.dlq_topic,
                    "dlq_bootstrap_servers": self.dlq_bootstrap_servers,
                },
            )
        except Exception:
            logger.exception(
                "Failed to connect the Kafka producer at startup; will retry on "
                "first produce"
            )

    async def stop(self) -> None:
        """Close both producers on shutdown.

        Started eagerly but never closed, they are reclaimed by process exit and
        aiokafka logs "Unclosed AIOKafkaProducer" on every restart. Each stop is
        bounded and guarded: shutting down must not hang on a broker that has
        already gone away.
        """
        for name, producer in (("main", self.producer), ("dlq", self.dlq_producer)):
            try:
                await asyncio.wait_for(producer.stop(), timeout=self.stop_timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "Timed out closing the %s Kafka producer after %ss",
                    name,
                    self.stop_timeout,
                )
            except Exception:
                logger.exception("Failed to close the %s Kafka producer", name)
        self._started = False

    async def health(self, attempt_reconnect: bool = False) -> tuple[bool, dict]:
        """Producer connectivity for /readyz. With `attempt_reconnect` the probe
        doubles as a reconnect trigger, so a cold pod keeps retrying while
        reporting NotReady instead of quietly DLQ-ing what arrives next."""
        if not self._started and attempt_reconnect:
            try:
                await self._ensure_started()
                logger.info(
                    "Kafka producer reconnected successfully",
                    extra={
                        "topic": self.topic,
                        "bootstrap_servers": self.bootstrap_servers,
                        "dlq_topic": self.dlq_topic,
                        "dlq_bootstrap_servers": self.dlq_bootstrap_servers,
                    },
                )
            except Exception:
                logger.warning(
                    "Kafka producer still not connected: %s", self._last_start_error
                )

        detail = {
            "producer": "kafka",
            "started": self._started,
            "topic": self.topic,
            "bootstrap_servers": self.bootstrap_servers,
            "dlq_topic": self.dlq_topic,
            "dlq_bootstrap_servers": self.dlq_bootstrap_servers,
        }
        if self._last_start_error:
            detail["last_error"] = self._last_start_error
        if self._last_result is not None:
            detail["last_produce_result"] = self._last_result.value
        return self._started, detail

    def last_produce_result(self) -> Optional[ProduceResult]:
        return self._last_result

    async def _send_to_dlq(self, value: bytes, trace_id: str) -> str:
        """Divert to the dead-letter topic, and say so.

        Bounded like the main send. Only the main topic is consumed, so an event
        that lands here is NOT ingested — the returned task name carries the
        marker that lets the route answer 503 instead of reporting success.
        """
        try:
            await self.dlq_producer.start()
        except RuntimeError:
            pass

        await self._send_bounded(
            self.dlq_producer, self.dlq_topic, value, self.send_timeout
        )
        logger.warning(
            "Produced event to the DLQ topic — it will NOT be ingested by "
            "keep-event-handler",
            extra={"dlq_topic": self.dlq_topic, "trace_id": trace_id},
        )
        self._last_result = ProduceResult.DLQ

        return DLQ_TASK_NAME

    def _build_payload(self, event: dict, event_type: EventType, **kwargs) -> dict:
        return {
            "event": event,
            "event_type": event_type.value if hasattr(event_type, "value") else event_type,
            "tenant_id": kwargs.get("tenant_id"),
            "provider_type": kwargs.get("provider_type"),
            "provider_id": kwargs.get("provider_id"),
            "fingerprint": kwargs.get("fingerprint"),
            "api_key_name": kwargs.get("api_key_name"),
            "trace_id": kwargs.get("trace_id", "unknown"),
            "provider_name": kwargs.get("provider_name"),
        }

    def _serialize_payload(self, payload: dict) -> bytes:
        try:
            # Serialize payload, handling Pydantic models (like AlertDto) and other objects
            return json.dumps(
                payload, default=lambda o: o.dict() if hasattr(o, "dict") else str(o)
            ).encode("utf-8")
        except Exception:
            logger.exception("Failed to serialize event payload")
            raise

    async def _send_bounded(self, producer, topic: str, value: bytes, timeout: float):
        """A single send that cannot outlast `timeout`.

        `send_and_wait` against an unreachable broker — or a topic that does not
        exist — otherwise blocks for the client's full request timeout. Bounding
        it means a failure costs seconds, not tens of seconds, whatever the
        cause.
        """
        await asyncio.wait_for(producer.send_and_wait(topic, value), timeout=timeout)

    async def _produce_with_retry(self, value: bytes, trace_id: str) -> Optional[str]:
        """Attempt the main topic with bounded, backed-off retries.

        The backoff is the point: a leader election takes seconds, so attempts
        fired back-to-back all hit the same broken state. Spacing them lets
        attempt 2 or 3 land after the new leader is elected.

        All attempts share one deadline, so retries × per-send timeout can't add
        up to a longer stall than the one this replaces.
        """
        deadline = time.monotonic() + self.produce_timeout
        backoff = self.retry_backoff

        for attempt in range(self.max_retries):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning(
                    "Produce budget of %ss exhausted after %s attempt(s) to %s: %s",
                    self.produce_timeout,
                    attempt,
                    self.topic,
                    trace_id,
                )
                break

            try:
                await self._send_bounded(
                    self.producer, self.topic, value, min(self.send_timeout, remaining)
                )
                logger.info(f"Successfully produced event to Kafka topic {self.topic}: {trace_id}")

                return MAIN_TASK_NAME
            except asyncio.TimeoutError:
                logger.warning(
                    f"Timed out producing to Kafka main topic {self.topic} "
                    f"(attempt {attempt + 1}/{self.max_retries}, "
                    f"limit {self.send_timeout}s): {trace_id}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to produce to Kafka main topic {self.topic} (attempt {attempt + 1}/{self.max_retries}): {e}")

            if attempt < self.max_retries - 1:
                pause = min(backoff, max(0.0, deadline - time.monotonic()))
                if pause > 0:
                    await asyncio.sleep(pause)
                backoff = min(backoff * 2, self.retry_backoff_max)

        return None

    async def produce(self, event: dict, event_type: EventType = EventType.ALERT, **kwargs) -> str:
        trace_id = kwargs.get("trace_id", "unknown")
        payload = self._build_payload(event, event_type, **kwargs)
        value = self._serialize_payload(payload)

        try:
            await self._ensure_started()
        except KafkaConnectionError as e:
            logger.warning(f"Failed to connect to kafka: {e}. Sending directly to DLQ")
            return await self._send_to_dlq_or_raise(value, trace_id)

        result = await self._produce_with_retry(value, trace_id)
        if result:
            self._last_result = ProduceResult.MAIN
            return result

        logger.warning(f"All {self.max_retries} attempts to main topic {self.topic} failed. Sending to DLQ")
        return await self._send_to_dlq_or_raise(value, trace_id)

    async def _send_to_dlq_or_raise(self, value: bytes, trace_id: str) -> str:
        try:
            return await self._send_to_dlq(value, trace_id)
        except Exception:
            logger.exception("Failed to produce event to DLQ")
            raise

    async def close(self):
        if self._started:
            await self.producer.stop()
            await self.dlq_producer.stop()
            self._started = False
