import logging
from typing import Optional

from src.services.producers.base_event_handler import EventProducer
from src.services.producers.kafka_producer import KafkaEventProducer
from src.config.core import config

logger = logging.getLogger(__name__)

# Global producer instance for reuse
_kafka_producer_instance = None


def _messaging_type() -> str:
    return config("MESSAGING_TYPE", default="KAFKA").upper()


async def get_event_producer() -> EventProducer:
    messaging_type = _messaging_type()
    global _kafka_producer_instance

    if messaging_type != "KAFKA":
        logger.warning(f"Unknown MESSAGING_TYPE: {messaging_type}, defaulting to KAFKA")

    if _kafka_producer_instance is None:
        _kafka_producer_instance = KafkaEventProducer()
    return _kafka_producer_instance


def get_producer_instance() -> Optional[EventProducer]:
    """The producer for this process, or None if it hasn't been created yet.

    Side-effect free, so /readyz can inspect it without constructing one.
    """
    return _kafka_producer_instance


async def start_event_producer() -> Optional[EventProducer]:
    """Create and connect the producer during app startup, so a cold pod doesn't
    divert the first alerts it receives to the (unconsumed) DLQ topic.

    Never raises: a broker that isn't up yet must not stop the gateway from
    starting. /readyz reports the producer as unhealthy until it connects.
    """
    try:
        producer = await get_event_producer()
    except Exception:
        logger.exception("Failed to create the event producer at startup")
        return None

    try:
        await producer.start()
    except Exception:
        logger.exception("Failed to start the event producer at startup")

    return producer


async def stop_event_producer() -> None:
    """Close the producer on app shutdown. Never raises — a failure here must not
    hold up the rest of the shutdown."""
    producer = get_producer_instance()
    if producer is None:
        return

    try:
        await producer.stop()
        logger.info("Event producer stopped")
    except Exception:
        logger.exception("Failed to stop the event producer")
