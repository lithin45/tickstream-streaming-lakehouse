"""Thin helpers over confluent-kafka: producer/consumer factories, topic admin, health.

Keeps Kafka boilerplate in one place so producer/processor code stays readable.
"""

from __future__ import annotations

import time
from collections.abc import Iterable

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from tickstream.config import KafkaSettings, Settings
from tickstream.logging import get_logger

log = get_logger("kafka")


def build_producer(settings: Settings, **overrides: object) -> Producer:
    """Construct a confluent-kafka Producer with sane streaming defaults."""
    conf = {
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "client.id": settings.kafka.client_id,
        "broker.address.family": settings.kafka.broker_address_family,
        # Durability + ordering: idempotent producer keeps per-partition order on retries.
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
        **overrides,
    }
    return Producer(conf)


def build_consumer(
    settings: Settings,
    group_id: str,
    *,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = True,
    **overrides: object,
) -> Consumer:
    """Construct a confluent-kafka Consumer."""
    conf = {
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "group.id": group_id,
        "client.id": settings.kafka.client_id,
        "broker.address.family": settings.kafka.broker_address_family,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
        **overrides,
    }
    return Consumer(conf)


def admin_client(kafka: KafkaSettings) -> AdminClient:
    return AdminClient(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "broker.address.family": kafka.broker_address_family,
        }
    )


def wait_for_broker(settings: Settings, timeout: float = 30.0, interval: float = 1.0) -> bool:
    """Block until the broker answers a metadata request or ``timeout`` elapses.

    Returns ``True`` if the broker is reachable, ``False`` otherwise (no exception).
    """
    admin = admin_client(settings.kafka)
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            md = admin.list_topics(timeout=min(interval * 2, 5.0))
            if md.brokers:
                return True
        except KafkaException as exc:  # broker not up yet
            last_err = exc
        time.sleep(interval)
    log.warning(
        "broker_unreachable", bootstrap=settings.kafka.bootstrap_servers, error=str(last_err)
    )
    return False


def ensure_topics(
    settings: Settings,
    topics: Iterable[str] | None = None,
    *,
    timeout: float = 20.0,
) -> list[str]:
    """Create any missing topics. Idempotent — existing topics are left untouched.

    Returns the list of topics that were newly created.
    """
    names = list(topics) if topics is not None else settings.topics.all()
    admin = admin_client(settings.kafka)
    existing = set(admin.list_topics(timeout=timeout).topics)
    to_create = [n for n in names if n not in existing]
    if not to_create:
        return []

    new_topics = [
        NewTopic(
            n,
            num_partitions=settings.kafka.default_partitions,
            replication_factor=settings.kafka.default_replication,
        )
        for n in to_create
    ]
    futures = admin.create_topics(new_topics)
    created: list[str] = []
    for name, fut in futures.items():
        try:
            fut.result(timeout=timeout)
            created.append(name)
        except KafkaException as exc:
            # TOPIC_ALREADY_EXISTS can happen under a race; treat as success.
            if "already exists" in str(exc).lower():
                continue
            raise
    if created:
        log.info("topics_created", topics=created)
    return created


def _delivery_report(err: object, msg: object) -> None:
    """confluent-kafka delivery callback for structured logging."""
    if err is not None:
        log.error("delivery_failed", error=str(err))
    else:
        log.debug(
            "delivered",
            topic=msg.topic(),  # type: ignore[attr-defined]
            partition=msg.partition(),  # type: ignore[attr-defined]
            offset=msg.offset(),  # type: ignore[attr-defined]
        )


def delivery_report(err: object, msg: object) -> None:
    """Public alias for the delivery callback."""
    _delivery_report(err, msg)


class DeliveryCounter:
    """Stateful delivery callback that counts acks and failures.

    confluent-kafka's ``flush()`` only reports how many messages remain *queued*; a message
    that the broker rejected after being sent is already off the queue, so ``flush()`` alone
    can't tell you everything landed. Pass an instance as ``on_delivery`` and check
    ``failed == 0`` to be sure.
    """

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def __call__(self, err: object, _msg: object) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery_failed", error=str(err))
        else:
            self.delivered += 1


def end_offset_total(settings: Settings, topics: Iterable[str], *, timeout: float = 10.0) -> int:
    """Sum the high-watermark offsets across all partitions of ``topics``.

    Used by tests to count messages that actually landed, independent of how many runs have
    accumulated (no consuming required).
    """
    consumer = build_consumer(
        settings, group_id="tickstream-offset-probe", enable_auto_commit=False
    )
    total = 0
    try:
        cluster = consumer.list_topics(timeout=timeout)
        for topic in topics:
            meta = cluster.topics.get(topic)
            if meta is None:
                continue
            for partition in meta.partitions:
                _low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=timeout
                )
                total += high
    finally:
        consumer.close()
    return total
