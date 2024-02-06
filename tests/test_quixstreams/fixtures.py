import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Union
from unittest.mock import create_autospec, patch

import pytest
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,  # type: ignore
    NewPartitions,  # type: ignore
)

from quixstreams.app import Application, MessageProcessedCallback
from quixstreams.error_callbacks import (
    ConsumerErrorCallback,
    ProducerErrorCallback,
    ProcessingErrorCallback,
)
from quixstreams.kafka import (
    Partitioner,
    AutoOffsetReset,
    Consumer,
    Producer,
    AssignmentStrategy,
)
from quixstreams.models import MessageContext
from quixstreams.models.rows import Row
from quixstreams.models.serializers import (
    Serializer,
    Deserializer,
    JSONSerializer,
    JSONDeserializer,
)
from quixstreams.models.timestamps import (
    TimestampType,
    MessageTimestamp,
)
from quixstreams.models.topics import Topic, TopicManager, TopicAdmin
from quixstreams.platforms.quix import QuixTopicManager
from quixstreams.platforms.quix.config import (
    QuixKafkaConfigsBuilder,
    prepend_workspace_id,
    strip_workspace_id_prefix,
)
from quixstreams.rowconsumer import RowConsumer
from quixstreams.rowproducer import RowProducer
from quixstreams.state import StateStoreManager
from quixstreams.state.recovery import ChangelogManager, RecoveryManager


@pytest.fixture()
def kafka_admin_client(kafka_container) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_container.broker_address})


@pytest.fixture()
def random_consumer_group() -> str:
    return str(uuid.uuid4())


@pytest.fixture()
def consumer_factory(kafka_container, random_consumer_group):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        assignment_strategy: AssignmentStrategy = "range",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
    ) -> Consumer:
        consumer_group = consumer_group or random_consumer_group
        extra_config = extra_config or {}

        # Make consumers to refresh cluster metadata often
        # to react on re-assignment changes faster
        extra_config["topic.metadata.refresh.interval.ms"] = 3000

        return Consumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            assignment_strategy=assignment_strategy,
            auto_offset_reset=auto_offset_reset,
            extra_config=extra_config,
        )

    return factory


@pytest.fixture()
def consumer(consumer_factory) -> Consumer:
    return consumer_factory()


@pytest.fixture()
def producer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        extra_config: dict = None,
    ) -> Producer:
        extra_config = extra_config or {}

        return Producer(
            broker_address=broker_address,
            extra_config=extra_config,
        )

    return factory


@pytest.fixture()
def producer(producer_factory) -> Producer:
    return producer_factory()


@pytest.fixture()
def executor() -> ThreadPoolExecutor:
    executor = ThreadPoolExecutor(1)
    yield executor
    # Kill all the threads after leaving the test
    executor.shutdown(wait=False)


@pytest.fixture()
def topic_factory(kafka_admin_client):
    """
    For when you need to create a topic in Kafka.

    The factory will return the resulting topic name and partition count
    """

    def factory(
        topic: str = None, num_partitions: int = 1, timeout: float = 20.0
    ) -> (str, int):
        topic_name = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_topics(
            [NewTopic(topic=topic_name, num_partitions=num_partitions)]
        )
        futures[topic_name].result(timeout)
        return topic_name, num_partitions

    return factory


@pytest.fixture()
def topic_json_serdes_factory(topic_factory):
    """
    For when you need to create a topic in Kafka and want a `Topic` object afterward.
    Additionally, uses JSON serdes for message values by default.

    The factory will return the resulting Topic object.
    """

    def factory(
        topic: str = None,
        num_partitions: int = 1,
        timeout: float = 10.0,
        create_topic: bool = True,
    ):
        if create_topic:
            topic_name, _ = topic_factory(
                topic=topic, num_partitions=num_partitions, timeout=timeout
            )
        else:
            topic_name = uuid.uuid4()
        return Topic(
            name=topic or topic_name,
            value_deserializer=JSONDeserializer(),
            value_serializer=JSONSerializer(),
        )

    return factory


@pytest.fixture()
def set_topic_partitions(kafka_admin_client):
    def func(
        topic: str = None, num_partitions: int = 1, timeout: float = 10.0
    ) -> (str, int):
        topic = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_partitions(
            [NewPartitions(topic=topic, new_total_count=num_partitions)]
        )
        futures[topic].result(timeout)
        return topic, num_partitions

    return func


@pytest.fixture()
def row_consumer_factory(kafka_container, random_consumer_group):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
        on_error: Optional[ConsumerErrorCallback] = None,
    ) -> RowConsumer:
        extra_config = extra_config or {}
        consumer_group = consumer_group or random_consumer_group

        # Make consumers to refresh cluster metadata often
        # to react on re-assignment changes faster
        extra_config["topic.metadata.refresh.interval.ms"] = 3000
        return RowConsumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            auto_offset_reset=auto_offset_reset,
            extra_config=extra_config,
            on_error=on_error,
        )

    return factory


@pytest.fixture()
def row_producer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        partitioner: Partitioner = "murmur2",
        extra_config: dict = None,
        on_error: Optional[ProducerErrorCallback] = None,
    ) -> RowProducer:
        return RowProducer(
            broker_address=broker_address,
            partitioner=partitioner,
            extra_config=extra_config,
            on_error=on_error,
        )

    return factory


@pytest.fixture()
def row_factory():
    """
    This factory includes only the fields typically handed to a producer when
    producing a message; more generally, the fields you would likely
    need to validate upon producing/consuming.
    """

    def factory(
        value,
        topic="input-topic",
        key=b"key",
        headers=None,
        partition: int = 0,
        offset: int = 0,
    ) -> Row:
        headers = headers or {}
        context = MessageContext(
            key=key,
            headers=headers,
            topic=topic,
            partition=partition,
            offset=offset,
            size=0,
            timestamp=MessageTimestamp(0, TimestampType.TIMESTAMP_NOT_AVAILABLE),
        )
        return Row(value=value, context=context)

    return factory


@pytest.fixture()
def app_factory(kafka_container, random_consumer_group, tmp_path):
    def factory(
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        state_dir: Optional[str] = None,
        auto_create_topics: bool = True,
        use_changelog_topics: bool = True,
        topic_manager: Optional[TopicManager] = None,
    ) -> Application:
        state_dir = state_dir or (tmp_path / "state").absolute()
        return Application(
            broker_address=kafka_container.broker_address,
            consumer_group=consumer_group or random_consumer_group,
            auto_offset_reset=auto_offset_reset,
            consumer_extra_config=consumer_extra_config,
            producer_extra_config=producer_extra_config,
            on_consumer_error=on_consumer_error,
            on_producer_error=on_producer_error,
            on_processing_error=on_processing_error,
            on_message_processed=on_message_processed,
            state_dir=state_dir,
            auto_create_topics=auto_create_topics,
            use_changelog_topics=use_changelog_topics,
            topic_manager=topic_manager,
        )

    return factory


@pytest.fixture()
def state_manager_factory(tmp_path):
    def factory(
        group_id: Optional[str] = None,
        state_dir: Optional[str] = None,
        changelog_manager: Optional[ChangelogManager] = None,
        recovery_manager: Optional[RecoveryManager] = None,
    ) -> StateStoreManager:
        group_id = group_id or str(uuid.uuid4())
        state_dir = state_dir or str(uuid.uuid4())
        return StateStoreManager(
            group_id=group_id,
            state_dir=str(tmp_path / state_dir),
            changelog_manager=changelog_manager,
            recovery_manager=recovery_manager,
        )

    return factory


@pytest.fixture()
def state_manager(state_manager_factory) -> StateStoreManager:
    manager = state_manager_factory()
    manager.init()
    yield manager
    manager.close()


@pytest.fixture()
def changelog_manager_factory(
    topic_manager_factory,
    row_producer_factory,
):
    def factory(
        topic_admin: Optional[TopicAdmin] = None,
        producer: RowProducer = row_producer_factory(),
    ):
        changelog_manager = ChangelogManager(
            topic_manager=topic_manager_factory(topic_admin),
            producer=producer,
        )
        return changelog_manager

    return factory


@pytest.fixture()
def state_manager_changelogs(
    state_manager_factory,
    topic_admin,
    changelog_manager_factory,
    recovery_manager_mock_consumer,
) -> StateStoreManager:
    manager = state_manager_factory(
        changelog_manager=changelog_manager_factory(topic_admin=topic_admin),
        recovery_manager=recovery_manager_mock_consumer,
    )
    manager.init()
    yield manager
    manager.close()


@pytest.fixture()
def quix_mock_config_builder_factory(kafka_container):
    def factory(workspace_id: Optional[str] = None):
        if not workspace_id:
            workspace_id = "my_ws"
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
        cfg_builder._workspace_id = workspace_id
        cfg_builder.workspace_id = workspace_id
        cfg_builder.get_confluent_broker_config.side_effect = lambda: {
            "bootstrap.servers": kafka_container.broker_address
        }
        # Slight change to ws stuff in case you pass a blank workspace (which makes
        #  some things easier
        cfg_builder.prepend_workspace_id.side_effect = (
            lambda s: prepend_workspace_id(workspace_id, s) if workspace_id else s
        )
        cfg_builder.strip_workspace_id_prefix.side_effect = (
            lambda s: strip_workspace_id_prefix(workspace_id, s) if workspace_id else s
        )
        return cfg_builder

    return factory


@pytest.fixture()
def quix_topic_manager_factory(
    quix_mock_config_builder_factory, topic_admin, topic_manager_factory
):
    """
    Allows for creating topics with a test cluster while keeping the workspace aspects
    """

    def factory(workspace_id: Optional[str] = None):
        topic_manager = topic_manager_factory(topic_admin)
        quix_topic_manager = QuixTopicManager(
            topic_admin=topic_admin,
            quix_config_builder=quix_mock_config_builder_factory(
                workspace_id=workspace_id
            ),
        )
        quix_topic_manager._create_topics = topic_manager._create_topics
        patcher = patch.object(quix_topic_manager, "_topic_replication", 1)
        patcher.start()
        return quix_topic_manager

    return factory


@pytest.fixture()
def quix_app_factory(
    random_consumer_group,
    kafka_container,
    tmp_path,
    topic_admin,
    quix_mock_config_builder_factory,
    quix_topic_manager_factory,
):
    """
    For doing testing with Application.Quix() against a local cluster.

    Almost all behavior is standard, except the quix_config_builder is mocked out, and
    thus topic creation is handled with the TopicAdmin client.
    """

    def factory(
        auto_offset_reset: AutoOffsetReset = "latest",
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        state_dir: Optional[str] = None,
        auto_create_topics: bool = True,
        use_changelog_topics: bool = True,
        workspace_id: str = "my_ws",
    ) -> Application:
        state_dir = state_dir or (tmp_path / "state").absolute()
        return Application.Quix(
            consumer_group=random_consumer_group,
            state_dir=state_dir,
            quix_config_builder=quix_mock_config_builder_factory(
                workspace_id=workspace_id
            ),
            auto_offset_reset=auto_offset_reset,
            consumer_extra_config=consumer_extra_config,
            producer_extra_config=producer_extra_config,
            on_consumer_error=on_consumer_error,
            on_producer_error=on_producer_error,
            on_processing_error=on_processing_error,
            on_message_processed=on_message_processed,
            auto_create_topics=auto_create_topics,
            use_changelog_topics=use_changelog_topics,
            topic_manager=quix_topic_manager_factory(workspace_id=workspace_id),
        )

    return factory


@pytest.fixture()
def message_context_factory():
    def factory(key: object = "test", timestamp_ms: int = 0) -> MessageContext:
        timestamp_type = 0 if timestamp_ms == 0 else 1
        return MessageContext(
            key=key,
            topic="test",
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(timestamp_type, timestamp_ms),
        )

    return factory


@pytest.fixture()
def topic_admin(kafka_container):
    return TopicAdmin(broker_address=kafka_container.broker_address)


@pytest.fixture()
def topic_manager_factory(topic_admin):
    """
    TopicManager with option to add an TopicAdmin (which uses Kafka Broker)
    """

    def factory(
        topic_admin_: Optional[TopicAdmin] = None, create_timeout: int = 10
    ) -> TopicManager:
        return TopicManager(
            topic_admin=topic_admin_ or topic_admin, create_timeout=create_timeout
        )

    return factory


@pytest.fixture()
def topic_manager_topic_factory(topic_manager_factory):
    """
    Uses TopicManager to generate a Topic, create it, and return the Topic object
    """

    def factory(
        name: Optional[str] = str(uuid.uuid4()),
        partitions: int = 1,
        create_topic: bool = True,
        key_serializer: Optional[Union[Serializer, str]] = None,
        value_serializer: Optional[Union[Serializer, str]] = None,
        key_deserializer: Optional[Union[Deserializer, str]] = None,
        value_deserializer: Optional[Union[Deserializer, str]] = None,
    ):
        topic_manager = topic_manager_factory()
        topic_args = {
            "key_serializer": key_serializer,
            "value_serializer": value_serializer,
            "key_deserializer": key_deserializer,
            "value_deserializer": value_deserializer,
            "config": topic_manager.topic_config(num_partitions=partitions),
        }
        topic = topic_manager.topic(
            name, **{k: v for k, v in topic_args.items() if v is not None}
        )
        if create_topic:
            topic_manager.create_all_topics()
        return topic

    return factory
