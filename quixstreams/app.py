import contextlib
import logging
import signal
from typing import Optional, List, Callable, Literal, Mapping

from confluent_kafka import TopicPartition
from typing_extensions import Self

from .context import set_message_context, copy_context
from .core.stream import Filtered
from .dataframe import StreamingDataFrame
from .error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
    default_on_processing_error,
)
from .kafka import AutoOffsetReset, AssignmentStrategy, Partitioner, Admin
from .logging import configure_logging, LogLevel
from .models import (
    Topic,
    TopicConfig,
    SerializerType,
    DeserializerType,
)
from .platforms.quix import (
    QuixKafkaConfigsBuilder,
    check_state_dir,
    check_state_management_enabled,
)
from .rowconsumer import RowConsumer
from .rowproducer import RowProducer
from .state import StateStoreManager, ChangelogManager
from .state.rocksdb import RocksDBOptionsType
from .topic_manager import TopicManager, TopicManagerType

__all__ = ("Application",)

logger = logging.getLogger(__name__)
MessageProcessedCallback = Callable[[str, int, int], None]


class Application:
    """
    The main Application class.

    Typically, the primary object needed to get a kafka application up and running.

    Most functionality is explained the various methods, except for
    "column assignment".


    What it Does:

    - During user setup:
        - Provides defaults or helper methods for commonly needed objects
        - For Quix Platform Users: Configures the app for it
            (see `Application.Quix()`)
    - When executed via `.run()` (after setup):
        - Initializes Topics and StreamingDataFrames
        - Facilitates processing of Kafka messages with a `StreamingDataFrame`
        - Handles all Kafka client consumer/producer responsibilities.


    Example Snippet:

    ```python
    from quixstreams import Application

    # Set up an `app = Application` and  `sdf = StreamingDataFrame`;
    # add some operations to `sdf` and then run everything.

    app = Application(broker_address='localhost:9092', consumer_group='group')
    topic = app.topic('test-topic')
    df = app.dataframe(topic)
    df.apply(lambda value, context: print('New message', value))

    app.run(dataframe=df)
    ```
    """

    def __init__(
        self,
        broker_address: str,
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        assignment_strategy: AssignmentStrategy = "range",
        partitioner: Partitioner = "murmur2",
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        state_dir: str = "state",
        rocksdb_options: Optional[RocksDBOptionsType] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
        loglevel: Optional[LogLevel] = "INFO",
        auto_create_topics: bool = True,
        use_changelog_topics: bool = True,
        topic_validation: Optional[Literal["exists", "required", "all"]] = "exists",
        topic_manager: Optional[TopicManagerType] = None,
    ):
        """

        :param broker_address: Kafka broker host and port in format `<host>:<port>`.
            Passed as `bootstrap.servers` to `confluent_kafka.Consumer`.
        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`
        :param auto_offset_reset: Consumer `auto.offset.reset` setting
        :param auto_commit_enable: If true, periodically commit offset of
            the last message handed to the application. Default - `True`.
        :param assignment_strategy: The name of a partition assignment strategy.
        :param partitioner: A function to be used to determine the outgoing message
            partition.
        :param consumer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
        :param producer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Producer` as is.
        :param state_dir: path to the application state directory.
            Default - ".state".
        :param rocksdb_options: RocksDB options.
            If `None`, the default options will be used.
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`. Default - 1.0s
        :param producer_poll_timeout: timeout for `RowProducer.poll()`. Default - 0s.
        :param on_message_processed: a callback triggered when message is successfully
            processed.
        :param loglevel: a log level for "quixstreams" logger.
            Should be a string or None.
            If `None` is passed, no logging will be configured.
            You may pass `None` and configure "quixstreams" logger
            externally using `logging` library.
            Default - "INFO".
        :param auto_create_topics: Create all `Topic`s made via Application.topic()
            Default - `True`
        :param use_changelog_topics: Use changelog topics to back stateful operations
            Default - `True`
        :param topic_validation: The degree of topic validation; Default - "exists"
            None - No validation.
            "exists" - Confirm expected topics exist.
            "required" - Confirm topics match your provided
                `Topic` partition + replication factor
            "all" - Confirm topic settings are EXACT.
        :param topic_manager: A TopicManager instance

        ***Error Handlers***

        To handle errors, `Application` accepts callbacks triggered when
            exceptions occur on different stages of stream processing. If the callback
            returns `True`, the exception will be ignored. Otherwise, the exception
            will be propagated and the processing will eventually stop.
        :param on_consumer_error: triggered when internal `RowConsumer` fails
        to poll Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when RowProducer fails to serialize
            or to produce a message to Kafka.
        """
        configure_logging(loglevel=loglevel)
        self._consumer = RowConsumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            assignment_strategy=assignment_strategy,
            extra_config=consumer_extra_config,
            on_error=on_consumer_error,
        )
        self._producer = RowProducer(
            broker_address=broker_address,
            partitioner=partitioner,
            extra_config=producer_extra_config,
            on_error=on_producer_error,
        )
        self._consumer_poll_timeout = consumer_poll_timeout
        self._producer_poll_timeout = producer_poll_timeout
        self._running = False
        self._on_processing_error = on_processing_error or default_on_processing_error
        self._on_message_processed = on_message_processed
        self._quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None
        self._auto_create_topics = auto_create_topics
        self._topic_validation = topic_validation

        if not topic_manager:
            topic_manager = TopicManager()
        if not topic_manager.has_admin:
            topic_manager.set_admin(
                Admin(
                    broker_address=broker_address,
                    extra_config=producer_extra_config,
                )
            )
        self._topic_manager = topic_manager

        changelog_manager = (
            ChangelogManager(
                topic_manager=self._topic_manager,
                producer=RowProducer(
                    broker_address=broker_address,
                    partitioner=partitioner,
                    extra_config=producer_extra_config,
                    on_error=on_producer_error,
                ),
            )
            if use_changelog_topics
            else None
        )
        self._state_manager = StateStoreManager(
            group_id=consumer_group,
            state_dir=state_dir,
            rocksdb_options=rocksdb_options,
            changelog_manager=changelog_manager,
        )

    def _set_quix_config_builder(self, config_builder: QuixKafkaConfigsBuilder):
        self._quix_config_builder = config_builder

    @classmethod
    def Quix(
        cls,
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        assignment_strategy: AssignmentStrategy = "range",
        partitioner: Partitioner = "murmur2",
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        state_dir: str = "state",
        rocksdb_options: Optional[RocksDBOptionsType] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
        loglevel: Optional[LogLevel] = "INFO",
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
        auto_create_topics: bool = True,
        use_changelog_topics: bool = True,
        topic_validation: Optional[Literal["exists", "required", "all"]] = "exists",
        topic_manager: Optional[TopicManager.Quix] = None,
    ) -> Self:
        """
        Initialize an Application to work with Quix platform,
        assuming environment is properly configured (by default in the platform).

        It takes the credentials from the environment and configures consumer and
        producer to properly connect to the Quix platform.

        >***NOTE:*** Quix platform requires `consumer_group` and topic names to be
            prefixed with workspace id.
            If the application is created via `Application.Quix()`, the real consumer
            group will be `<workspace_id>-<consumer_group>`,
            and the real topic names will be `<workspace_id>-<topic_name>`.



        Example Snippet:

        ```python
        from quixstreams import Application

        # Set up an `app = Application.Quix` and  `sdf = StreamingDataFrame`;
        # add some operations to `sdf` and then run everything. Also shows off how to
        # use the quix-specific serializers and deserializers.

        app = Application.Quix()
        input_topic = app.topic("topic-in", value_deserializer="quix")
        output_topic = app.topic("topic-out", value_serializer="quix_timeseries")
        df = app.dataframe(topic_in)
        df = df.to_topic(output_topic)

        app.run(dataframe=df)
        ```

        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`.
              >***NOTE:*** The consumer group will be prefixed by Quix workspace id.
        :param auto_offset_reset: Consumer `auto.offset.reset` setting
        :param auto_commit_enable: If true, periodically commit offset of
            the last message handed to the application. Default - `True`.
        :param assignment_strategy: The name of a partition assignment strategy.
        :param partitioner: A function to be used to determine the outgoing message
            partition.
        :param consumer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
        :param producer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Producer` as is.
        :param state_dir: path to the application state directory.
            Default - ".state".
        :param rocksdb_options: RocksDB options.
            If `None`, the default options will be used.
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`. Default - 1.0s
        :param producer_poll_timeout: timeout for `RowProducer.poll()`. Default - 0s.
        :param on_message_processed: a callback triggered when message is successfully
            processed.
        :param loglevel: a log level for "quixstreams" logger.
            Should be a string or None.
            If `None` is passed, no logging will be configured.
            You may pass `None` and configure "quixstreams" logger
            externally using `logging` library.
            Default - "INFO".
        :param auto_create_topics: Create all `Topic`s made via Application.topic()
            Default - `True`
        :param use_changelog_topics: Use changelog topics to back stateful operations
            Default - `True`
        :param topic_validation: The degree of topic validation; Default - "exists"
            None - No validation.
            "exists" - Confirm expected topics exist.
            "required" - Confirm topics match your provided `Topic`
                partition + replication factor
            "all" - Confirm topic settings are EXACT.
        :param topic_manager: A QuixTopicManager instance

        ***Error Handlers***

        To handle errors, `Application` accepts callbacks triggered when
            exceptions occur on different stages of stream processing. If the callback
            returns `True`, the exception will be ignored. Otherwise, the exception
            will be propagated and the processing will eventually stop.
        :param on_consumer_error: triggered when internal `RowConsumer` fails to poll
            Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when RowProducer fails to serialize
            or to produce a message to Kafka.


        ***Quix-specific Parameters***

        :param quix_config_builder: instance of `QuixKafkaConfigsBuilder` to be used
            instead of the default one.

        :return: `Application` object
        """
        configure_logging(loglevel=loglevel)
        quix_config_builder = quix_config_builder or QuixKafkaConfigsBuilder()
        quix_configs = quix_config_builder.get_confluent_broker_config()

        # Check if the state dir points to the mounted PVC while running on Quix
        # Otherwise, the state won't be shared and replicas won't be able to
        # recover the same state.
        check_state_dir(state_dir=state_dir)

        broker_address = quix_configs.pop("bootstrap.servers")
        # Quix platform prefixes consumer group with workspace id
        consumer_group = quix_config_builder.prepend_workspace_id(consumer_group)
        consumer_extra_config = {**quix_configs, **(consumer_extra_config or {})}
        producer_extra_config = {**quix_configs, **(producer_extra_config or {})}
        app = cls(
            broker_address=broker_address,
            consumer_group=consumer_group,
            consumer_extra_config=consumer_extra_config,
            producer_extra_config=producer_extra_config,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            assignment_strategy=assignment_strategy,
            partitioner=partitioner,
            on_consumer_error=on_consumer_error,
            on_processing_error=on_processing_error,
            on_producer_error=on_producer_error,
            on_message_processed=on_message_processed,
            consumer_poll_timeout=consumer_poll_timeout,
            producer_poll_timeout=producer_poll_timeout,
            state_dir=state_dir,
            rocksdb_options=rocksdb_options,
            auto_create_topics=auto_create_topics,
            use_changelog_topics=use_changelog_topics,
            topic_validation=topic_validation,
            topic_manager=topic_manager
            or TopicManager.Quix(quix_config_builder=quix_config_builder),
        )
        app._set_quix_config_builder(quix_config_builder)
        return app

    @property
    def is_quix_app(self) -> bool:
        return self._quix_config_builder is not None

    def topic(
        self,
        name: str,
        value_deserializer: DeserializerType = "json",
        key_deserializer: DeserializerType = "bytes",
        value_serializer: SerializerType = "json",
        key_serializer: SerializerType = "bytes",
        config: Optional[TopicConfig] = None,
    ) -> Topic:
        """
        Create a topic definition.

        Allows you to specify serialization that should be used when consuming/producing
        to the topic in the form of a string name (i.e. "json" for JSON) or a
        serialization class instance directly, like JSONSerializer().


        Example Snippet:

        ```python
        from quixstreams import Application

        # Specify an input and output topic for a `StreamingDataFrame` instance,
        # where the output topic requires adjusting the key serializer.

        app = Application()
        input_topic = app.topic("input-topic", value_deserializer="json")
        output_topic = app.topic(
            "output-topic", key_serializer="str", value_serializer=JSONSerializer()
        )
        sdf = app.dataframe(input_topic)
        sdf.to_topic(output_topic)
        ```

        :param name: topic name
            >***NOTE:*** If the application is created via `Quix.Application()`,
              the topic name will be prefixed by Quix workspace id, and it will
              be `<workspace_id>-<name>`
        :param value_deserializer: a deserializer type for values; default="json"
        :param key_deserializer: a deserializer type for keys; default="bytes"
        :param value_serializer: a serializer type for values; default="json"
        :param key_serializer: a serializer type for keys; default="bytes"
        :param config: optional topic configurations (for creation/validation)
            >***NOTE:*** will not create without Application's auto_create_topics set
            to True (is True by default)

        :return: `Topic` object
        """
        return self._topic_manager.topic(
            name=name,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            config=config,
            auto_create_config=self._auto_create_topics,
        )

    def topic_config(
        self,
        name: str,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        extra_config: Optional[Mapping] = None,
    ) -> TopicConfig:
        """
        Convenience method for generating a `TopicConfig` with default settings

        :param name: the topic name
        :param num_partitions: the number of topic partitions
        :param replication_factor: the topic replication factor
        :param extra_config: other optional configuration settings

        :return: a TopicConfig object
        """
        return self._topic_manager.topic_config(
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            extra_config=extra_config,
        )

    def dataframe(
        self,
        topic: Topic,
    ) -> StreamingDataFrame:
        """
        A simple helper method that generates a `StreamingDataFrame`, which is used
        to define your message processing pipeline.

        See :class:`quixstreams.dataframe.StreamingDataFrame` for more details.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Set up an `app = Application` and  `sdf = StreamingDataFrame`;
        # add some operations to `sdf` and then run everything.

        app = Application(broker_address='localhost:9092', consumer_group='group')
        topic = app.topic('test-topic')
        df = app.dataframe(topic)
        df.apply(lambda value, context: print('New message', value)

        app.run(dataframe=df)
        ```


        :param topic: a `quixstreams.models.Topic` instance
            to be used as an input topic.
        :return: `StreamingDataFrame` object
        """
        sdf = StreamingDataFrame(topic=topic, state_manager=self._state_manager)
        sdf.producer = self._producer
        return sdf

    def stop(self):
        """
        Stop the internal poll loop and the message processing.

        Only necessary when manually managing the lifecycle of the `Application` (
        likely through some sort of threading).

        To otherwise stop an application, either send a `SIGTERM` to the process
        (like Kubernetes does) or perform a typical `KeyboardInterrupt` (`Ctrl+C`).
        """
        self._running = False

    def clear_state(self):
        """
        Clear the state of the application.
        """
        self._state_manager.clear_stores()

    def _quix_runtime_init(self):
        """
        Do a runtime setup only applicable to an Application.Quix instance
        - Ensure that "State management" flag is enabled for deployment if the app
          is stateful and is running on Quix platform
        """
        # Ensure that state management is enabled if application is stateful
        # and is running on Quix platform
        if self._state_manager.stores:
            check_state_management_enabled()

    def _setup_topics(self):
        logger.info(
            "topics (with provided configs) required for app operation:\n"
            f"{self._topic_manager.pretty_formatted_topic_configs}"
        )
        if self._auto_create_topics:
            logger.info("Auto-create topics enabled. Initializing topic creation...")
            self._topic_manager.create_all_topics()
        if self._topic_validation:
            logger.info("Topic validation enabled. Initializing topic validation...")
            self._topic_manager.validate_all_topics(
                validation_level=self._topic_validation
            )

    def run(
        self,
        dataframe: StreamingDataFrame,
    ):
        """
        Start processing data from Kafka using provided `StreamingDataFrame`

        One started, can be safely terminated with a `SIGTERM` signal
        (like Kubernetes does) or a typical `KeyboardInterrupt` (`Ctrl+C`).


        Example Snippet:

        ```python
        from quixstreams import Application

        # Set up an `app = Application` and  `sdf = StreamingDataFrame`;
        # add some operations to `sdf` and then run everything.

        app = Application(broker_address='localhost:9092', consumer_group='group')
        topic = app.topic('test-topic')
        df = app.dataframe(topic)
        df.apply(lambda value, context: print('New message', value)

        app.run(dataframe=df)
        ```

        :param dataframe: instance of `StreamingDataFrame`
        """
        self._setup_signal_handlers()

        logger.info("Initializing processing of StreamingDataFrame")
        if self.is_quix_app:
            self._quix_runtime_init()

        self._setup_topics()

        exit_stack = contextlib.ExitStack()
        exit_stack.enter_context(self._producer)
        exit_stack.enter_context(self._consumer)
        exit_stack.enter_context(self._state_manager)

        exit_stack.callback(
            lambda *_: logger.debug("Closing Kafka consumers & producers")
        )
        exit_stack.callback(lambda *_: self.stop())

        if self._state_manager.stores:
            # Store manager has stores registered, use real state transactions
            # during processing
            start_state_transaction = self._state_manager.start_store_transaction
        else:
            # Application is stateless, use dummy state transactions
            start_state_transaction = _dummy_state_transaction

        with exit_stack:
            # Subscribe to topics in Kafka and start polling
            self._consumer.subscribe(
                [dataframe.topic],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
                on_lost=self._on_lost,
            )
            logger.info("Waiting for incoming messages")
            # Start polling Kafka for messages and callbacks
            self._running = True

            dataframe_composed = dataframe.compose()
            while self._running:
                # Serve producer callbacks
                self._producer.poll(self._producer_poll_timeout)
                rows = self._consumer.poll_row(timeout=self._consumer_poll_timeout)

                if rows is None:
                    continue

                # Deserializer may return multiple rows for a single message
                rows = rows if isinstance(rows, list) else [rows]
                if not rows:
                    continue

                first_row = rows[0]
                topic_name, partition, offset = (
                    first_row.topic,
                    first_row.partition,
                    first_row.offset,
                )
                # Create a new contextvars.Context and set the current MessageContext
                # (it's the same across multiple rows)
                context = copy_context()
                context.run(set_message_context, first_row.context)

                with start_state_transaction(
                    topic=topic_name, partition=partition, offset=offset
                ):
                    for row in rows:
                        try:
                            # Execute StreamingDataFrame in a context
                            context.run(dataframe_composed, row.value)
                        except Filtered:
                            # The message was filtered by StreamingDataFrame
                            continue
                        except Exception as exc:
                            # TODO: This callback might be triggered because of Producer
                            #  errors too because they happen within ".process()"
                            to_suppress = self._on_processing_error(exc, row, logger)
                            if not to_suppress:
                                raise

                # Store the message offset after it's successfully processed
                self._consumer.store_offsets(
                    offsets=[
                        TopicPartition(
                            topic=topic_name,
                            partition=partition,
                            offset=offset + 1,
                        )
                    ]
                )

                if self._on_message_processed is not None:
                    self._on_message_processed(topic_name, partition, offset)

            logger.info("Stop processing of StreamingDataFrame")

    def _on_assign(self, _, topic_partitions: List[TopicPartition]):
        """
        Assign new topic partitions to consumer and state.

        :param topic_partitions: list of `TopicPartition` from Kafka
        """
        if self._state_manager.stores:
            logger.debug(f"Rebalancing: assigning state store partitions")
            for tp in topic_partitions:
                # Assign store partitions
                store_partitions = self._state_manager.on_partition_assign(tp)

                # Check if the latest committed offset >= stored offset
                # Otherwise, the re-processed messages might use already updated
                # state, which can lead to inconsistent outputs
                stored_offsets = [
                    offset
                    for offset in (
                        store_tp.get_processed_offset() for store_tp in store_partitions
                    )
                    if offset is not None
                ]
                min_stored_offset = min(stored_offsets) + 1 if stored_offsets else None
                if min_stored_offset is not None:
                    tp_committed = self._consumer.committed([tp], timeout=30)[0]
                    if min_stored_offset > tp_committed.offset:
                        logger.warning(
                            f'Warning: offset "{tp_committed.offset}" '
                            f"for topic partition "
                            f'"{tp_committed.topic}[{tp_committed.partition}]" '
                            f'is behind the stored offset "{min_stored_offset}". '
                            f"It may lead to distortions in produced data."
                        )

    def _on_revoke(self, _, topic_partitions: List[TopicPartition]):
        """
        Revoke partitions from consumer and state
        """
        if self._state_manager.stores:
            logger.debug(f"Rebalancing: revoking state store partitions")
            for tp in topic_partitions:
                self._state_manager.on_partition_revoke(tp)

    def _on_lost(self, _, topic_partitions: List[TopicPartition]):
        """
        Dropping lost partitions from consumer and state
        """
        if self._state_manager.stores:
            logger.debug(f"Rebalancing: dropping lost state store partitions")
            for tp in topic_partitions:
                self._state_manager.on_partition_lost(tp)

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._on_sigint)
        signal.signal(signal.SIGTERM, self._on_sigterm)

    def _on_sigint(self, *_):
        # Re-install the default SIGINT handler so doing Ctrl+C twice
        # raises KeyboardInterrupt
        signal.signal(signal.SIGINT, signal.default_int_handler)
        logger.debug(f"Received SIGINT, stopping the processing loop")
        self.stop()

    def _on_sigterm(self, *_):
        logger.debug(f"Received SIGTERM, stopping the processing loop")
        self.stop()


_nullcontext = contextlib.nullcontext()


def _dummy_state_transaction(topic: str, partition: int, offset: int):
    return _nullcontext
