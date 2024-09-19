import json
import logging

from typing import Optional, Dict, Any, Tuple

from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.state.exceptions import PartitionNotAssignedError
from quixstreams.state.recovery import ChangelogProducerFactory, ChangelogProducer
from quixstreams.state.types import (
    Store,
    StorePartition,
    PartitionTransaction,
    PartitionTransactionStatus,
)

from quixstreams.state.rocksdb.metadata import PREFIX_SEPARATOR
from quixstreams.state.rocksdb.serialization import serialize, deserialize

logger = logging.getLogger(__name__)


class MemoryStore(Store):

    def __init__(
        self,
        name: str,
        topic: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
    ) -> None:
        super().__init__()

        self._name = name
        self._topic = topic
        self._partitions: Dict[int, MemoryStorePartition] = {}

        self._changelog_producer_factory = changelog_producer_factory

    @property
    def topic(self) -> str:
        """
        Topic name
        """
        return self._topic

    @property
    def name(self) -> str:
        """
        Store name
        """
        return self._name

    @property
    def partitions(self) -> Dict[int, "StorePartition"]:
        """
        Mapping of assigned store partitions
        :return: dict of "{partition: <StorePartition>}"
        """
        return self._partitions

    def assign_partition(self, partition: int) -> "StorePartition":
        """
        Assign new store partition

        :param partition: partition number
        :return: instance of `StorePartition`
        """
        if partition in self._partitions:
            return self._partitions[partition]

        store_partition = MemoryStorePartition(
            changelog_producer=ChangelogProducerFactory(self._topic, partition)
        )
        self._partitions[partition] = store_partition
        return store_partition

    def revoke_partition(self, partition: int):
        """
        Revoke assigned store partition

        :param partition: partition number
        """
        partition = self._partitions.pop(partition, None)
        if partition is None:
            return

        partition.close()
        logger.debug(
            'Revoked store partition "%s[%s]" topic("%s")',
            self._name,
            partition,
            self._topic,
        )

    def start_partition_transaction(self, partition: int) -> "MemoryStoreTransaction":
        """
        Start a new partition transaction.

        `PartitionTransaction` is the primary interface for working with data in Stores.
        :param partition: partition number
        :return: instance of `PartitionTransaction`
        """
        partition = self._partitions.get(partition)
        if partition is None:
            raise PartitionNotAssignedError(
                f'Store partition "{self._name}[{partition}]" '
                f'(topic "{self._topic}") is not assigned'
            )

        return partition.begin()

    def close(self):
        """
        Close store and revoke all store partitions
        """
        logger.debug(f'Closing store "{self._name}" (topic "{self._topic}")')
        partitions = list(self._partitions.keys())
        for partition in partitions:
            self.revoke_partition(partition)
        logger.debug(f'Closed store "{self._name}" (topic "{self._topic}")')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MemoryStorePartition(StorePartition):

    def __init__(self, changelog_producer: ChangelogProducer) -> None:
        self._state = {}
        self._processed_offset = None
        self._changelog_offset = None

        self._changelog_producer = changelog_producer

    def begin(self) -> "MemoryStoreTransaction":
        """
        Start new `PartitionTransaction`
        """
        return MemoryStoreTransaction(self._changelog_producer, self._state)

    def recover_from_changelog_message(
        self, changelog_message: ConfluentKafkaMessageProto, committed_offset: int
    ):
        """
        Updates state from a given changelog message.

        :param changelog_message: A raw Confluent message read from a changelog topic.
        :param committed_offset: latest committed offset for the partition
        """
        self._changelog_offset = changelog_message.offset()
        value = deserialize(changelog_message.value(), json.loads)

        self._state.update(value["state"])
        self._processed_offset = value["processed_offset"]

    def get_processed_offset(self) -> Optional[int]:
        """
        Get last processed offset for the given partition
        :return: offset or `None` if there's no processed offset yet
        """
        return self._processed_offset

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        return self._changelog_offset


class MemoryStoreTransaction(PartitionTransaction):

    def __init__(self, changelog_producer: ChangelogProducer, state: Dict):
        self._state = state
        self._transaction_state = {}
        self._changelog_producer = changelog_producer

        self._dumps = json.dumps
        self._loads = json.loads

        self._status = PartitionTransactionStatus.STARTED

    def as_state(self, prefix: Any) -> State:
        """
        Create an instance implementing the `State` protocol to be provided
        to `StreamingDataFrame` functions.
        All operations called on this State object will be prefixed with
        the supplied `prefix`.

        :return: an instance implementing the `State` protocol
        """
        ...

    def get(self, key: Any, prefix: bytes, default: Any = None) -> Optional[Any]:
        """
        Get the value for key if key is present in the state, else default

        :param key: key
        :param prefix: a key prefix
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """
        skey = self._serialize_key(key, prefix)

        try:
            return self._transaction_state[skey]
        except KeyError:
            pass

        try:
            return self._deserialize_value(self._state[skey])
        except KeyError:
            return None

    def set(self, key: Any, prefix: bytes, value: Any):
        """
        Set value for the key.
        :param key: key
        :param prefix: a key prefix
        :param value: value
        """
        self._transaction_state[self._serialize_key(key, prefix)] = value

    def delete(self, key: Any, prefix: bytes):
        """
        Delete value for the key.

        This function always returns `None`, even if value is not found.
        :param key: key
        :param prefix: a key prefix
        """
        self._transaction_state[self._serialize_key(key, prefix)] = None

    def exists(self, key: Any, prefix: bytes) -> bool:
        """
        Check if the key exists in state.
        :param key: key
        :param prefix: a key prefix
        :return: True if key exists, False otherwise
        """

        skey = self._serialize_key(key, prefix)
        return skey in self._transaction_state or skey in self._state

    def _serialize_value(self, value: Any) -> bytes:
        return serialize(value, dumps=self._dumps)

    def _deserialize_value(self, value: bytes) -> Any:
        return deserialize(value, loads=self._loads)

    def _serialize_key(self, key: Any, prefix: bytes) -> bytes:
        key_bytes = serialize(key, dumps=self._dumps)
        prefix = prefix + PREFIX_SEPARATOR if prefix else b""
        return prefix + key_bytes

    @property
    def failed(self) -> bool:
        """
        Return `True` if transaction failed to update data at some point.

        Failed transactions cannot be re-used.
        :return: bool
        """
        return self._status == PartitionTransactionStatus.FAILED

    @property
    def completed(self) -> bool:
        """
        Return `True` if transaction is successfully completed.

        Completed transactions cannot be re-used.
        :return: bool
        """
        return self._status == PartitionTransactionStatus.COMPLETE

    @property
    def prepared(self) -> bool:
        """
        Return `True` if transaction is prepared completed.

        Prepared transactions cannot receive new updates, but can be flushed.
        :return: bool
        """
        return self._status == PartitionTransactionStatus.PREPARED

    def prepare(self, processed_offset: int):
        """
        Produce changelog messages to the changelog topic for all changes accumulated
        in this transaction and prepare transcation to flush its state to the state
        store.

        After successful `prepare()`, the transaction status is changed to PREPARED,
        and it cannot receive updates anymore.

        If changelog is disabled for this application, no updates will be produced
        to the changelog topic.

        :param processed_offset: the offset of the latest processed message
        """
        try:
            self._produce_changelog(processed_offset=processed_offset)
            self._status = PartitionTransactionStatus.PREPARED
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    def _produce_changelog(self, processed_offset: int):
        logger.debug(
            f"Flushing state changes to the changelog topic "
            f'topic_name="{self._changelog_producer.changelog_name}" '
            f"partition={self._changelog_producer.partition} "
            f"processed_offset={processed_offset}"
        )

        self._changelog_producer.produce(
            value=self._serialize_value(
                {"state": self._transaction_state, "processed_offset": processed_offset}
            )
        )

    @property
    def changelog_topic_partition(self) -> Optional[Tuple[str, int]]:
        """
        Return the changelog topic-partition for the StorePartition of this transaction.

        Returns `None` if changelog_producer is not provided.

        :return: (topic, partition) or None
        """
        return (self._topic, self._partition)

    def flush(
        self,
        processed_offset: Optional[int] = None,
        changelog_offset: Optional[int] = None,
    ):
        """
        Flush the recent updates to the storage.

        :param processed_offset: offset of the last processed message, optional.
        :param changelog_offset: offset of the last produced changelog message,
            optional.
        """
        pass

    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None and not self.failed:
            self.flush()
