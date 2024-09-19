import logging

from typing import Optional, Dict

from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.state.exceptions import PartitionNotAssignedError
from quixstreams.state.recovery import ChangelogProducerFactory
from quixstreams.state.types import Store, StorePartition, PartitionTransaction

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

        store_partition = MemoryStorePartition()
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

    def __init__(self) -> None:
        self._state = {}
        self._processed_offset = None
        self._changelog_offset = None

    def begin(self) -> "MemoryStoreTransaction":
        """
        Start new `PartitionTransaction`
        """
        return MemoryStoreTransaction(self._state)

    def recover_from_changelog_message(
        self, changelog_message: ConfluentKafkaMessageProto, committed_offset: int
    ):
        """
        Updates state from a given changelog message.

        :param changelog_message: A raw Confluent message read from a changelog topic.
        :param committed_offset: latest committed offset for the partition
        """
        ...

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
    pass
