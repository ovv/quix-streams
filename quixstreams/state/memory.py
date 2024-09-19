from typing import Optional, Dict

from quixstreams.state.recovery import ChangelogProducerFactory
from quixstreams.state.types import Store


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
        self._partitions = {}

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

        self._partitions[partition] = {}

    def revoke_partition(self, partition: int):
        """
        Revoke assigned store partition

        :param partition: partition number
        """
        ...

    def start_partition_transaction(self, partition: int) -> "PartitionTransaction":
        """
        Start a new partition transaction.

        `PartitionTransaction` is the primary interface for working with data in Stores.
        :param partition: partition number
        :return: instance of `PartitionTransaction`
        """

    def close(self):
        """
        Close store and revoke all store partitions
        """
        ...
