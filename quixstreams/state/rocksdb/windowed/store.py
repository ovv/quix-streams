from typing import Optional, cast

from .partition import WindowedRocksDBStorePartition
from .transaction import WindowedRocksDBPartitionTransaction
from ..store import RocksDBStore
from ..types import RocksDBOptionsType
from ...recovery import ChangelogManager, ChangelogProducer


class WindowedRocksDBStore(RocksDBStore):
    """
    RocksDB-based windowed state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    def __init__(
        self,
        name: str,
        topic: str,
        base_dir: str,
        changelog_manager: Optional[ChangelogManager] = None,
        options: Optional[RocksDBOptionsType] = None,
    ):
        """
        :param name: a unique store name
        :param topic: a topic name for this store
        :param base_dir: path to a directory with the state
        :param changelog_manager: if using changelogs, a ChangelogManager instance
        :param options: RocksDB options. If `None`, the default options will be used.
        """
        super().__init__(
            name=name,
            topic=topic,
            base_dir=base_dir,
            changelog_manager=changelog_manager,
            options=options,
        )

    def create_new_partition(
        self, path: str, changelog_producer: Optional[ChangelogProducer] = None
    ) -> WindowedRocksDBStorePartition:
        db_partition = WindowedRocksDBStorePartition(
            path=path, options=self._options, changelog_producer=changelog_producer
        )
        return db_partition

    def assign_partition(self, partition: int) -> WindowedRocksDBStorePartition:
        return cast(
            WindowedRocksDBStorePartition, super().assign_partition(partition=partition)
        )

    def start_partition_transaction(
        self, partition: int
    ) -> WindowedRocksDBPartitionTransaction:
        return cast(
            WindowedRocksDBPartitionTransaction,
            super().start_partition_transaction(partition=partition),
        )
