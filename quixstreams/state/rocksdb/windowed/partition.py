import logging
from typing import Optional

from rocksdict import ReadOptions, RdictItems  # type: ignore

from .transaction import WindowedRocksDBPartitionTransaction
from .. import ColumnFamilyDoesNotExist
from ..metadata import (
    METADATA_CF_NAME,
    LATEST_TIMESTAMP_KEY,
)
from ..partition import (
    RocksDBStorePartition,
)
from ..serialization import int_from_int64_bytes
from ..types import RocksDBOptionsType

from ...recovery import ChangelogProducer

logger = logging.getLogger(__name__)


class WindowedRocksDBStorePartition(RocksDBStorePartition):
    """
    A base class to access windowed state in RocksDB.
    It represents a single RocksDB database.

    Besides the data, it keeps track of the latest observed timestamp and
    stores the expiration index to delete expired windows.

    :param path: an absolute path to the RocksDB folder
    :param options: RocksDB options. If `None`, the default options will be used.
    """

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        super().__init__(
            path=path, options=options, changelog_producer=changelog_producer
        )
        self._latest_timestamp_ms = self._get_latest_timestamp_from_db()
        self._expiration_index_cf_name = f"__expiration-index__"
        self._ensure_column_family(self._expiration_index_cf_name)

    @property
    def expiration_index_cf_name(self) -> str:
        return self._expiration_index_cf_name

    def iter_items(
        self, from_key: bytes, read_opt: ReadOptions, cf_name: str = "default"
    ) -> RdictItems:
        cf = self.get_column_family(cf_name=cf_name)
        return cf.items(from_key=from_key, read_opt=read_opt)

    def begin(
        self, changelog_writer: Optional[ChangelogProducer] = None
    ) -> "WindowedRocksDBPartitionTransaction":
        """
        Start a WindowedTransaction.

        :param changelog_writer: If using changelogs, a ChangelogWriter instance
        :return: a WindowedRocksDBPartitionTransaction instance
        """
        return WindowedRocksDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            latest_timestamp_ms=self._latest_timestamp_ms,
        )

    def set_latest_timestamp(self, timestamp_ms: int):
        self._latest_timestamp_ms = timestamp_ms

    def _get_latest_timestamp_from_db(self) -> int:
        value = self.get(LATEST_TIMESTAMP_KEY, cf_name=METADATA_CF_NAME)
        if value is None:
            return 0
        return int_from_int64_bytes(value)

    def _ensure_column_family(self, cf_name: str):
        try:
            self.get_column_family(cf_name)
        except ColumnFamilyDoesNotExist:
            self.create_column_family(cf_name)
