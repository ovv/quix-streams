import contextlib
import functools
import logging
import struct
import time
from typing import Any, Union, Optional

import rocksdict
from typing_extensions import Self

from quixstreams.state.types import (
    DumpsFunc,
    LoadsFunc,
    PartitionTransaction,
    PartitionRecovery,
    StorePartition,
)
from .exceptions import (
    StateTransactionError,
    NestedPrefixError,
)
from .options import RocksDBOptions
from .serialization import serialize, deserialize, serialize_key
from .types import RocksDBOptionsType
from ..state import TransactionState
from ..changelog import ChangelogWriter

__all__ = (
    "RocksDBStorePartition",
    "RocksDBPartitionTransaction",
)

logger = logging.getLogger(__name__)

_sentinel = object()

_PREFIX_SEPARATOR = b"|"

_DEFAULT_PREFIX = b""

_PROCESSED_OFFSET_KEY = b"__topic_offset__"

_CHANGELOG_OFFSET_KEY = b"__changelog_offset__"


def _int_to_int64_bytes(value: int) -> bytes:
    return struct.pack(">q", value)


def _int_from_int64_bytes(value: bytes) -> int:
    return struct.unpack(">q", value)[0]


class RocksDBStorePartition(StorePartition):
    """
    A base class to access state in RocksDB.
    It represents a single RocksDB database.

    Responsibilities:
     1. Managing access to the RocksDB instance
     2. Creating transactions to interact with data
     3. Flushing WriteBatches to the RocksDB

    It opens the RocksDB on `__init__`. If the db is locked by another process,
    it will retry according to `open_max_retries` and `open_retry_backoff`.

    :param path: an absolute path to the RocksDB folder
    :param options: RocksDB options. If `None`, the default options will be used.
    :param open_max_retries: number of times to retry opening the database
        if it's locked by another process. To disable retrying, pass 0.
    :param open_retry_backoff: number of seconds to wait between each retry.
    """

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        open_max_retries: int = 10,
        open_retry_backoff: float = 3.0,
    ):
        self._path = path
        self._options = options or RocksDBOptions()
        self._open_max_retries = open_max_retries
        self._open_retry_backoff = open_retry_backoff
        self._db = self._init_db()

    def begin(
        self,
        changelog_writer: Optional[ChangelogWriter] = None,
    ) -> "RocksDBPartitionTransaction":
        """
        Create a new `RocksDBTransaction` object.
        Using `RocksDBTransaction` is a recommended way for accessing the data.

        :return: an instance of `RocksDBTransaction`
        """
        return RocksDBPartitionTransaction(
            partition=self,
            dumps=self._options.dumps,
            loads=self._options.loads,
            changelog_writer=changelog_writer,
        )

    def recover(self, offset) -> "RocksDBPartitionRecovery":
        """
        Create a new `RocksDBTransaction` object.
        Using `RocksDBTransaction` is a recommended way for accessing the data.

        :return: an instance of `RocksDBTransaction`
        """
        return RocksDBPartitionRecovery(
            partition=self,
            offset=offset,
        )

    def write(self, batch: rocksdict.WriteBatch):
        """
        Write `WriteBatch` to RocksDB
        :param batch: an instance of `rocksdict.WriteBatch`
        """
        self._db.write(batch)

    def get(self, key: bytes, default: Any = None) -> Union[None, bytes, Any]:
        """
        Get a key from RocksDB.

        :param key: a key encoded to `bytes`
        :param default: a default value to return if the key is not found.
        :return: a value if the key is present in the DB. Otherwise, `None` or `default`
        """
        return self._db.get(key, default)

    def exists(self, key: bytes) -> bool:
        """
        Check if a key is present in the DB.

        :param key: a key encoded to `bytes`.
        :return: `True` if the key is present, `False` otherwise.
        """
        return key in self._db

    def get_processed_offset(self) -> Optional[int]:
        """
        Get last processed offset for the given partition
        :return: offset or `None` if there's no processed offset yet
        """
        offset_bytes = self._db.get(_PROCESSED_OFFSET_KEY)
        if offset_bytes is not None:
            return _int_from_int64_bytes(offset_bytes)

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get last produced offset for the given changelog partition
        :return: offset or `None` if there's no produced offset yet
        """
        # TODO-CF: UNCOMMENT AND REPLACE ALL BELOW
        # metadata_cf = self.get_column_family(METADATA_CF_NAME)
        # offset_bytes = metadata_cf.get(_CHANGELOG_OFFSET_KEY)
        # if offset_bytes is None:
        #     offset_bytes = self._db.get(_CHANGELOG_OFFSET_KEY)
        # return int_from_int64_bytes(offset_bytes) if offset_bytes is not None else 0
        offset_bytes = self._db.get(_CHANGELOG_OFFSET_KEY)
        return _int_from_int64_bytes(offset_bytes) if offset_bytes is not None else 0

    def close(self):
        """
        Close the underlying RocksDB
        """
        logger.debug(f'Closing rocksdb partition on "{self._path}"')
        self._db.close()
        logger.debug(f'Closed rocksdb partition on "{self._path}"')

    @property
    def path(self) -> str:
        """
        Get absolute path to RocksDB database folder
        :return: file path
        """
        return self._path

    @classmethod
    def destroy(cls, path: str):
        """
        Delete underlying RocksDB database

        The database must be closed first.

        :param path: an absolute path to the RocksDB folder
        """
        rocksdict.Rdict.destroy(path=path)  # noqa

    def _init_db(self) -> rocksdict.Rdict:
        db = self._open_rocksdb()
        return db

    def _open_rocksdb(self) -> rocksdict.Rdict:
        attempt = 1
        while True:
            logger.debug(
                f'Opening rocksdb partition on "{self._path}" attempt={attempt}',
            )
            try:
                db = rocksdict.Rdict(
                    path=self._path,
                    options=self._options.to_options(),
                    access_type=rocksdict.AccessType.read_write(),  # noqa
                )
                logger.debug(
                    f'Successfully opened rocksdb partition on "{self._path}"',
                )
                return db
            except Exception as exc:
                is_locked = str(exc).lower().startswith("io error: lock")
                if not is_locked:
                    raise

                if self._open_max_retries <= 0 or attempt >= self._open_max_retries:
                    raise

                logger.warning(
                    f"Failed to open rocksdb partition, cannot acquire a lock. "
                    f"Retrying in {self._open_retry_backoff}sec."
                )

                attempt += 1
                time.sleep(self._open_retry_backoff)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def _validate_transaction_state(func):
    """
    Check that the state of `RocksDBTransaction` is valid before calling a method
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: "RocksDBPartitionTransaction" = args[0]
        if self.failed:
            raise StateTransactionError(
                "Transaction is failed, create a new one to proceed"
            )
        if self.completed:
            raise StateTransactionError(
                "Transaction is already finished, create a new one to proceed"
            )

        return func(*args, **kwargs)

    return wrapper


class RocksDBPartitionTransaction(PartitionTransaction):
    """
    A transaction class to perform simple key-value operations like
    "get", "set", "delete" and "exists" on a single RocksDB partition.

    Serialization
    *************
    `RocksDBTransaction` automatically serializes keys and values to bytes.

    Prefixing
    *********
    `RocksDBTransaction` allows to set prefixes for the keys in the given code block
    using :meth:`with_prefix()` context manager.
    Normally, `StreamingDataFrame` class will use message keys as prefixes
    in order to namespace the stored keys across different messages.

    Transactional properties
    ************************
    `RocksDBTransaction` uses a combination of in-memory update cache
    and RocksDB's WriteBatch in order to accumulate all the state mutations
    in a single batch, flush them atomically, and allow the updates be visible
    within the transaction before it's flushed (aka "read-your-own-writes" problem).

    If any mutation fails during the transaction
    (e.g. we failed to write the updates to the RocksDB), the whole transaction
    will be marked as failed and cannot be used anymore.
    In this case, a new `RocksDBTransaction` should be created.

    `RocksDBTransaction` can be used only once.

    :param partition: instance of `RocksDBStatePartition` to be used for accessing
        the underlying RocksDB
    :param dumps: a function to serialize data to bytes.
    :param loads: a function to deserialize data from bytes.
    """

    __slots__ = (
        "_partition",
        "_update_cache",
        "_delete_cache",
        "_batch",
        "_prefix",
        "_failed",
        "_completed",
        "_dumps",
        "_loads",
        "_changelog_writer",
    )

    def __init__(
        self,
        partition: RocksDBStorePartition,
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_writer: Optional[ChangelogWriter] = None,
    ):
        self._partition = partition
        self._update_cache = {}
        self._delete_cache = set()
        self._batch = rocksdict.WriteBatch(raw_mode=True)
        self._prefix = _DEFAULT_PREFIX
        self._failed = False
        self._completed = False
        self._dumps = dumps
        self._loads = loads
        self._changelog_writer = changelog_writer
        self._state = TransactionState(transaction=self)

    @property
    def state(self) -> TransactionState:
        return self._state

    @contextlib.contextmanager
    def with_prefix(self, prefix: Any = b"") -> Self:
        """
        A context manager set the prefix for all keys in the scope.

        Normally, it's called by Streaming DataFrames engine to ensure that every
        message key is stored separately.

        The `with_prefix` calls should not be nested.
        Only one prefix can be set at a time.

        :param prefix: a prefix string to be used.
            Should be either `bytes` or object serializable to `bytes`
            by `dumps` function.
            The prefix doesn't need to contain the separator, it will be added
            automatically between the key and the prefix if the prefix
            is not empty.
        """
        if self._prefix != _DEFAULT_PREFIX:
            raise NestedPrefixError("The transaction already has a prefix")
        self._prefix = (
            prefix if isinstance(prefix, bytes) else self._serialize_value(prefix)
        )
        if self._prefix:
            self._prefix += _PREFIX_SEPARATOR

        try:
            yield self
        finally:
            self._prefix = _DEFAULT_PREFIX

    @_validate_transaction_state
    def get(self, key: Any, default: Any = None) -> Optional[Any]:
        """
        Get a key from the store.

        It first looks up the key in the update cache in case it has been updated
        but not flushed yet.

        It returns `None` if the key is not found and `default` is not provided.

        :param key: a key to get from DB
        :param default: value to return if the key is not present in the state.
            It can be of any type.
        :return: value or `default`
        """

        # First, check the update cache in case the value was previously written
        # Use sentinel as default because the actual value can be "None"
        key_serialized = self._serialize_key(key)
        cached = self._update_cache.get(key_serialized, _sentinel)
        if cached is not _sentinel:
            return self._deserialize_value(cached)

        # The value is not found in cache, check the db
        stored = self._partition.get(key_serialized, _sentinel)
        if stored is not _sentinel:
            return self._deserialize_value(stored)
        return default

    @_validate_transaction_state
    def set(self, key: Any, value: Any):
        """
        Set a key to the store.

        It first updates the key in the update cache.

        :param key: key to store in DB
        :param value: value to store in DB
        """

        key_serialized = self._serialize_key(key)
        value_serialized = self._serialize_value(value)

        try:
            self._batch.put(key_serialized, value_serialized)
            self._update_cache[key_serialized] = value_serialized
            self._delete_cache.discard(key_serialized)
        except Exception:
            self._failed = True
            raise

    @_validate_transaction_state
    def delete(self, key: Any):
        """
        Delete a key from the store.

        It first deletes the key from the update cache.

        :param key: key to delete from DB
        """
        key_serialized = self._serialize_key(key)
        try:
            self._batch.delete(key_serialized)
            self._update_cache.pop(key_serialized, None)
            self._delete_cache.add(key_serialized)
        except Exception:
            self._failed = True
            raise

    @_validate_transaction_state
    def exists(self, key: Any) -> bool:
        """
        Check if a key exists in the store.

        It first looks up the key in the update cache.

        :param key: a key to check in DB
        :return: `True` if the key exists, `False` otherwise.
        """

        key_serialized = self._serialize_key(key)
        if key_serialized in self._update_cache:
            return True
        return self._partition.exists(key_serialized)

    @property
    def completed(self) -> bool:
        """
        Check if the transaction is completed.

        It doesn't indicate whether transaction is successful or not.
        Use `RocksDBTransaction.failed` for that.

        The completed transaction should not be re-used.

        :return: `True` if transaction is completed, `False` otherwise.
        """
        return self._completed

    @property
    def failed(self) -> bool:
        """
        Check if the transaction has failed.

        The failed transaction should not be re-used because the update cache
        and

        :return: `True` if transaction is failed, `False` otherwise.
        """
        return self._failed

    def _update_changelog(self, cf_handle=None):
        logger.debug("Flushing state changes to the changelog topic...")
        offset = self._partition.get_changelog_offset() or 0
        offset += len(self._update_cache) + len(self._delete_cache)

        # TODO-CF: UNCOMMENT AND REPLACE ALL BELOW, ADD CF_HANDLE_TYPE
        # TODO-CF: Need to add delete cache back for clearing keys out in changelog
        # self._batch.put(_CHANGELOG_OFFSET_KEY, _int_to_int64_bytes(offset), cf_handle)
        # logger.debug(f"Changelog offset set to {offset}")
        # for cf_name in self._update_cache:
        #     for k, v in self._update_cache[cf_name].items():
        #         self._changelog_writer.produce(key=k, cf_name=cf_name, value=v)
        self._batch.put(_CHANGELOG_OFFSET_KEY, _int_to_int64_bytes(offset))
        logger.debug(f"Changelog offset set to {offset}")
        for k, v in self._update_cache.items():
            self._changelog_writer.produce(key=k, cf_name="default", value=v)
        for k in self._delete_cache:
            self._changelog_writer.produce(key=k, cf_name="default")  # tombstone record

    @_validate_transaction_state
    def maybe_flush(self, offset: Optional[int] = None):
        """
        Flush the recent updates to the database and empty the update cache.
        It writes the WriteBatch to RocksDB and marks itself as finished.

        If writing fails, the transaction will be also marked as "failed" and
        cannot be used anymore.

        >***NOTE:*** If no keys have been modified during the transaction
            (i.e. no "set" or "delete" have been called at least once), it will
            not flush ANY data to the database including the offset in order to optimize
            I/O.

        :param offset: offset of the last processed message, optional.
        """
        try:
            # Don't write batches if this transaction doesn't change any keys
            if len(self._batch):
                if offset is not None:
                    self._batch.put(_PROCESSED_OFFSET_KEY, _int_to_int64_bytes(offset))
                if self._changelog_writer:
                    self._update_changelog(cf_handle=None)  # TODO-CF: CF_HANDLE LOGIC
                self._partition.write(self._batch)
        except Exception:
            self._failed = True
            raise
        finally:
            self._completed = True

    def _serialize_value(self, value: Any) -> bytes:
        return serialize(value, dumps=self._dumps)

    def _deserialize_value(self, value: bytes) -> Any:
        return deserialize(value, loads=self._loads)

    def _serialize_key(self, key: Any) -> bytes:
        return serialize_key(key, prefix=self._prefix, dumps=self._dumps)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None and not self._failed:
            self.maybe_flush()


class RocksDBPartitionRecovery(PartitionRecovery):
    __slots__ = ("_partition", "_batch", "_failed", "_completed", "_offset")

    def __init__(
        self,
        partition: RocksDBStorePartition,
        offset: int,
    ):
        self._partition = partition
        self._batch = rocksdict.WriteBatch(raw_mode=True)
        self._offset = offset
        self._failed = False
        self._completed = False

    @_validate_transaction_state
    def set(self, key: bytes, value: bytes):
        """
        Set a key to the store.

        :param key: key to store in DB
        :param value: value to store in DB
        """
        try:
            # TODO-CF: ADD cf_name ARG AND UNCOMMENT/REPLACE
            # self._batch.put(key, value, cf_name)
            self._batch.put(key, value)
        except Exception:
            self._failed = True
            raise

    @_validate_transaction_state
    def delete(self, key: bytes):
        """
        Delete a key to the store.

        :param key: key to delete from DB
        """
        try:
            # TODO-CF: ADD cf_name ARG AND UNCOMMENT/REPLACE
            # self._batch.delete(key, cf_name)
            self._batch.delete(key)
        except Exception:
            self._failed = True
            raise

    @property
    def completed(self) -> bool:
        """
        Check if the transaction is completed.

        It doesn't indicate whether transaction is successful or not.
        Use `RocksDBTransaction.failed` for that.

        The completed transaction should not be re-used.

        :return: `True` if transaction is completed, `False` otherwise.
        """
        return self._completed

    @property
    def failed(self) -> bool:
        """
        Check if the transaction has failed.

        The failed transaction should not be re-used because the update cache
        and

        :return: `True` if transaction is failed, `False` otherwise.
        """
        return self._failed

    @_validate_transaction_state
    def flush(self):
        """
        Flush the recent updates to the database and empty the update cache.
        It writes the WriteBatch to RocksDB and marks itself as finished.

        If writing fails, the transaction will be also marked as "failed" and
        cannot be used anymore.

        >***NOTE:*** If no keys have been modified during the transaction
            (i.e no "set" or "delete" have been called at least once), it will
            not flush ANY data to the database including the offset in order to optimize
            I/O.
        """
        try:
            self._batch.put(
                _CHANGELOG_OFFSET_KEY, _int_to_int64_bytes(self._offset + 1)
            )
            self._partition.write(self._batch)
        except Exception:
            self._failed = True
            raise
        finally:
            self._completed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None and not self._failed:
            self.flush()
