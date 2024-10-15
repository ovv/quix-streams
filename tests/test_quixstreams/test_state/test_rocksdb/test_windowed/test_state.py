from contextlib import contextmanager
from timeit import timeit

import pytest


@pytest.fixture
def store(windowed_rocksdb_store_factory):
    store = windowed_rocksdb_store_factory()
    store.assign_partition(0)
    return store


@pytest.fixture
def transaction_state(store):
    @contextmanager
    def _transaction_state():
        with store.start_partition_transaction(0) as tx:
            yield tx.as_state(prefix=b"__key__")

    return _transaction_state


def test_update_window(transaction_state):
    with transaction_state() as state:
        state.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
        assert state.get_window(start_ms=0, end_ms=10) == 1

    with transaction_state() as state:
        assert state.get_window(start_ms=0, end_ms=10) == 1


def test_update_window_with_window_timestamp(transaction_state):
    with transaction_state() as state:
        state.update_window(
            start_ms=0, end_ms=10, value=1, timestamp_ms=2, window_timestamp_ms=3
        )
        assert state.get_window(start_ms=0, end_ms=10) == [3, 1]

    with transaction_state() as state:
        assert state.get_window(start_ms=0, end_ms=10) == [3, 1]


@pytest.mark.parametrize("delete", [True, False])
def test_expire_windows(transaction_state, delete):
    duration_ms = 10

    with transaction_state() as state:
        state.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
        state.update_window(start_ms=10, end_ms=20, value=2, timestamp_ms=10)

    with transaction_state() as state:
        state.update_window(start_ms=20, end_ms=30, value=3, timestamp_ms=20)
        watermark = state.get_latest_timestamp() - duration_ms
        expired = state.expire_windows(watermark=watermark, delete=delete)
        # "expire_windows" must update the expiration index so that the same
        # windows are not expired twice
        assert not state.expire_windows(watermark=watermark, delete=delete)

    assert len(expired) == 2
    assert expired == [
        ((0, 10), 1),
        ((10, 20), 2),
    ]

    with transaction_state() as state:
        assert state.get_window(start_ms=0, end_ms=10) == None if delete else 1
        assert state.get_window(start_ms=10, end_ms=20) == None if delete else 2
        assert state.get_window(start_ms=20, end_ms=30) == 3


def test_same_keys_in_db_and_update_cache(transaction_state):
    duration_ms = 10

    with transaction_state() as state:
        state.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)

    with transaction_state() as state:
        # The same window already exists in the db
        state.update_window(start_ms=0, end_ms=10, value=3, timestamp_ms=8)

        state.update_window(start_ms=10, end_ms=20, value=2, timestamp_ms=10)
        watermark = state.get_latest_timestamp() - duration_ms
        expired = state.expire_windows(watermark=watermark)

        # Value from the cache takes precedence over the value in the db
        assert expired == [((0, 10), 3)]


def test_get_latest_timestamp(windowed_rocksdb_store_factory):
    store = windowed_rocksdb_store_factory()
    partition = store.assign_partition(0)
    timestamp = 123
    prefix = b"__key__"
    with partition.begin() as tx:
        state = tx.as_state(prefix)
        state.update_window(0, 10, value=1, timestamp_ms=timestamp)
    store.revoke_partition(0)

    partition = store.assign_partition(0)
    with partition.begin() as tx:
        assert tx.get_latest_timestamp() == timestamp


@pytest.mark.parametrize(
    "db_windows, cached_windows, deleted_windows, get_windows_args, expected_windows",
    [
        pytest.param(
            [
                dict(start_ms=1, end_ms=11, value=1, timestamp_ms=1),
                dict(start_ms=2, end_ms=12, value=2, timestamp_ms=2),
                dict(start_ms=3, end_ms=13, value=3, timestamp_ms=3),
            ],
            [],
            [],
            dict(start_from_ms=1, start_to_ms=2),
            [((2, 12), 2)],
            id="start-from-exclusive-start-to-inclusive",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            [],
            dict(start_from_ms=-1, start_to_ms=2),
            [((0, 10), 1), ((1, 11), 2), ((2, 12), 3)],
            id="messages-in-db",
        ),
        pytest.param(
            [],
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=2),
            [((0, 10), 1), ((1, 11), 2), ((2, 12), 3)],
            id="messages-in-cache",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
            ],
            [
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=2),
            [((0, 10), 1), ((1, 11), 2), ((2, 12), 3)],
            id="messages-both-in-db-and-in-cache",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=3, end_ms=13, value=4, timestamp_ms=4),
            ],
            [
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
                dict(start_ms=0, end_ms=10, value=5, timestamp_ms=1),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=3),
            [((0, 10), 5), ((1, 11), 2), ((2, 12), 3), ((3, 13), 4)],
            id="cache-message-overrides-db-message",
        ),
        pytest.param(
            [
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
            ],
            [],
            [dict(start_ms=0, end_ms=10)],
            dict(start_from_ms=-1, start_to_ms=2),
            [((1, 11), 2), ((2, 12), 3)],
            id="ignore-deleted-windows",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            [],
            dict(start_from_ms=-1, start_to_ms=2, backwards=True),
            [((2, 12), 3), ((1, 11), 2), ((0, 10), 1)],
            id="messages-in-db-backwards",
        ),
        pytest.param(
            [],
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=2, backwards=True),
            [((2, 12), 3), ((1, 11), 2), ((0, 10), 1)],
            id="messages-in-cache-backwards",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
            ],
            [
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=2, backwards=True),
            [((2, 12), 3), ((1, 11), 2), ((0, 10), 1)],
            id="messages-both-in-db-and-in-cache-backwards",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=3, end_ms=13, value=4, timestamp_ms=4),
            ],
            [
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
                dict(start_ms=0, end_ms=10, value=5, timestamp_ms=1),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=3, backwards=True),
            [((3, 13), 4), ((2, 12), 3), ((1, 11), 2), ((0, 10), 5)],
            id="cache-message-overrides-db-message",
        ),
        pytest.param(
            [
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
            ],
            [],
            [dict(start_ms=0, end_ms=10)],
            dict(start_from_ms=-1, start_to_ms=2, backwards=True),
            [((2, 12), 3), ((1, 11), 2)],
            id="ignore-deleted-windows",
        ),
    ],
)
def test_get_windows(
    db_windows,
    cached_windows,
    deleted_windows,
    get_windows_args,
    expected_windows,
    transaction_state,
):
    with transaction_state() as state:
        for window in db_windows:
            state.update_window(**window)

    with transaction_state() as state:
        for window in cached_windows:
            state.update_window(**window)
        for window in deleted_windows:
            state._transaction.delete_window(**window, prefix=state._prefix)

        windows = state.get_windows(**get_windows_args)
        assert list(windows) == expected_windows


@pytest.mark.timeit
@pytest.mark.parametrize("backwards", [False, True])
def test_get_windows_timeit(transaction_state, backwards):
    db_windows, cached_windows, deleted_windows = [], [], []
    number_of_windows = 100000

    for i in range(number_of_windows):
        window = {"start_ms": i, "end_ms": i + 10, "value": i, "timestamp_ms": i + 1}
        db_windows.append(window)

        if not i % 2:
            cached_windows.append(window)
        if not i % 4:
            deleted_windows.append(
                {"start_ms": window["start_ms"], "end_ms": window["end_ms"]}
            )

    with transaction_state() as state:
        for window in db_windows:
            state.update_window(**window)

    with transaction_state() as state:
        for window in cached_windows:
            state.update_window(**window)
        for window in deleted_windows:
            state._transaction.delete_window(**window, prefix=state._prefix)

        func = lambda: list(
            state.get_windows(
                start_from_ms=-1, start_to_ms=number_of_windows, backwards=backwards
            )
        )
        execution_time = timeit(func, number=100)

    print("Execution time:", execution_time)


def test_delete_windows(transaction_state):
    with transaction_state() as state:
        state.update_window(start_ms=1, end_ms=2, value=1, timestamp_ms=1)
        state.update_window(start_ms=2, end_ms=3, value=2, timestamp_ms=2)
        state.update_window(start_ms=3, end_ms=4, value=3, timestamp_ms=3)

    with transaction_state() as state:
        assert state.get_window(start_ms=1, end_ms=2)
        assert state.get_window(start_ms=2, end_ms=3)
        assert state.get_window(start_ms=3, end_ms=4)

        state.delete_windows(watermark=2)

        assert not state.get_window(start_ms=1, end_ms=2)
        assert not state.get_window(start_ms=2, end_ms=3)
        assert state.get_window(start_ms=3, end_ms=4)
