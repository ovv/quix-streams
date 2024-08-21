from operator import setitem

import pytest

from quixstreams.dataframe.exceptions import InvalidOperation
from quixstreams.core.stream import Stream
from quixstreams.core.stream.functions import (
    ApplyFunction,
    UpdateFunction,
    FilterFunction,
    TransformFunction,
)
from .utils import Sink


class TestStream:
    def test_add_apply(self):
        stream = Stream().add_apply(lambda v: v + 1)
        sink = Sink()
        stream.compose(sink=sink.append_record)(1, "key", 0, [])
        assert sink[0] == (2, "key", 0, [])

    def test_add_update(self):
        stream = Stream().add_update(lambda v: v.append(1))
        result = Sink()
        stream.compose(sink=result.append_record)([0], "key", 0, [])
        assert result[0] == ([0, 1], "key", 0, [])

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            (1, "key", 1, [], []),
            (0, "key", 1, [], [(0, "key", 1, [])]),
        ],
    )
    def test_add_filter(self, value, key, timestamp, headers, expected):
        stream = Stream().add_filter(lambda v: v == 0)
        result = Sink()
        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == expected

    def test_root_path(self):
        stream = (
            Stream()
            .add_apply(lambda v: ...)
            .add_filter(lambda v: ...)
            .add_update(lambda v: ...)
            .add_transform(lambda v, k, t, h: ...)
        )
        tree = stream.root_path()
        assert len(tree) == 5
        assert isinstance(tree[0].func, ApplyFunction)
        assert isinstance(tree[1].func, ApplyFunction)
        assert isinstance(tree[2].func, FilterFunction)
        assert isinstance(tree[3].func, UpdateFunction)
        assert isinstance(tree[4].func, TransformFunction)

    def test_diff_success(self):
        stream = Stream()
        stream = stream.add_apply(lambda v: v)
        stream2 = (
            stream.add_apply(lambda v: v)
            .add_update(lambda v: v)
            .add_filter(lambda v: v)
        )

        diff = stream.diff(stream2)

        diff_tree = diff.root_path()
        assert len(diff_tree) == 3
        assert isinstance(diff_tree[0].func, ApplyFunction)
        assert isinstance(diff_tree[1].func, UpdateFunction)
        assert isinstance(diff_tree[2].func, FilterFunction)

    def test_diff_differing_origin_fails(self):
        stream = Stream()
        stream = stream.add_apply(lambda v: v)
        stream2 = (
            stream.add_apply(lambda v: v)
            .add_update(lambda v: v)
            .add_filter(lambda v: v)
        )
        stream = stream.add_apply(lambda v: v)

        with pytest.raises(InvalidOperation):
            stream.diff(stream2)

    def test_diff_shared_origin_with_additional_split_fails(self):
        stream = Stream()
        stream = stream.add_apply(lambda v: v)
        stream2 = stream.add_apply(lambda v: v)
        stream3 = stream2.add_apply(lambda v: v)
        stream2 = stream2.add_apply(lambda v: v)

        with pytest.raises(InvalidOperation):
            stream.diff(stream2)

    def test_diff_empty_same_stream_fails(self):
        stream = Stream()
        with pytest.raises(ValueError, match="The diff is empty"):
            stream.diff(stream)

    def test_diff_empty_stream_full_child_fails(self):
        stream = Stream()
        stream2 = stream.add_apply(lambda v: v)
        with pytest.raises(ValueError, match="The diff is empty"):
            stream2.diff(stream)

    def test_diff_no_common_parent_fails(self):
        stream = Stream()
        stream2 = Stream()
        with pytest.raises(ValueError, match="Common parent not found"):
            stream.diff(stream2)

    def test_compose_allow_filters_false(self):
        stream = Stream().add_filter(lambda v: v)
        with pytest.raises(ValueError, match="Filter functions are not allowed"):
            stream.compose(allow_filters=False)

    def test_compose_allow_updates_false(self):
        stream = Stream().add_update(lambda v: v)
        with pytest.raises(ValueError, match="Update functions are not allowed"):
            stream.compose(allow_updates=False)

    def test_compose_allow_transforms_false(self):
        stream = Stream().add_transform(lambda value, key, timestamp, headers: ...)
        with pytest.raises(ValueError, match="Transform functions are not allowed"):
            stream.compose(allow_transforms=False)

    def test_repr(self):
        stream = (
            Stream()
            .add_apply(lambda v: v)
            .add_update(lambda v: v)
            .add_filter(lambda v: v)
        )
        repr(stream)

    def test_apply_expand(self):
        stream = Stream().add_apply(lambda v: [v, v], expand=True)
        result = Sink()
        value, key, timestamp, headers = 1, "key", 1, []

        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == [
            (value, key, timestamp, headers),
            (value, key, timestamp, headers),
        ]

    def test_apply_expand_not_iterable_returned(self):
        stream = Stream().add_apply(lambda v: 1, expand=True)
        with pytest.raises(TypeError):
            stream.compose()(1, "key", 0, [])

    def test_apply_expand_multiple(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v + 1, v + 1], expand=True)
            .add_apply(lambda v: [v, v + 1], expand=True)
        )
        result = Sink()
        value, key, timestamp, headers = 1, "key", 1, [("key", b"value")]
        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == [
            (value + 1, key, timestamp, headers),
            (value + 2, key, timestamp, headers),
            (value + 1, key, timestamp, headers),
            (value + 2, key, timestamp, headers),
        ]

    def test_apply_expand_filter_all_filtered(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v, v], expand=True)
            .add_apply(lambda v: [v, v], expand=True)
            .add_filter(lambda v: v != 1)
        )
        result = Sink()
        stream.compose(sink=result.append_record)(1, "key", 0, [])
        assert result == []

    def test_apply_expand_filter_some_filtered(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v, v + 1], expand=True)
            .add_filter(lambda v: v != 1)
            .add_apply(lambda v: [v, v], expand=True)
        )
        result = Sink()
        value, key, timestamp, headers = 1, "key", 1, None
        stream.compose(sink=result.append_record)(1, key, timestamp, headers)
        assert result == [(2, key, timestamp, headers), (2, key, timestamp, headers)]

    def test_apply_expand_update(self):
        stream = (
            Stream()
            .add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
            .add_update(lambda v: setitem(v, "x", v["x"] + 1))
        )
        result = Sink()
        key, timestamp, headers = "key", 123, None
        stream.compose(sink=result.append_record)(1, key, timestamp, headers)
        assert result == [
            ({"x": 2}, key, timestamp, headers),
            ({"x": 3}, key, timestamp, headers),
        ]

    def test_apply_expand_update_filter(self):
        stream = (
            Stream()
            .add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
            .add_update(lambda v: setitem(v, "x", v["x"] + 1))
            .add_filter(lambda v: v["x"] != 2)
        )
        result = Sink()
        key, timestamp, headers = "key", 123, []
        stream.compose(sink=result.append_record)(1, key, timestamp, headers)
        assert result == [({"x": 3}, key, timestamp, headers)]

    def test_compose_allow_expands_false(self):
        stream = Stream().add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
        with pytest.raises(ValueError, match="Expand functions are not allowed"):
            stream.compose(allow_expands=False)

    def test_add_apply_with_metadata(self):
        stream = Stream().add_apply(
            lambda v, key, timestamp, headers: v + 1, metadata=True
        )
        sink = Sink()
        stream.compose(sink=sink.append_record)(1, "key", 0, None)
        assert sink[0] == (2, "key", 0, None)

    def test_apply_record_with_metadata_expanded(self):
        stream = Stream().add_apply(
            lambda value_, key_, timestamp_, headers_: [value_, value_],
            expand=True,
            metadata=True,
        )
        result = Sink()
        value, key, timestamp, headers = 1, "key", 1, []

        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == [
            (value, key, timestamp, headers),
            (value, key, timestamp, headers),
        ]

    def test_add_update_with_metadata(self):
        stream = Stream().add_update(
            lambda value, key, timestamp, headers: value.append(1), metadata=True
        )
        result = Sink()
        stream.compose(sink=result.append_record)([0], "key", 0, [])
        assert result[0] == ([0, 1], "key", 0, [])

    @pytest.mark.parametrize(
        "value, key, timestamp, headers , expected",
        [
            (1, "key", 1, [], []),
            (0, "key", 1, [], [(0, "key", 1, [])]),
        ],
    )
    def test_add_filter_with_metadata(self, value, key, timestamp, headers, expected):
        stream = Stream().add_filter(
            lambda value_, key_, timestamp_, headers_: value_ == 0, metadata=True
        )
        result = Sink()
        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == expected

    def test_compose_returning(self):
        stream = Stream().add_apply(lambda v: v + 1)
        assert stream.compose_returning()(1, "key", 0, []) == (2, "key", 0, [])
        assert stream.compose_returning()(2, "key", 0, []) == (3, "key", 0, [])

    def test_compose_returning_exception(self):
        """
        Check that internal buffer of the composed function is emptied correctly
        in case of error
        """

        def _fail(value):
            if value == 1:
                raise ValueError("fail")
            return value + 1

        stream = Stream().add_apply(_fail)
        assert stream.compose_returning()(2, "key", 0, None) == (3, "key", 0, None)
        with pytest.raises(ValueError):
            assert stream.compose_returning()(1, "key", 0, None) == (3, "key", 0, None)
        assert stream.compose_returning()(2, "key", 0, None) == (3, "key", 0, None)

    @pytest.mark.parametrize(
        "stream, err",
        [
            (Stream().add_update(lambda v: ...), "Update functions are not allowed"),
            (
                Stream().add_update(lambda v, k, t, h: ..., metadata=True),
                "Update functions are not allowed",
            ),
            (Stream().add_filter(lambda v: ...), "Filter functions are not allowed"),
            (
                Stream().add_filter(lambda v, k, t, h: ..., metadata=True),
                "Filter functions are not allowed",
            ),
            (
                Stream().add_apply(lambda v: ..., expand=True),
                "Expand functions are not allowed",
            ),
            (
                Stream().add_apply(lambda v, k, t, h: ..., expand=True, metadata=True),
                "Expand functions are not allowed",
            ),
            (
                Stream().add_transform(lambda v, k, t, h: ..., expand=True),
                "Transform functions are not allowed",
            ),
            (
                Stream().add_transform(lambda v, k, t, h: ...),
                "Transform functions are not allowed",
            ),
        ],
    )
    def test_compose_returning_not_allowed_operations_fails(self, stream, err):
        with pytest.raises(ValueError, match=err):
            stream.compose_returning()

    def test_transform_record(self):
        stream = Stream().add_transform(
            lambda value, key, timestamp, headers: (
                value + 1,
                key + "1",
                timestamp + 1,
                [("key", b"value")],
            )
        )
        result = Sink()
        stream.compose(sink=result.append_record)(0, "key", 0, [])
        assert result[0] == (1, "key1", 1, [("key", b"value")])

    def test_transform_record_expanded(self):
        stream = Stream().add_transform(
            lambda value, key, timestamp, headers: [
                (value + 1, key + "1", timestamp + 1, [("key", b"value")]),
                (value + 2, key + "2", timestamp + 2, [("key", b"value2")]),
            ],
            expand=True,
        )
        result = Sink()
        stream.compose(sink=result.append_record)(0, "key", 0, [])
        assert result == [
            (1, "key1", 1, [("key", b"value")]),
            (2, "key2", 2, [("key", b"value2")]),
        ]


class TestStreamSplitting:

    def test_basic_split(self):
        calls = []

        def add_n(n):
            def wrapper(value):
                calls.append(n)
                print(f"adding {value} + {n}")
                return value + n

            return wrapper

        stream = Stream().add_apply(add_n(1))
        stream_2 = stream.add_apply(add_n(10))
        stream_3 = stream.add_apply(add_n(20))
        stream = stream.add_apply(add_n(100))
        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)(0, *extras)
        expected = [(11, *extras), (21, *extras), (101, *extras)]

        # each operation is only called once (no redundant processing)
        assert len(calls) == 4
        assert sink == expected

    def test_split_confirm_data_copied(self):
        """
        Use mutable object to confirm data is copied during splits.
        :return:
        """

        def d(n):
            def wrapper(value):
                value[n] = [n]
                if n - 1 in value:
                    value[n - 1].append(n)
                return value

            return wrapper

        stream = Stream().add_apply(d(0))
        stream_2 = stream.add_apply(d(1))
        stream = stream.add_apply(d(2))
        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)({}, *extras)
        expected = [({0: [0, 1], 1: [1]}, *extras), ({0: [0], 2: [2]}, *extras)]

        assert sink == expected

    def test_chained_split(self):
        calls = []

        def add_n(n):
            def wrapper(value):
                calls.append(n)
                return value + n

            return wrapper

        stream = Stream().add_apply(add_n(1))
        stream_2 = stream.add_apply(add_n(10)).add_apply(add_n(20))
        stream = stream.add_apply(add_n(100))
        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)(0, *extras)
        expected = [(31, *extras), (101, *extras)]

        # each operation is only called once (no redundant processing)
        assert len(calls) == 4
        assert sink == expected

    def test_longer_splits(self):
        calls = []

        def add_n(n):
            def wrapper(value):
                calls.append(n)
                return value + n

            return wrapper

        stream = Stream().add_apply(add_n(1))
        stream = stream.add_apply(add_n(2))
        stream_2 = stream.add_apply(add_n(10))
        stream_2 = stream_2.add_apply(add_n(20))
        stream = stream.add_apply(add_n(100))
        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)(0, *extras)
        expected = [(33, *extras), (103, *extras)]

        # each operation is only called once (no redundant processing)
        assert len(calls) == 5
        assert sink == expected

    def test_multi_split(self):
        """
        --< is a split
        "S'" denotes the continuation of the stream that was split from

        stream     ---[ add_120, div_2  ]---<      (stream', stream2), 60
        stream_2   ---[     div_3       ]---<      (stream_2', stream_3, stream_4), 20
        stream_3   ---[ add_10, add_3   ]---|END   33
        stream_4   ---[     add_24      ]---|END   44
        stream_2'  ---[     add_2       ]---|END   22
        stream'    ---[     add_40      ]---<      (stream'', stream_5), 100
        stream_5   ---[ div_2, add_5    ]---|END   55
        stream''   ---[ div_100, add_10 ]---|END   11

        :return:
        """

        calls = []

        def add_n(n):
            def wrapper(value):
                calls.append(f"add_{n}")
                return value + n

            return wrapper

        def div_n(n):
            def wrapper(value):
                calls.append(f"div_{n}")
                return value // n

            return wrapper

        stream = Stream().add_apply(add_n(120)).add_apply(div_n(2))  # 60
        stream_2 = stream.add_apply(div_n(3))  # 20
        stream_3 = stream_2.add_apply(add_n(10)).add_apply(add_n(3))  # 33
        stream_4 = stream_2.add_apply(add_n(24))  # 44
        stream_2 = stream_2.add_apply(add_n(2))  # 22
        stream = stream.add_apply(add_n(40))  # 100
        stream_5 = stream.add_apply(div_n(2)).add_apply(add_n(5))  # 55
        stream = stream.add_apply(div_n(100)).add_apply(add_n(10))  # 11
        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)(0, *extras)
        expected = [
            (33, *extras),
            (44, *extras),
            (22, *extras),
            (55, *extras),
            (11, *extras),
        ]

        # each operation is only called once (no redundant processing)
        assert len(calls) == 12
        assert sink == expected

    def test_filter(self):
        calls = []

        def add_n(n):
            def wrapper(value):
                calls.append(n)
                return value + n

            return wrapper

        def less_than(n):
            def wrapper(value):
                calls.append(n)
                return value < n

            return wrapper

        stream = Stream().add_apply(add_n(10))
        stream2 = stream.add_apply(add_n(5)).add_filter(less_than(0))
        stream2 = stream2.add_apply(add_n(200))
        stream3 = (
            stream.add_apply(add_n(7)).add_filter(less_than(20)).add_apply(add_n(4))
        )
        stream = stream.add_apply(add_n(30)).add_filter(less_than(50))
        stream4 = stream.add_apply(add_n(60))
        stream.add_apply(add_n(800))

        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)(0, *extras)
        expected = [(21, *extras), (100, *extras), (840, *extras)]

        # each operation is only called once (no redundant processing)
        assert len(calls) == 10
        assert sink == expected

    def test_update(self):
        calls = []

        def add_n(n):
            def wrapper(value):
                calls.append(n)
                return value + [n]

            return wrapper

        def update_n(n):
            def wrapper(value):
                calls.append(n)
                value.append(n)

            return wrapper

        stream = Stream().add_apply(add_n(10))
        stream2 = stream.add_update(update_n(5))
        stream = stream.add_update(update_n(30)).add_apply(add_n(6))
        stream3 = stream.add_update(update_n(100))
        stream4 = stream.add_update(update_n(456))
        stream = stream.add_apply(add_n(700)).add_update(update_n(222))

        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)([], *extras)
        expected = [
            ([10, 5], *extras),
            ([10, 30, 6, 100], *extras),
            ([10, 30, 6, 456], *extras),
            ([10, 30, 6, 700, 222], *extras),
        ]

        # each operation is only called once (no redundant processing)
        assert len(calls) == 8
        assert sink == expected

    def test_expand(self):
        calls = []

        def add_n(n):
            def wrapper(value):
                calls.append(n)
                return value + n

            return wrapper

        def expand(n):
            def wrapper(value):
                calls.append(n)
                return [v for v in value[n]]

            return wrapper

        stream = Stream()
        stream_2 = stream.add_apply(expand(0), expand=True).add_apply(add_n(22))
        stream_3 = stream.add_apply(expand(1), expand=True).add_apply(add_n(33))
        stream = stream.add_apply(expand(2), expand=True)
        stream_4 = stream.add_apply(add_n(44))
        stream = stream.add_apply(add_n(11))
        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)([(1, 2), (3, 4), (5, 6)], *extras)
        expected = [(n, *extras) for n in [23, 24, 36, 37, 49, 50, 16, 17]]

        # each operation is only called once (no redundant processing)
        assert len(calls) == 11
        assert sink == expected

    def test_transform(self):
        calls = []

        def add_n(n):
            def wrapper(value):
                calls.append(n)
                return value + n

            return wrapper

        def transform(n):
            def wrapper(value, k, t, h):
                calls.append(n)
                return value, k + "_" + str(n), t + n, h

            return wrapper

        stream = Stream().add_apply(add_n(1))
        stream_2 = stream.add_transform(transform(2))
        stream = stream.add_transform(transform(3))
        stream_3 = stream.add_apply(add_n(30)).add_transform(transform(4))
        stream_4 = stream.add_apply(add_n(40)).add_transform(transform(5))
        stream = stream.add_apply(add_n(100)).add_transform(transform(6))

        sink = Sink()
        extras = ("key", 0, [])
        stream.compose(sink=sink.append_record)(0, *extras)
        expected = [
            (1, "key_2", 2, []),
            (31, "key_3_4", 7, []),
            (41, "key_3_5", 8, []),
            (101, "key_3_6", 9, []),
        ]

        # each operation is only called once (no redundant processing)
        assert len(calls) == 9
        assert sink == expected
