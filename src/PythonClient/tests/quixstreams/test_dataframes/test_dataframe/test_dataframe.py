import pytest

from src.quixstreams.dataframes.dataframe.pipeline import (
    Pipeline,
)


class TestDataframe:
    def test_dataframe(self, dataframe):
        assert isinstance(dataframe._pipeline, Pipeline)
        assert dataframe._pipeline.id == dataframe.id

    def test__clone(self, dataframe):
        cloned_df = dataframe._clone()
        assert id(cloned_df) != id(dataframe)
        assert cloned_df._pipeline.id != dataframe._pipeline.id

    def test_apply(self, dataframe):
        def test_func(row):
            return row

        dataframe2 = dataframe.apply(test_func)
        assert id(dataframe) != id(dataframe2)


class TestDataframeProcess:
    def test_apply(self, dataframe, row_msg_value_factory, row_plus_n_func):
        dataframe = dataframe.apply(row_plus_n_func(1))
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) == row_msg_value_factory({'x': 2, 'y': 3})

    def test_apply_fluent(self, dataframe, row_msg_value_factory, row_plus_n_func):
        dataframe = dataframe.apply(row_plus_n_func(n=1)).apply(row_plus_n_func(n=2))
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) == row_msg_value_factory({'x': 4, 'y': 5})

    def test_apply_sequential(self, dataframe, row_msg_value_factory, row_plus_n_func):
        dataframe = dataframe.apply(row_plus_n_func(n=1))
        dataframe = dataframe.apply(row_plus_n_func(n=2))
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) == row_msg_value_factory({'x': 4, 'y': 5})

    def test_setitem_primitive(self, dataframe, row_msg_value_factory):
        dataframe['new'] = 1
        row = row_msg_value_factory({'x': 1})
        assert dataframe.process(row) == row_msg_value_factory({'x': 1, 'new': 1})

    def test_setitem_column_only(self, dataframe, row_msg_value_factory):
        dataframe['new'] = dataframe['x']
        row = row_msg_value_factory({'x': 1})
        assert dataframe.process(row) == row_msg_value_factory({'x': 1, 'new': 1})

    def test_setitem_column_with_function(self, dataframe, row_msg_value_factory):
        dataframe['new'] = dataframe['x'].apply(lambda v: v + 5)
        row = row_msg_value_factory({'x': 1})
        assert dataframe.process(row) == row_msg_value_factory({'x': 1, 'new': 6})

    def test_setitem_column_with_operations(self, dataframe, row_msg_value_factory):
        dataframe['new'] = dataframe['x'] + dataframe['y'].apply(lambda v: v + 5) + 1
        row = row_msg_value_factory({'x': 1, 'y': 2})
        expected = row_msg_value_factory({'x': 1, 'y': 2, 'new': 9})
        assert dataframe.process(row) == expected

    def test_column_subset(self, dataframe, row_msg_value_factory):
        dataframe = dataframe[['x', 'y']]
        row = row_msg_value_factory({'x': 1, 'y': 2, 'z': 3})
        expected = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) == expected

    def test_column_subset_with_funcs(
            self, dataframe, row_msg_value_factory, row_plus_n_func
    ):
        dataframe = dataframe[['x', 'y']].apply(row_plus_n_func(n=5))
        row = row_msg_value_factory({'x': 1, 'y': 2, 'z': 3})
        expected = row_msg_value_factory({'x': 6, 'y': 7})
        assert dataframe.process(row) == expected

    def test_inequality_filter(self, dataframe, row_msg_value_factory):
        dataframe = dataframe[dataframe['x'] > 0]
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) == row

    def test_inequality_filter_is_filtered(self, dataframe, row_msg_value_factory):
        dataframe = dataframe[dataframe['x'] >= 1000]
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) is None

    def test_inequality_filter_with_operation(self, dataframe, row_msg_value_factory):
        dataframe = dataframe[(dataframe['x'] - 0 + dataframe['y']) > 0]
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) == row

    def test_inequality_filter_with_operation_is_filtered(
            self, dataframe, row_msg_value_factory
    ):
        dataframe = dataframe[(dataframe['x'] - dataframe['y']) > 0]
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) is None

    def test_inequality_filtering_with_apply(
            self, dataframe, row_msg_value_factory
    ):
        dataframe = dataframe[dataframe['x'].apply(lambda v: v - 1) >= 0]
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) == row

    def test_inequality_filtering_with_apply_is_filtered(
            self, dataframe, row_msg_value_factory
    ):
        dataframe = dataframe[dataframe['x'].apply(lambda v: v - 10) >= 0]
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) is None

    def test_compound_inequality_filter(self, dataframe, row_msg_value_factory):
        dataframe = dataframe[(dataframe['x'] >= 0) & (dataframe['y'] < 10)]
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) == row

    def test_compound_inequality_filter_is_filtered(
            self, dataframe, row_msg_value_factory):
        dataframe = dataframe[(dataframe['x'] >= 0) & (dataframe['y'] < 0)]
        row = row_msg_value_factory({'x': 1, 'y': 2})
        assert dataframe.process(row) is None

    @pytest.mark.skip('This should fail based on our outline but currently does not')
    # TODO: make this fail correctly
    def test_non_row_apply_breaks_things(self, dataframe, row_msg_value_factory):
        dataframe = dataframe.apply(lambda row: False)
        dataframe = dataframe.apply(lambda row: row)
        row = row_msg_value_factory({'x': 1, 'y': 2})
        dataframe.process(row)

    def test_multiple_row_generation(
            self, dataframe, more_rows_func, row_msg_value_factory
    ):
        dataframe = dataframe.apply(more_rows_func)
        expected = [row_msg_value_factory({'x': 1, 'x_list': i}) for i in range(3)]
        row = row_msg_value_factory({'x': 1, 'x_list': [0, 1, 2]})
        assert dataframe.process(row) == expected

    def test_multiple_row_generation_with_additional_apply(
            self, dataframe, more_rows_func, row_msg_value_factory, row_plus_n_func
    ):
        dataframe = dataframe.apply(more_rows_func)
        dataframe = dataframe.apply(row_plus_n_func(n=1))
        expected = [row_msg_value_factory({'x': 2, 'x_list': i + 1}) for i in range(3)]
        row = row_msg_value_factory({'x': 1, 'x_list': [0, 1, 2]})
        assert dataframe.process(row) == expected

    def test_multiple_row_generation_with_additional_filtering(
            self, dataframe, more_rows_func, row_msg_value_factory
    ):
        dataframe = dataframe.apply(more_rows_func)
        dataframe = dataframe.apply(lambda row: row if row['x_list'] > 0 else None)
        expected = [row_msg_value_factory({'x': 1, 'x_list': i}) for i in range(1, 3)]
        row = row_msg_value_factory({'x': 1, 'x_list': [0, 1, 2]})
        assert dataframe.process(row) == expected
