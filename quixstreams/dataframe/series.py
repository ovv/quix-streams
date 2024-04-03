import contextvars
import operator
from typing import Optional, Union, Callable, Container, Any, Mapping

from typing_extensions import Self

from quixstreams.context import set_message_context
from quixstreams.core.stream.functions import StreamCallable, ApplyFunction
from quixstreams.core.stream.stream import Stream
from quixstreams.models.messagecontext import MessageContext
from .base import BaseStreaming
from .exceptions import InvalidOperation, MissingColumn, InvalidColumnReference

__all__ = ("StreamingSeries",)


def _getitem(d: Mapping, column_name: Union[str, int]) -> object:
    """
    Special error handling around column referencing with SDF.

    :param d: a dict-like object (usually just a dict).
    :column_name: the column name.

    :return: Nested data from column name
    """
    try:
        return d[column_name]
    except KeyError:
        raise MissingColumn(
            f"Column name '{column_name}' does not exist in the StreamingDataFrame"
        )
    except TypeError:
        d_type = getattr(type(d), "__name__", type(d))
        raise InvalidColumnReference(
            f"Cannot reference column name '{column_name}' for StreamingDataFrame "
            f"data type '{d_type}'; a dictionary is required."
        )


class StreamingSeries(BaseStreaming):
    """
    `StreamingSeries` are typically generated by `StreamingDataframes` when getting
    elements from, or performing certain operations on, a `StreamingDataframe`,
    thus acting as a representation of "column" value.

    They share some operations with the `StreamingDataframe`, but also provide some
    additional functionality.

    Most column value operations are handled by this class, and `StreamingSeries` can
    generate other `StreamingSeries` as a result of said operations.


    What it Does:

    - Allows ways to do simple operations with dataframe "column"/dictionary values:
        - Basic ops like add, subtract, modulo, etc.
    - Enables comparisons/inequalities:
        - Greater than, equals, etc.
        - and/or, is/not operations
    - Can check for existence of columns in `StreamingDataFrames`
    - Enables chaining of various operations together


    How to Use:

    For the most part, you may not even notice this class exists!
    They will naturally be created as a result of typical `StreamingDataFrame` use.

    Auto-complete should help you with valid methods and type-checking should alert
    you to invalid operations between `StreamingSeries`.

    In general, any typical Pands dataframe operation between columns should be valid
    with `StreamingSeries`, and you shouldn't have to think about them explicitly.


    Example Snippet:

    ```python
    # Random methods for example purposes. More detailed explanations found under
    # various methods or in the docs folder.

    sdf = StreamingDataframe()
    sdf = sdf["column_a"].apply(a_func).apply(diff_func, stateful=True)
    sdf["my_new_bool_field"] = sdf["column_b"].contains("this_string")
    sdf["new_sum_field"] = sdf["column_c"] + sdf["column_d"] + 2
    sdf = sdf[["column_a"] & (sdf["new_sum_field"] >= 10)]
    ```
    """

    def __init__(
        self,
        name: Optional[str] = None,
        stream: Optional[Stream] = None,
    ):
        if not (name or stream):
            raise ValueError('Either "name" or "stream" must be passed')
        self._stream = stream or Stream(func=ApplyFunction(lambda v: _getitem(v, name)))

    @classmethod
    def from_func(cls, func: StreamCallable) -> Self:
        """
        Create a StreamingSeries from a function.

        The provided function will be wrapped into `Apply`
        :param func: a function to apply
        :return: instance of `StreamingSeries`
        """
        return cls(stream=Stream(ApplyFunction(func)))

    @property
    def stream(self) -> Stream:
        return self._stream

    def apply(self, func: StreamCallable) -> Self:
        """
        Add a callable to the execution list for this series.

        The provided callable should accept a single argument, which will be its input.
        The provided callable should similarly return one output, or None

        They can be chained together or included with other operations.


        Example Snippet:

        ```python
        # The `StreamingSeries` are generated when `sdf["COLUMN_NAME"]` is called.
        # This stores a string in state and capitalizes the column value; the result is
        # assigned to a new column.
        #  Another apply converts a str column to an int, assigning it to a new column.

        def func(value: str, state: State):
            if value != state.get("my_store_key"):
                state.set("my_store_key") = value
            return v.upper()

        sdf = StreamingDataframe()
        sdf["new_col"] = sdf["a_column"]["nested_dict_key"].apply(func, stateful=True)
        sdf["new_col_2"] = sdf["str_col"].apply(lambda v: int(v)) + sdf["str_col2"] + 2
        ```

        :param func: a callable with one argument and one output
        :return: a new `StreamingSeries` with the new callable added
        """
        child = self._stream.add_apply(func)
        return self.__class__(stream=child)

    def compose(
        self,
        allow_filters: bool = True,
        allow_updates: bool = True,
    ) -> StreamCallable:
        """
        Compose all functions of this StreamingSeries into one big closure.

        Closures are more performant than calling all the functions in the
        `StreamingDataFrame` one-by-one.

        Generally not required by users; the `quixstreams.app.Application` class will
        do this automatically.


        Example Snippet:

        ```python
        from quixstreams import Application

        app = Application(...)

        sdf = app.dataframe()
        sdf = sdf["column_a"].apply(apply_func)
        sdf = sdf["column_b"].contains(filter_func)
        sdf = sdf.compose()

        result_0 = sdf({"my": "record"})
        result_1 = sdf({"other": "record"})
        ```

        :param allow_filters: If False, this function will fail with ValueError if
            the stream has filter functions in the tree. Default - True.
        :param allow_updates: If False, this function will fail with ValueError if
            the stream has update functions in the tree. Default - True.

        :raises ValueError: if disallowed functions are present in the tree of
            underlying `Stream`.

        :return: a function that accepts "value"
            and returns a result of `StreamingSeries`
        """

        return self._stream.compose(
            allow_filters=allow_filters, allow_updates=allow_updates
        )

    def test(self, value: Any, ctx: Optional[MessageContext] = None) -> Any:
        """
        A shorthand to test `StreamingSeries` with provided value
        and `MessageContext`.

        :param value: value to pass through `StreamingSeries`
        :param ctx: instance of `MessageContext`, optional.
            Provide it if the StreamingSeries instance has
            functions calling `get_current_key()`.
            Default - `None`.
        :return: result of `StreamingSeries`
        """
        context = contextvars.copy_context()
        context.run(set_message_context, ctx)
        composed = self.compose()
        return context.run(composed, value)

    def _operation(
        self,
        other: Union[Self, str, int, object],
        operator_: Callable[
            [Union[Self, Container, Mapping, object], Union[Self, str, int, object]],
            Union[bool, object],
        ],
    ) -> Self:
        self_composed = self.compose()
        if isinstance(other, self.__class__):
            other_composed = other.compose()
            return self.from_func(
                func=lambda v, op=operator_: op(self_composed(v), other_composed(v))
            )
        else:
            return self.from_func(
                func=lambda v, op=operator_: op(self_composed(v), other)
            )

    def isin(self, other: Container) -> Self:
        """
        Check if series value is in "other".
        Same as "StreamingSeries in other".

        Runtime result will be a `bool`.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Check if "str_column" is contained in a column with a list of strings and
        # assign the resulting `bool` to a new column: "has_my_str".

        sdf = app.dataframe()
        sdf["has_my_str"] = sdf["str_column"].isin(sdf["column_with_list_of_strs"])
        ```

        :param other: a container to check
        :return: new StreamingSeries
        """
        return self._operation(
            other, lambda a, b, contains=operator.contains: contains(b, a)
        )

    def contains(self, other: Union[Self, object]) -> Self:
        """
        Check if series value contains "other"
        Same as "other in StreamingSeries".

        Runtime result will be a `bool`.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Check if "column_a" contains "my_substring" and assign the resulting
        # `bool` to a new column: "has_my_substr"

        sdf = app.dataframe()
        sdf["has_my_substr"] = sdf["column_a"].contains("my_substring")
        ```

        :param other: object to check
        :return: new StreamingSeries
        """
        return self._operation(other, operator.contains)

    def is_(self, other: Union[Self, object]) -> Self:
        """
        Check if series value refers to the same object as `other`

        Runtime result will be a `bool`.


        Example Snippet:

        ```python
        # Check if "column_a" is the same as "column_b" and assign the resulting `bool`
        #  to a new column: "is_same"

        from quixstreams import Application
        sdf = app.dataframe()
        sdf["is_same"] = sdf["column_a"].is_(sdf["column_b"])
        ```

        :param other: object to check for "is"
        :return: new StreamingSeries
        """
        return self._operation(other, operator.is_)

    def isnot(self, other: Union[Self, object]) -> Self:
        """
        Check if series value does not refer to the same object as `other`

        Runtime result will be a `bool`.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Check if "column_a" is the same as "column_b" and assign the resulting `bool`
        # to a new column: "is_not_same"

        sdf = app.dataframe()
        sdf["is_not_same"] = sdf["column_a"].isnot(sdf["column_b"])
        ```

        :param other: object to check for "is_not"
        :return: new StreamingSeries
        """
        return self._operation(other, operator.is_not)

    def isnull(self) -> Self:
        """
        Check if series value is None.

        Runtime result will be a `bool`.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Check if "column_a" is null and assign the resulting `bool` to a new column:
        # "is_null"

        sdf = app.dataframe()
        sdf["is_null"] = sdf["column_a"].isnull()
        ```

        :return: new StreamingSeries
        """
        return self._operation(None, operator.is_)

    def notnull(self) -> Self:
        """
        Check if series value is not None.

        Runtime result will be a `bool`.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Check if "column_a" is not null and assign the resulting `bool` to a new column:
        # "is_not_null"

        sdf = app.dataframe()
        sdf["is_not_null"] = sdf["column_a"].notnull()
        ```

        :return: new StreamingSeries
        """
        return self._operation(None, operator.is_not)

    def abs(self) -> Self:
        """
        Get absolute value of the series value.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Get absolute value of "int_col" and add it to "other_int_col".
        # Finally, assign the result to a new column: "abs_col_sum".

        sdf = app.dataframe()
        sdf["abs_col_sum"] = sdf["int_col"].abs() + sdf["other_int_col"]
        ```

        :return: new StreamingSeries
        """
        return self.apply(func=lambda v: abs(v))

    def __bool__(self):
        raise InvalidOperation(
            f"Cannot assess truth level of a {self.__class__.__name__} "
            f"using 'bool()' or any operations that rely on it; "
            f"use '&' or '|' for logical and/or comparisons"
        )

    def __getitem__(self, item: Union[str, int]) -> Self:
        return self._operation(item, operator.getitem)

    def __mod__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.mod)

    def __add__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.add)

    def __sub__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.sub)

    def __mul__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.mul)

    def __truediv__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.truediv)

    def __eq__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.eq)

    def __ne__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.ne)

    def __lt__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.lt)

    def __le__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.le)

    def __gt__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.gt)

    def __ge__(self, other: Union[Self, object]) -> Self:
        return self._operation(other, operator.ge)

    def __and__(self, other: Union[Self, object]) -> Self:
        """
        Do a logical "and" comparison.

        >***NOTE:*** It behaves differently than `pandas`. `pandas` performs
            a bitwise "and" if one of the arguments is a number.
            This function always does a logical "and" instead.
        """

        # Do the "and" check manually instead of calling `self._operation`
        # to preserve Python's lazy evaluation of `and`.
        # Otherwise, it always evaluates both left and right side of the expression
        # to compute the result which is not always desired.
        # See https://docs.python.org/3/reference/expressions.html#boolean-operations
        self_composed = self.compose()
        if isinstance(other, self.__class__):
            other_composed = other.compose()
            return self.from_func(func=lambda v: self_composed(v) and other_composed(v))
        else:
            return self.from_func(func=lambda v: self_composed(v) and other)

    def __or__(self, other: Union[Self, object]) -> Self:
        """
        Do a logical "or" comparison.

        >***NOTE:*** It behaves differently than `pandas`. `pandas` performs
            a bitwise "or" if one of the arguments is a number.
            This function always does a logical "or" instead.
        """

        # Do the "or" check manually instead of calling `self._operation`
        # to preserve Python's lazy evaluation of `or`.
        # Otherwise, it always evaluates both left and right side of the expression
        # to compute the result which is not always desired.
        # See https://docs.python.org/3/reference/expressions.html#boolean-operations
        self_composed = self.compose()
        if isinstance(other, self.__class__):
            other_composed = other.compose()
            return self.from_func(func=lambda v: self_composed(v) or other_composed(v))
        else:
            return self.from_func(func=lambda v: self_composed(v) or other)

    def __invert__(self) -> Self:
        """
        Do a logical "not".

        >***NOTE:*** It behaves differently than `pandas`. `pandas` performs
            a bitwise "not" if argument is a number.
            This function always does a logical "not" instead.
        """
        return self.apply(lambda v: not v)
