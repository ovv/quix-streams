import logging

from typing import Iterable, Optional, Tuple

from quixstreams.models.messages import KafkaMessage

from .base import BaseSource

logger = logging.getLogger(__name__)


class ValueIterableSource(BaseSource):
    def __init__(
        self,
        values: Iterable[object],
        key: Optional[object] = None,
        shutdown_timeout: int = 10,
    ) -> None:
        super().__init__(shutdown_timeout)

        self._key = key
        self._values = values

    def poll(self):
        return self.serialize(key=self._key, value=next(self._values))


class ValueKeyIterableSource(BaseSource):
    def __init__(
        self, iterable: Iterable[Tuple[object, object]], shutdown_timeout: int = 10
    ) -> None:
        super().__init__(shutdown_timeout)

        self._iterable = iterable

    def poll(self) -> KafkaMessage:
        key, value = next(self._iterable)
        return self.serialize(key=key, value=value)
