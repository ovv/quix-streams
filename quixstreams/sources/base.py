import threading
import logging
import time

from typing import Optional, Union, Iterator

from quixstreams.models import Topic
from quixstreams.models.messages import KafkaMessage
from quixstreams.models.types import Headers
from quixstreams.rowproducer import RowProducer

from quixstreams.checkpointing.exceptions import CheckpointProducerTimeout

logger = logging.getLogger(__name__)


class BaseSource(threading.Thread):
    def __init__(self, shutdown_timeout: int = 10) -> None:
        super().__init__()

        self._running: bool = False
        self._topic: Optional[Topic] = None
        self._producer: Optional[RowProducer] = None
        self._shutdown_timeout = shutdown_timeout

        self.stopping = threading.Event()

    def configure(self, topic: Topic, producer: RowProducer) -> None:
        self._producer = producer
        self._producer_topic = topic

    def start(
        self,
    ) -> None:

        logger.info("Starting source %s", self)
        super().start()
        return self

    def serialize(
        self,
        key: Optional[object] = None,
        value: Optional[object] = None,
        headers: Optional[Headers] = None,
        timestamp_ms: Optional[int] = None,
    ) -> KafkaMessage:
        return self._producer_topic.serialize(
            key=key, value=value, headers=headers, timestamp_ms=timestamp_ms
        )

    def produce(
        self,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        poll_timeout: float = 5.0,
        buffer_error_max_tries: int = 3,
    ) -> None:

        self._producer.produce(
            topic=self._producer_topic.name,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp=timestamp,
            poll_timeout=poll_timeout,
            buffer_error_max_tries=buffer_error_max_tries,
        )

    def checkpoint(self) -> None:
        logger.debug("checkpoint: checkpointing source %s", self)
        unproduced_msg_count = self._producer.flush()
        if unproduced_msg_count > 0:
            raise CheckpointProducerTimeout(
                f"'{unproduced_msg_count}' messages failed to be produced before the producer flush timeout"
            )

    def run(self) -> None:
        try:
            for msg in self:
                self.produce(
                    key=msg.key,
                    value=msg.value,
                    headers=msg.headers,
                    timestamp=msg.timestamp,
                )

                if self.stopping.is_set():
                    break

            self.stop()
        except BaseException:
            logger.exception(f"Error in source {self}")

    def __iter__(self) -> Iterator[KafkaMessage]:
        while not self.stopping.is_set():
            try:
                msg = self.poll()
                if msg is not None:
                    yield msg
            except SourceCompleteException:
                return
            except Exception:
                logger.exception("Failed polling source %s", self)
                time.sleep(1)

    def sleep(self, seconds: float) -> None:
        if self.stopping.wait(seconds):
            raise SourceCompleteException("shutdown")

    def poll(self) -> Optional[KafkaMessage]:
        raise NotImplementedError(self.poll)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def default_topic(self, app) -> Topic:
        return app.topic(self.name)

    def stop(self) -> None:
        if not self.stopping.is_set():
            logger.info("Stopping source %s", self)
            self.stopping.set()

    def wait_stopped(self) -> None:
        self.join(self._shutdown_timeout)
        if self.is_alive():
            raise TimeoutError(f"source '{self}' failed to shutdown gracefully")


class SourceCompleteException(Exception):
    pass
