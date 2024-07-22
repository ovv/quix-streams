import logging
import sys
import time
from typing import Tuple, Dict

from confluent_kafka import TopicPartition

from quixstreams.kafka import Consumer

logger = logging.getLogger(__name__)

_MAX_FLOAT = sys.float_info.max


class PausingManager:
    # TODO: Come up with a better name
    # TODO: Docs, tests
    # A class to keep a list of the paused (i.e. paused) partitions

    _paused_tps: Dict[Tuple[str, int], float]

    def __init__(self, consumer: Consumer):
        self._consumer = consumer
        self._paused_tps = {}
        self._next_resume_at = _MAX_FLOAT

    def revoke(self, topic: str, partition: int):
        """
        Remove partition from the list of paused TPs if it's revoked
        """
        tp = (topic, partition)
        if tp not in self._paused_tps:
            return
        self._paused_tps.pop(tp)
        self._reset_next_resume_at()

    def pause(self, topic: str, partition: int, resume_after: float):
        """
        Pause the topic-partition for a certain period of time.

        This method is supposed to be called in case of backpressure from Sinks.
        """
        if self.is_paused(topic=topic, partition=partition):
            # Exit early if the TP is already paused
            return

        # Add a TP to the dict to avoid repetitive pausing
        resume_at = time.monotonic() + resume_after
        self._paused_tps[(topic, partition)] = resume_at
        # Remember when the next TP should be resumed to exit early in the unpause()
        # calls.
        # Partitions get rarely paused, but the resume checks can be done
        # thousands times a sec.
        self._next_resume_at = min(self._next_resume_at, resume_at)
        logger.debug(
            f'Pause topic partition "{topic}[{partition}]" for {resume_after}s'
        )
        self._consumer.pause([TopicPartition(topic=topic, partition=partition)])

    def is_paused(self, topic: str, partition: int) -> bool:
        """
        Check if the topic-partition is already paused
        """
        return (topic, partition) in self._paused_tps

    def resume_if_ready(self):
        """
        Resume consuming from topic-partitions after the wait period has elapsed.
        """
        now = time.monotonic()
        if self._next_resume_at > now:
            # Nothing to resume yet, exit early
            return

        tps_to_resume = [
            tp for tp, resume_at in self._paused_tps.items() if resume_at <= now
        ]
        for topic, partition in tps_to_resume:
            logger.debug(f'Resume topic partition "{topic}[{partition}]"')
            self._consumer.resume([TopicPartition(topic=topic, partition=partition)])
            self._paused_tps.pop((topic, partition))
        self._reset_next_resume_at()

    def _reset_next_resume_at(self):
        if self._paused_tps:
            self._next_resume_at = min(self._paused_tps.values())
        else:
            self._next_resume_at = _MAX_FLOAT
