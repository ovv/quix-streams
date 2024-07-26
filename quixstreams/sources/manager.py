from typing import List

from .base import BaseSource


class SourceManager:
    def __init__(self):
        self._sources = {}

    def register(self, source: BaseSource, producer, topic):
        if topic not in self._sources:
            self._sources[topic] = (source, producer)

    @property
    def sources(self) -> List[BaseSource]:
        return [source for source, _ in self._sources.values()]

    def start_sources(self):
        for topic, (source, producer) in self._sources.items():
            source.configure(topic, producer)
            source.start()

    def stop_sources(self):
        for source, _ in self._sources.values():
            source.stop()

        for source, _ in self._sources.values():
            source.wait_stopped()
