"""
In-process mock Kafka for testing without Docker.
Drop-in replacement for confluent_kafka Producer/Consumer.
"""

import json
import threading
import time
from collections import defaultdict, deque
from typing import Dict, Optional


_topics = defaultdict(deque)  # type: Dict[str, deque]
_lock = threading.Lock()
_offsets = defaultdict(int)  # type: Dict[str, int]


class _Message:
    def __init__(self, value, topic):
        self._value = value
        self._topic = topic
        self._error = None

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def error(self):
        return self._error


class Producer:
    def __init__(self, config):
        pass

    def produce(self, topic, value):
        with _lock:
            _topics[topic].append(value)

    def flush(self):
        pass


class Consumer:
    def __init__(self, config):
        self._topics = []
        self._group = config.get("group.id", "default")

    def subscribe(self, topics):
        self._topics = topics

    def poll(self, timeout=0.1):
        for t in self._topics:
            with _lock:
                key = f"{self._group}:{t}"
                offset = _offsets[key]
                if offset < len(_topics[t]):
                    val = _topics[t][offset]
                    _offsets[key] += 1
                    return _Message(val, t)
        time.sleep(timeout)
        return None

    def close(self):
        pass
