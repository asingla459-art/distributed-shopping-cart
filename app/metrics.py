"""In-process metrics exposed in Prometheus text format at GET /metrics.

Per-instance counters live here; cluster-wide business counters
(conflicts, idempotent replays, failovers...) are also mirrored into the Redis
hash `metrics:cluster` so the admin endpoint can show one total.
"""
from __future__ import annotations

import bisect
import threading
from collections import defaultdict

BUCKETS = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.counters: dict[tuple, float] = defaultdict(float)
        self.hist: dict[str, list[int]] = defaultdict(lambda: [0] * (len(BUCKETS) + 1))
        self.hist_sum: dict[str, float] = defaultdict(float)
        self.hist_count: dict[str, int] = defaultdict(int)

    def inc(self, name: str, value: float = 1, **labels: str) -> None:
        key = (name, tuple(sorted(labels.items())))
        with self._lock:
            self.counters[key] += value

    def observe(self, route: str, seconds: float) -> None:
        i = bisect.bisect_left(BUCKETS, seconds)
        with self._lock:
            self.hist[route][i] += 1
            self.hist_sum[route] += seconds
            self.hist_count[route] += 1

    def get(self, name: str, **labels: str) -> float:
        return self.counters.get((name, tuple(sorted(labels.items()))), 0.0)

    def render(self, instance: str) -> str:
        lines: list[str] = []
        with self._lock:
            for (name, labels), v in sorted(self.counters.items()):
                lab = ",".join([f'instance="{instance}"'] + [f'{k}="{val}"' for k, val in labels])
                lines.append(f"{name}{{{lab}}} {v}")
            for route, counts in sorted(self.hist.items()):
                cum = 0
                for b, c in zip(BUCKETS + [float("inf")], counts):
                    cum += c
                    le = "+Inf" if b == float("inf") else str(b)
                    lines.append(f'cart_http_request_duration_seconds_bucket{{instance="{instance}",route="{route}",le="{le}"}} {cum}')
                lines.append(f'cart_http_request_duration_seconds_sum{{instance="{instance}",route="{route}"}} {self.hist_sum[route]:.6f}')
                lines.append(f'cart_http_request_duration_seconds_count{{instance="{instance}",route="{route}"}} {self.hist_count[route]}')
        return "\n".join(lines) + "\n"


METRICS = Metrics()
