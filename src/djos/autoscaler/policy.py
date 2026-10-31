"""How many workers to run, given the backlog. A control loop modelled on the Kubernetes
Horizontal Pod Autoscaler.

    recommendation = clamp(ceil(backlog / target_per_worker), min_workers, max_workers)

where backlog = jobs waiting + jobs running.

- Scale **up** straight to the recommendation: a backlog is latency users feel now.
- Scale **down** only to the *highest* recommendation seen during the last
  `scale_down_window` seconds. A spiky load would otherwise flap: scale down in a lull,
  then up again a moment later, churning containers (each start and stop has a cost,
  and a stopping worker has to finish its job first). This is the HPA's "stabilization
  window", a form of hysteresis.
"""

import math
from collections import deque
from dataclasses import dataclass, field


@dataclass
class ScalingPolicy:
    min_workers: int = 1
    max_workers: int = 10
    target_per_worker: int = 20  # backlog one worker should handle
    scale_down_window: float = 30.0  # seconds
    _history: deque[tuple[float, int]] = field(default_factory=deque, repr=False)

    def __post_init__(self) -> None:
        if not 1 <= self.min_workers <= self.max_workers:
            raise ValueError("need 1 <= min_workers <= max_workers")
        if self.target_per_worker < 1:
            raise ValueError("target_per_worker must be >= 1")

    def recommend(self, backlog: int) -> int:
        wanted = math.ceil(max(backlog, 0) / self.target_per_worker)
        return max(self.min_workers, min(self.max_workers, wanted))

    def desired(self, current: int, backlog: int, now: float) -> int:
        """The worker count to run now. `now` is a monotonic clock reading."""
        recommendation = self.recommend(backlog)
        self._history.append((now, recommendation))
        while self._history and self._history[0][0] < now - self.scale_down_window:
            self._history.popleft()
        if recommendation >= current:
            return recommendation  # scale up (or hold) immediately
        return max(r for _, r in self._history)  # scale down only as far as the window allows
