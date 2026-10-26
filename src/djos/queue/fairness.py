"""Fair sharing between priority tiers, so low priority can't starve.

Strict priority (always pop the highest non-empty tier) starves the lower tiers for as
long as higher-priority work keeps arriving. Instead, each pop *prefers* one tier, chosen
by a weighted rotation, and falls back to strict priority order when the preferred tier
is empty. Under contention every tier gets a guaranteed share proportional to its weight
(6/3/1 → 60%/30%/10% of pops); with no contention it behaves exactly like strict priority.

The rotation is nginx's "smooth weighted round-robin": it spreads each tier's turns out
(H N H H N H L H N H for 6/3/1) instead of bursting them (H H H H H H N N N L).
"""

from djos.models import Priority

DEFAULT_WEIGHTS = {Priority.HIGH: 6, Priority.NORMAL: 3, Priority.LOW: 1}


class WeightedRotation:
    def __init__(self, weights: dict[Priority, int] | None = None):
        self.weights = dict(weights or DEFAULT_WEIGHTS)
        if set(self.weights) != set(Priority) or min(self.weights.values()) < 1:
            raise ValueError("need a weight >= 1 for every priority")
        self._current = dict.fromkeys(self.weights, 0)
        self._total = sum(self.weights.values())

    def next(self) -> Priority:
        """The tier the next pop should prefer."""
        # Every tier earns its weight; the richest is picked and pays the total back.
        for tier, weight in self.weights.items():
            self._current[tier] += weight
        chosen = max(self._current, key=self._current.__getitem__)
        self._current[chosen] -= self._total
        return chosen
