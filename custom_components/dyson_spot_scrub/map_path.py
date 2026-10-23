"""Remember observed cleaning actions per path edge, without modifying API data."""
from __future__ import annotations

from .const import MAP_PATH_ACTIONS


class PathHistory:
    """Preserve known prefixes; never guess the mode of rewritten history.

    Dyson does not provide a verified cleaning action per point. A new batch
    is attributed to the observed action. Delayed batches spanning an unseen
    action change cannot be reconstructed precisely.
    """

    def __init__(self) -> None:
        self.coords: list[tuple] = []
        self.actions: list[str | None] = []

    def annotate(self, live: dict, action: str | None) -> dict:
        path = live.get("cleanPath")
        if not isinstance(path, list) or len(path) < 2:
            return live
        try:
            coords = [(p["x"], p["y"]) if isinstance(p, dict) else (p[0], p[1])
                      for p in path]
        except (KeyError, IndexError, TypeError):
            return live
        prefix = 0
        for old, new in zip(self.coords, coords):
            if old != new:
                break
            prefix += 1
        rewritten = prefix < min(len(coords), len(self.coords))
        known = max(0, prefix - 1)
        action = action if action in MAP_PATH_ACTIONS and not rewritten else None
        actions = self.actions[:known] + [action] * (len(coords) - 1 - known)
        # A shorter cached snapshot must not erase a previously observed suffix.
        if len(coords) >= len(self.coords) or rewritten:
            self.coords, self.actions = coords, actions
        return {**live, "_pathModes": actions}
