"""Lightweight async rule executer for phase-based workflows."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


ConditionFn = Callable[[Any, Exception | None], bool | Awaitable[bool]]
ActionFn = Callable[[Any], Any | Awaitable[Any]]
PostActionFn = Callable[[dict[str, Any], Any, Exception | None], Any | Awaitable[Any]]


@dataclass
class Rule:
    condition: Any
    action: str
    node: str | None = None


class RuleExecuter:
    """
    Execute declarative rules for the current node/phase.

    Supported config forms:
    1) Dict:
       {
         "nodeField": "phase",
         "rules": [{"condition": "phase == 'PENDING'", "action": "init", "node": "NEXT"}]
       }
    2) YAML string with the same keys (requires PyYAML in environment).

    `condition` supports:
    - bool: direct true/false
    - str: python expression using names: model, error, plus model attributes at top-level
    - callable: function(model, error) -> bool
    """

    def __init__(
        self,
        config: dict[str, Any] | str,
        actions: dict[str, ActionFn],
        post_action: PostActionFn | None = None,
    ) -> None:
        self.config = self._load_config(config)
        self.node_field = self.config.get("nodeField", "phase")
        self.rules = [Rule(**rule) for rule in self.config.get("rules", [])]
        self.actions = actions
        self.post_action = post_action

    async def execute(self, model: Any) -> dict[str, Any] | None:
        """Match and execute the first rule for current node."""
        current_node = getattr(model, self.node_field, None)

        for rule in self.rules:
            if not await self._matches(rule.condition, model, None):
                continue

            if rule.action not in self.actions:
                raise KeyError(f"Unknown action '{rule.action}'")

            error: Exception | None = None
            try:
                action_fn = self.actions[rule.action]
                await self._maybe_await(action_fn(model))
                if rule.node is not None:
                    setattr(model, self.node_field, rule.node)
            except Exception as exc:  # pragma: no cover - exercised by callers
                error = exc

            rule_data = {
                "condition": rule.condition,
                "action": rule.action,
                "from": current_node,
                "to": getattr(model, self.node_field, None),
                "node": rule.node,
            }

            if self.post_action is not None:
                await self._maybe_await(self.post_action(rule_data, model, error))

            if error is not None:
                raise error

            return rule_data

        return None

    def _load_config(self, config: dict[str, Any] | str) -> dict[str, Any]:
        if isinstance(config, dict):
            return config

        if isinstance(config, str):
            try:
                import yaml
            except ModuleNotFoundError as exc:  # pragma: no cover
                raise ModuleNotFoundError(
                    "YAML config requires PyYAML; install pyyaml or pass dict config"
                ) from exc
            loaded = yaml.safe_load(config)
            if not isinstance(loaded, dict):
                raise ValueError("Rule config must be a dict-like object")
            return loaded

        raise TypeError("config must be dict or YAML string")

    async def _matches(self, condition: Any, model: Any, error: Exception | None) -> bool:
        if isinstance(condition, bool):
            return condition

        if callable(condition):
            return bool(await self._maybe_await(condition(model, error)))

        if isinstance(condition, str):
            env = {"model": model, "error": error}
            if hasattr(model, "__dict__"):
                env.update(model.__dict__)
            result = eval(condition, {"__builtins__": {}}, env)
            return bool(result)

        raise TypeError(f"Unsupported condition type: {type(condition)!r}")

    async def _maybe_await(self, value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value


RuleExecutor = RuleExecuter
