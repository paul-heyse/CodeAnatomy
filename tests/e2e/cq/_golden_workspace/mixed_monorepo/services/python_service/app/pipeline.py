from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable


def trace(label: str) -> Callable[[Callable[..., str]], Callable[..., str]]:
    def decorate(fn: Callable[..., str]) -> Callable[..., str]:
        def wrapped(*args: object, **kwargs: object) -> str:
            _ = label
            return fn(*args, **kwargs)

        return wrapped

    return decorate


class Backend(ABC):
    @abstractmethod
    def dispatch(self, payload: str) -> str:
        raise NotImplementedError


class LocalBackend(Backend):
    def dispatch(self, payload: str) -> str:
        return payload.upper()


@trace("runtime")
def dispatch_wrapper(payload: str, backend: Backend | None = None) -> str:
    active = backend or LocalBackend()
    resolver: Callable[[str], str] = active.dispatch
    return resolver(payload)


def make_pipeline(prefix: str) -> Callable[[str], str]:
    def _inner(value: str) -> str:
        return f"{prefix}:{dispatch_wrapper(value)}"

    return _inner
