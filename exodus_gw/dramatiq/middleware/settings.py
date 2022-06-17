import inspect
from functools import partial

from dramatiq import Middleware


class SettingsMiddleware(Middleware):
    """Middleware to make a Settings object available to all actors."""

    def __init__(self, settings):
        self.__settings = settings

    def before_declare_actor(self, broker, actor):
        sig = inspect.signature(actor.fn)

        if "settings" in sig.parameters:
            actor.fn = partial(actor.fn, settings=self.__settings)
