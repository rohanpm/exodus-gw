import logging
from contextvars import ContextVar
from functools import wraps

from dramatiq import Middleware
from dramatiq.middleware import CurrentMessage

CURRENT_PREFIX: ContextVar[str] = ContextVar("CURRENT_PREFIX")
CURRENT_MESSAGE: ContextVar[str] = ContextVar("CURRENT_MESSAGE")

LOG = logging.getLogger("exodus-gw")


class PrefixFilter(logging.Filter):
    # A filter which will add CURRENT_PREFIX and dramatiq message ID
    # onto each message.

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = CURRENT_PREFIX.get("") + record.msg
        record.message_id = CURRENT_MESSAGE.get(None)
        return True


class LogActorMiddleware(Middleware):
    """Middleware to prefix every log message with the current actor name."""

    def __init__(self):
        self.filter = PrefixFilter()

    def after_process_boot(self, broker):
        logging.getLogger().handlers[0].addFilter(self.filter)

    def before_declare_actor(self, broker, actor):
        actor.fn = self.wrap_fn_with_prefix(actor.fn)

    def wrap_fn_with_prefix(self, fn):
        # Given a function, returns a wrapped version of it which will adjust
        # CURRENT_PREFIX around the function's invocation.

        @wraps(fn)
        def new_fn(*args, **kwargs):
            # We want to show the function name (which is the actor name)...
            prefix = fn.__name__

            # If the actor takes a publish or task ID as an argument, we want
            # to show that as well
            for key in ("publish_id", "task_id"):
                if key in kwargs:
                    prefix = f"{prefix} {kwargs[key]}"
                    break

            prefix = f"[{prefix}] "

            message_id = ""
            if message := CurrentMessage.get_current_message():
                message_id = message.message_id

            message_token = CURRENT_MESSAGE.set(message_id)
            prefix_token = CURRENT_PREFIX.set(prefix)
            try:
                # Ensure everything gets at least a start/complete log
                # in case the actor internally doesn't log anything.
                #
                # Not needed in the case of the actor failing, because
                # there will already be an unambiguous ERROR log generated
                # for that.
                #
                # Note these get the prefix, so the actual message will
                # be something like: "[commit abc123] Starting"
                LOG.info("Starting")
                out = fn(*args, **kwargs)
                LOG.info("Completed")
                return out
            finally:
                CURRENT_MESSAGE.reset(message_token)
                CURRENT_PREFIX.reset(prefix_token)

        return new_fn
