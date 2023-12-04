import datetime
import json
import logging
import logging.config

from asgi_correlation_id import CorrelationIdFilter


def loggers_init(settings, component: str):
    logging.config.dictConfig(settings.log_config)

    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():
        root_logger.addHandler(logging.StreamHandler())

    if component == "worker":
        # Handler maintaining a file for healthchecks.
        # Only relevant for worker component.
        if not [h for h in root_logger.handlers if isinstance(h, GWHandler)]:
            root_logger.addHandler(GWHandler(settings))

    # Additional fields to be included in JSON logs if set on log records.
    # Differs between components.
    extra_json_fields = {}
    if component == "web":
        # The ASGI correlation ID
        extra_json_fields["request_id"] = "correlation_id"
    elif component == "worker":
        # The dramatiq message ID
        extra_json_fields["message_id"] = "message_id"

    for handler in root_logger.handlers:
        datefmt = settings.log_config.get("datefmt")
        handler.setFormatter(JsonFormatter(datefmt, extra_json_fields))
        if component == "web":
            handler.addFilter(CorrelationIdFilter())


class GWHandler(logging.Handler):  # type: ignore
    """GWHandler's emit implementation just logs additional information to a
    file to indicate healthiness of worker(s).
    """

    def __init__(self, settings):
        super().__init__()
        self.settings = settings

    def emit(self, _):
        """Writes the current datetime to a file specified in settings.
        Discards the received record.
        """
        self.acquire()
        try:
            filepath = self.settings.worker_health_filepath
            with open(filepath, "w") as healthy:
                # The open mode is intentional. We don't need to append,
                # accumulating text and increasing the file's footprint, we
                # only need to touch the file so the changed date is updated.
                # What is written shouldn't matter either, but we'll record the
                # time of the write, as it may be interesting while debugging.
                healthy.write(str(datetime.datetime.utcnow()))
        finally:
            self.release()


class JsonFormatter(logging.Formatter):
    def __init__(self, datefmt=None, extra_fields=None):
        super().__init__()
        self.fmt = {
            "level": "levelname",
            "logger": "name",
            "time": "asctime",
            "message": "message",
            "event": "event",
            "success": "success",
        }
        self.fmt.update(extra_fields or {})
        self.datefmt = datefmt

    # Appended '_' on 'converter' because mypy doesn't approve of
    # overwriting a base class variable with another type.
    converter_ = datetime.datetime.fromtimestamp

    default_time_format = "%Y-%m-%d %H:%M:%S"
    default_msec_format = "%s.%03d"

    def formatTime(self, record, datefmt=None):
        ct = self.converter_(record.created, datetime.timezone.utc)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime(self.default_time_format)
            if self.default_msec_format:
                s = self.default_msec_format % (s, record.msecs)
        return s

    def formatMessage(self, record):
        return {k: record.__dict__.get(v) for k, v in self.fmt.items()}

    def format(self, record):
        record.message = record.getMessage()

        if "asctime" in self.fmt.values():
            record.asctime = self.formatTime(record, self.datefmt)

        d = self.formatMessage(record)

        if record.exc_info:
            record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            d["exc_info"] = record.exc_text

        if record.stack_info:
            d["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(d, sort_keys=True)
