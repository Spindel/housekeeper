import sys
import threading
import logging

from contextlib import contextmanager

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

_log = structlog.get_logger(__name__)


@contextmanager
def log_state(*args, **kws):
    bind_contextvars(**kws)
    try:
        yield
    finally:
        unbind_contextvars(*kws)


def add_thread_name(logger, method_name, event_dict):
    """Emulates the threadName data from stdlib"""
    thread = threading.current_thread()
    event_dict["_thread_name"] = thread.name
    return event_dict


def add_empty_events(logger, method_name, event_dict):
    """In case structlog got called with an empty event.

    The JSON Renderer works with empty events, but the ConsoleRenderer errors
    out with an exception.
    """
    if "event" not in event_dict:
        event_dict["event"] = ""
    return event_dict


def setup_logging(terminal=False):
    """Global state. Eat it."""
    iso_timestamps = structlog.processors.TimeStamper(fmt="iso", key="_timestamp")

    if terminal:
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        # We assume that our pipelines aren't ascii-only, and we want to see
        # emojis.
        renderer = structlog.processors.JSONRenderer(sort_keys=True, ensure_ascii=False)

    shared_processors = [
        # merge_contextvars needs to be first
        structlog.contextvars.merge_contextvars,
        iso_timestamps,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        add_thread_name,
        add_empty_events,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.format_exc_info,
        structlog.dev.set_exc_info,
    ]

    # Filter by level should be first
    structlog_processors = [structlog.stdlib.filter_by_level]
    # Our shared processors run for both std library logs and our own
    structlog_processors.extend(shared_processors)
    structlog_processors.append(
        # Needs to be the last processor
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter
    )

    structlog.configure(
        processors=structlog_processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure python.logging to use our Formatter
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        # Add our custom filters
        foreign_pre_chain=shared_processors,
    )
    structlog.contextvars.clear_contextvars()

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    root_logger.info("stdlib logging set up")
    _log.info("structlog logging", logging_setup=True)
    # logging_selftest()


def raises(arguments="Many"):
    _log.info("About to go boom", function=raises)
    raise Exception("Raising Exception")


def logging_selftest():
    """This should just be a brief demo to show  how our config works"""

    root_logger = logging.getLogger()
    other_logger = root_logger.getChild("other")

    root_logger.info("standard library root logger")
    other_logger.info("standard library, other logger")
    _log.info("structlog, module logger", structured="Value")

    # Empty log event should work
    _log.info(empty="True")
    try:
        raises()
    except Exception:
        other_logger.exception("Exception log")
        _log.exception(exception="intentional")

    _log.info("📗📓📖", emoji="📙📒")
    other_logger.info("📗📓📖: %s", "📙📒")
    value = 512
    other_logger.info("Using percent formatting of %s to text", value)

    # Log level test
    other_logger.setLevel(logging.INFO)
    _log.setLevel(logging.ERROR)

    other_logger.info("Structlog: error, stdlib: info")
    _log.info(structlog_level="error", stdlib_level="info")

    other_logger.setLevel(logging.ERROR)
    _log.setLevel(logging.INFO)

    other_logger.info("Structlog: info, stdlib: error")
    _log.info(structlog_level="info", stdlib_level="error")

    _log.debug("Logging, debug, initialized")
    _log.info("log.info initialized")
    _log.warning("log.warning initialized")

    sublogger = _log.getChild("Thread")
    sublogger.info("Go go")
