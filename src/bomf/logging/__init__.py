"""
Sets up the logging for the bomf package. The logger is stored inside a ContextVar to support concurrent processing
in e.g. web services.
"""
import logging
from contextvars import ContextVar, Token
from typing import Callable

logger: ContextVar[logging.Logger] = ContextVar("logger", default=logging.getLogger("bomf-unbound"))


def initialize_logger(context_specific_logger: logging.Logger) -> Callable[[], None]:
    """
    Initialize the logger context variable. You should use the returned teardown function to clear the context variable
    after the migration to prevent too much memory usage.
    """
    token = logger.set(context_specific_logger)

    def clear_logger():
        """
        Clear the logger context variable. You should call this method after the migration to prevent too much memory
        usage.
        """
        logger.reset(token)

    return clear_logger
