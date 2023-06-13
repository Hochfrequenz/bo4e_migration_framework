import logging
from contextvars import ContextVar, Token

logger: ContextVar[logging.Logger] = ContextVar("logger", default=logging.getLogger("bomf-unbound"))


def initialize_logger(context_specific_logger: logging.Logger) -> Token:
    """
    Initialize the logger context variable. You should use the returned token to clear the context variable after the
    migration to prevent too much memory usage.
    """
    return logger.set(context_specific_logger)


def clear_logger(token: Token):
    """
    Clear the logger context variable. You should call this method after the migration to prevent too much memory usage.
    """
    logger.reset(token)
