import logging

import pytest

from bomf.logging import initialize_logger


@pytest.fixture(scope="session", autouse=True)
def setup_log_context_var_fixture():
    """
    Set up the logging configuration. This fixture is automatically used by pytest.
    """
    initialize_logger(logging.getLogger("bomf-tests"))
    print("Initialized logger", flush=True)
