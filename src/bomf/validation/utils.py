"""
Contains some useful utility functions to be used in validator functions.
"""
import inspect
from typing import Any, Optional, TypeVar

from typeguard import check_type

from bomf.validation.core import Parameter, Parameters, ValidationManager


def param(param: str) -> "Parameter":
    call_stack = inspect.stack()
    # call_stack[0] -> this function
    # call_stack[1] -> must be the validator function
    # call_stack[2] -> should be either `_execute_sync_validator` or `_execute_async_validator`
    try:
        validation_manager: "ValidationManager" = call_stack[2].frame.f_locals["self"]
    except KeyError:
        raise RuntimeError(
            "You can call this function only directly from inside a function"
            "which is executed by the validation framework"
        )

    provided_params: "Parameters" = validation_manager.info.current_provided_params
    assert provided_params is not None, "This shouldn't happen"
    if param not in provided_params:
        raise RuntimeError(
            f"Parameter provider {validation_manager.info.current_param_provider} "
            f"did not provide parameter information for parameter '{param}'"
        )
    return provided_params[param] if param in provided_params else None
