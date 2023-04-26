"""
Contains some useful utility functions to be used in validator functions.
"""
import inspect
from typing import Optional

from bomf.validation.core import Parameter, Parameters, ValidationManager


def param(param_name: str) -> Parameter:
    """
    This function can only be used inside validator functions and will only work if the function is executed by the
    validation framework. If you run the validator function "by yourself" or use this function elsewhere it will
    raise a RuntimeError.
    When using inside a validator function, this function returns the Parameter object of the provided parameter name.
    E.g.:
    ```
    def validate_email(e_mail: Optional[str] = None):
        param_e_mail = param("e_mail")
        assert param_e_mail.name == "e_mail"
        if param_e_mail.provided:
            my_e_mail_validation(e_mail)
    ```
    """
    call_stack = inspect.stack()
    # call_stack[0] -> this function
    # call_stack[1] -> must be the validator function
    # call_stack[2] -> should be either `_execute_sync_validator` or `_execute_async_validator`
    validation_manager: Optional[ValidationManager] = None
    try:
        validation_manager = call_stack[2].frame.f_locals["self"]
        if not isinstance(validation_manager, ValidationManager):
            validation_manager = None
    except KeyError:
        pass

    if validation_manager is None:
        raise RuntimeError(
            "You can call this function only directly from inside a function "
            "which is executed by the validation framework"
        )

    provided_params: Optional[Parameters] = validation_manager.info.current_provided_params
    assert provided_params is not None, "This shouldn't happen"
    if param_name not in provided_params:
        raise RuntimeError(
            f"Parameter provider {validation_manager.info.current_mapped_validator} "
            f"did not provide parameter information for parameter '{param_name}'"
        )
    return provided_params[param_name]
