"""
Contains functionality to handle all the ValidationErrors and creating error IDs.
"""
import asyncio
import hashlib
import random
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, AsyncGenerator, Generic, Optional, TypeAlias

from bidict import bidict

from bomf.validation.core.types import DataSetT, MappedValidatorT, ValidatorT, validation_logger
from bomf.validation.core.validator import Parameters

if TYPE_CHECKING:
    from bomf.validation.core.execution import ValidationManager


def format_parameter_infos(
    validator: ValidatorT,
    provided_params: Parameters,
    start_indent: str = "",
    indent_step_size: str = "\t",
):
    """
    Nicely formats the parameter information for prettier output.
    """
    output = start_indent + "{"
    for param_name, param in validator.signature.parameters.items():
        is_provided = param_name in provided_params and provided_params[param_name].provided
        is_required = (
            validator.signature.parameters[param_name].default == validator.signature.parameters[param_name].empty
        )
        param_description = (
            f"value='{provided_params[param_name].value if is_provided else param.default}', "
            f"id='{provided_params[param_name].param_id if param_name in provided_params else 'unprovided'}', "
            f"{'required' if is_required else 'optional'}, "
            f"{'provided' if is_provided else 'unprovided'}"
        )

        output += f"\n{start_indent}{indent_step_size}{param_name}: {param_description}"
    return f"{output}\n{start_indent}" + "}"


_IdentifierType: TypeAlias = tuple[str, str, int]
_IDType: TypeAlias = int
_ERROR_ID_MAP: bidict[_IdentifierType, _IDType] = bidict()


def _get_identifier(exc: Exception) -> _IdentifierType:
    """
    Returns the module name and line number inside the function and its function name where the exception was
    originally raised.
    This tuple serves as identifier to create an error ID later on.
    """
    current_traceback = exc.__traceback__
    assert current_traceback is not None
    while current_traceback.tb_next is not None:
        current_traceback = current_traceback.tb_next
    raising_module_path = current_traceback.tb_frame.f_code.co_filename
    return (
        Path(raising_module_path).name,
        current_traceback.tb_frame.f_code.co_name,
        current_traceback.tb_lineno - current_traceback.tb_frame.f_code.co_firstlineno,
    )


def _generate_new_id(identifier: _IdentifierType, last_id: Optional[_IDType] = None) -> _IDType:
    """
    Generate a new random id with taking the identifier as seed. If last_id is provided it will be used as seed instead.
    """
    if last_id is not None:
        validation_logger.debug(
            "Duplicated ID for %s and %s. Generating new ID...", identifier, _ERROR_ID_MAP.inverse[last_id]
        )
        random.seed(last_id)
    else:
        module_name_hash = int(hashlib.blake2s((identifier[0] + identifier[1]).encode(), digest_size=4).hexdigest(), 16)
        random.seed(module_name_hash + identifier[2])
    # This range has no further meaning, but you have to define it.
    return random.randint(1_000_000, 9_999_999)


def _get_error_id(identifier: _IdentifierType) -> _IDType:
    """
    Returns a unique ID for the provided identifier.
    """
    if identifier not in _ERROR_ID_MAP:
        new_error_id = None
        while True:
            new_error_id = _generate_new_id(identifier, last_id=new_error_id)
            if new_error_id not in _ERROR_ID_MAP.inverse:
                break
        _ERROR_ID_MAP[identifier] = new_error_id
    return _ERROR_ID_MAP[identifier]


class ValidationError(RuntimeError):
    """
    A unified schema for error messages occurring during validation.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        message_detail: str,
        cause: Exception,
        data_set: DataSetT,
        mapped_validator: MappedValidatorT,
        validation_manager: "ValidationManager[DataSetT]",
        error_id: _IDType,
    ):
        provided_params = validation_manager.info.running_tasks[
            validation_manager.info.tasks[mapped_validator]
        ].current_provided_params
        message = (
            f"{error_id}: {message_detail}\n"
            f"\tDataSet: {data_set.__class__.__name__}(id={data_set.get_id()})\n"
            f"\tError ID: {error_id}\n"
            f"\tValidator function: {mapped_validator.name}"
        )
        if provided_params is not None:
            formatted_param_infos = format_parameter_infos(
                mapped_validator.validator,
                provided_params,
                start_indent="\t\t",
            )
            message += f"\n\tParameter information: \n{formatted_param_infos}"
        super().__init__(message)
        self.cause = cause
        self.data_set = data_set
        self.mapped_validator = mapped_validator
        self.validator_set = validation_manager
        self.error_id = error_id
        self.message_detail = message_detail
        self.provided_params = provided_params


# pylint: disable=too-few-public-methods
class ErrorHandler(Generic[DataSetT]):
    """
    This class provides functionality to easily log any occurring error.
    It can save one exception for each validator function.
    """

    def __init__(self, data_set: DataSetT):
        self.data_set = data_set
        self.excs: dict[MappedValidatorT, list[ValidationError]] = {}

    # pylint: disable=too-many-arguments
    async def catch(
        self,
        msg: str,
        error: Exception,
        mapped_validator: MappedValidatorT,
        validation_manager: "ValidationManager[DataSetT]",
        custom_error_id: Optional[int] = None,
    ):
        """
        Logs a new validation error with the defined message. The `error` parameter will be set as `__cause__` of the
        validation error.
        """
        error_id = _get_error_id(_get_identifier(error)) if custom_error_id is None else custom_error_id
        error_nested = ValidationError(
            msg,
            error,
            self.data_set,
            mapped_validator,
            validation_manager,
            error_id,
        )
        validation_logger.exception(
            str(error_nested),
            exc_info=error_nested,
        )
        async with asyncio.Lock():
            if mapped_validator not in self.excs:
                self.excs[mapped_validator] = []
            self.excs[mapped_validator].append(error_nested)

    @asynccontextmanager
    async def pokemon_catcher(
        self,
        mapped_validator: MappedValidatorT,
        validation_manager: "ValidationManager[DataSetT]",
        custom_error_id: Optional[int] = None,
    ) -> AsyncGenerator[None, None]:
        """
        This is an asynchronous context manager to easily implement a pokemon-catcher to catch any errors inside
        the body and envelops these inside ValidationErrors.
        """
        try:
            yield None
        except asyncio.TimeoutError as error:
            await self.catch(
                f"Timeout ("
                f"{validation_manager.validators[mapped_validator].timeout.total_seconds()}"  # type:ignore[union-attr]
                f"s) during execution",
                error,
                mapped_validator,
                validation_manager,
                custom_error_id,
            )
        except Exception as error:  # pylint: disable=broad-exception-caught
            await self.catch(str(error), error, mapped_validator, validation_manager, custom_error_id)
