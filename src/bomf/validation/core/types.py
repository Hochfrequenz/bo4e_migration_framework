"""
Contains the types used in the validation framework
"""
import logging
from typing import TYPE_CHECKING, Any, Callable, Coroutine, TypeAlias, TypeVar

from bomf.model import Bo4eDataSet

if TYPE_CHECKING:
    from bomf.validation.core.validator import MappedValidator, Validator

validation_logger = logging.getLogger(__name__)
DataSetT = TypeVar("DataSetT", bound=Bo4eDataSet)
AsyncValidatorFunction: TypeAlias = Callable[..., Coroutine[Any, Any, None]]
SyncValidatorFunction: TypeAlias = Callable[..., None]
ValidatorFunction: TypeAlias = AsyncValidatorFunction | SyncValidatorFunction
ValidatorFunctionT = TypeVar("ValidatorFunctionT", SyncValidatorFunction, AsyncValidatorFunction)
ValidatorT: TypeAlias = "Validator[DataSetT, ValidatorFunctionT]"  # pylint: disable=invalid-name
MappedValidatorT: TypeAlias = "MappedValidator[DataSetT, ValidatorFunctionT]"  # pylint: disable=invalid-name
MappedValidatorSyncAsync: TypeAlias = (
    "MappedValidator[DataSetT, AsyncValidatorFunction] | MappedValidator[DataSetT, SyncValidatorFunction]"
)
