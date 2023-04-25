import logging
from typing import TYPE_CHECKING, Any, Callable, Coroutine, TypeAlias, TypeVar

from bomf.model import Bo4eDataSet

if TYPE_CHECKING:
    from bomf.validation.core.validator import MappedValidator, Validator

validation_logger = logging.getLogger(__name__)
DataSetT = TypeVar("DataSetT", bound=Bo4eDataSet)
# QueryableObjectT = TypeVar("QueryableObjectT", bound=BaseModel)
AsyncValidatorFunction: TypeAlias = Callable[..., Coroutine[Any, Any, None]]
SyncValidatorFunction: TypeAlias = Callable[..., None]
ValidatorFunction: TypeAlias = AsyncValidatorFunction | SyncValidatorFunction
ValidatorFunctionT = TypeVar("ValidatorFunctionT", SyncValidatorFunction, AsyncValidatorFunction)
ValidatorT: TypeAlias = "Validator[DataSetT, ValidatorFunctionT]"
MappedValidatorT: TypeAlias = "MappedValidator[DataSetT, ValidatorFunctionT]"

# def _is_validator_type(
#     value: ValidatorType | tuple[ValidatorType, ParameterMapType] | _ValidatorMapInternIndexType
# ) -> TypeGuard[ValidatorType]:
#     """
#     Returns `True` if the provided value is of type `ValidatorType`. Otherwise, returns `False`.
#     """
#     try:
#         check_type("", value, ValidatorType)
#         return True
#     except TypeError:
#         return False
