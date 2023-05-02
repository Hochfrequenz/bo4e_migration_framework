"""
Contains functionality to build up a box of information around a validator function before registering it to a
ValidationManager. This reduces complexity inside the ValidationManager.
"""
import asyncio
import inspect
import types
from abc import ABC, abstractmethod
from typing import Any, Generator, Generic, TypeGuard, Union, overload

from frozendict import frozendict

from bomf.validation.core.types import (
    AsyncValidatorFunction,
    DataSetT,
    MappedValidatorT,
    SyncValidatorFunction,
    ValidatorFunctionT,
    ValidatorT,
    validation_logger,
)


class Validator(Generic[DataSetT, ValidatorFunctionT]):
    """
    Holds the actual validator function:
        - The parameter list must contain at least one element
        - The parameter list must be fully type hinted (they will be used for an explicit type check)
        - The parameter list must not contain POSITIONAL_ONLY parameters
        - The return type and value will be ignored (prints a warning if present)
        - It can be either sync or async
        - A validator will be executed asynchronously iff:
            - The validator is async
            - The validator has dependencies which are not finished yet (See `ValidationManager.register`
              for more details)

    This class will collect some information about the validator function for the ValidationManager.
    """

    def __init__(self, validator_func: ValidatorFunctionT):
        validator_signature = inspect.signature(validator_func)
        if len(validator_signature.parameters) == 0:
            raise ValueError("The validator function must take at least one argument")
        if any(param.kind == param.POSITIONAL_ONLY for param in validator_signature.parameters.values()):
            raise ValueError("The function parameters must not contain positional only parameters")
        if validator_signature.return_annotation not in (None, validator_signature.empty):
            validation_logger.warning(
                "Annotated return type is not None (the return value will be ignored): %s(...) -> %s",
                validator_func.__name__,
                validator_signature.return_annotation,
            )
        param: inspect.Parameter
        for param in validator_signature.parameters.values():
            if param.annotation == param.empty:
                raise ValueError(f"The parameter {param.name} has no annotated type.")
            if isinstance(param.annotation, types.UnionType):
                # This is a little workaround because typeguards check_type function doesn't work with '|' notation
                # but with Union.
                param._annotation = Union[*param.annotation.__args__]  # type: ignore[attr-defined]

        self.func: ValidatorFunctionT = validator_func
        self.signature = validator_signature
        self.param_names = set(validator_signature.parameters.keys())
        self.required_param_names = {
            param_name
            for param_name in self.param_names
            if validator_signature.parameters[param_name].default == validator_signature.parameters[param_name].empty
        }
        self.optional_param_names = self.param_names - self.required_param_names
        self.name = validator_func.__name__
        self.is_async: bool = asyncio.iscoroutinefunction(validator_func)
        validation_logger.debug("Created validator: %s", self.name)

    def __hash__(self):
        return hash(self.func)

    def __eq__(self, other):
        return isinstance(other, Validator) and self.func == other.func

    def __ne__(self, other):
        return not isinstance(other, Validator) or self.func != other.func

    def __repr__(self) -> str:
        return f"Validator({self.name})"


# pylint: disable=too-few-public-methods
class Parameter(Generic[DataSetT]):
    """
    Encapsulates a single parameter. A parameter must have an ID for better error output.
    """

    # pylint: disable=too-many-arguments
    def __init__(self, mapped_validator: MappedValidatorT, name: str, value: Any, param_id: str, provided: bool):
        self.mapped_validator = mapped_validator
        self.name = name
        self.value = value
        self.param_id = param_id
        self.provided = provided

    def __repr__(self) -> str:
        return f"Parameter({self.param_id} -> {self.name}: {self.value})"


class Parameters(frozendict[str, Parameter], Generic[DataSetT]):
    """
    A customized dictionary to hold the parameter list of a validator. Each parameter must refer to the same mapped
    validator.
    """

    mapped_validator: MappedValidatorT
    param_dict: dict[str, Any]

    def __new__(cls, mapped_validator: MappedValidatorT, /, *args, **kwargs):
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, mapped_validator: MappedValidatorT, /, **kwargs):
        super().__init__(**kwargs)
        mapped_validators = set(param.mapped_validator for param in self.values())
        if len(mapped_validators) > 1 or len(mapped_validators) == 1 and mapped_validators.pop() != mapped_validator:
            raise ValueError("You cannot add parameters with different providers")

        param_dict: dict[str, Any] = {param.name: param.value for param in self.values() if param.provided}

        # hijacke the frozendict to enable to set attributes to this subclass
        dict.__setattr__(self, "mapped_validator", mapped_validator)
        dict.__setattr__(self, "param_dict", param_dict)


class MappedValidator(ABC, Generic[DataSetT, ValidatorFunctionT]):
    """
    A validator which is capable to fill the parameter list by querying a data set instance.
    """

    def __init__(self, validator: ValidatorT):
        self.validator: ValidatorT = validator
        self.name = validator.name
        validation_logger.debug("Created ParameterProvider: %s, %s", self.__class__.__name__, self.validator.name)

    @abstractmethod
    def provide(self, data_set: DataSetT) -> Generator[Parameters[DataSetT] | Exception, None, None]:
        """
        A generator function to return on each yield a parameter list to fill the validator function with it.
        If a parameter list could not be built you should yield an exception instead but your generator should not
        raise any exceptions because this will cause the generator to be destroyed (because python does not know where
        to continue in the generator code after raising an exception).

        Note: You don't have to supply all parameters to support optional ones. However, if there are required
        parameters not provided the ValidationManager will catch a ValidationError.
        """

    def __repr__(self) -> str:
        return f"MappedValidator({self.name})"

    @property
    def is_async(self) -> bool:
        """True if the validator function is declared as async"""
        return self.validator.is_async


@overload
def is_async(
    validator: "MappedValidatorT",
) -> TypeGuard["MappedValidator[Any, AsyncValidatorFunction]"]:
    ...


@overload
def is_async(
    validator: "ValidatorT",
) -> TypeGuard["Validator[Any, AsyncValidatorFunction]"]:
    ...


def is_async(
    validator: "ValidatorT | MappedValidatorT",
) -> TypeGuard["Validator[Any, AsyncValidatorFunction] | MappedValidator[Any, AsyncValidatorFunction]"]:
    """True if the validator function is declared as async"""
    if isinstance(validator, Validator):
        return validator.is_async
    return validator.validator.is_async


def is_sync(
    validator: MappedValidatorT,
) -> TypeGuard[MappedValidator[Any, SyncValidatorFunction]]:
    """True if the validator function is declared as sync"""
    return not validator.validator.is_async
