import asyncio
import inspect
import types
from abc import ABC, abstractmethod
from typing import Any, Generator, Generic, TypeGuard, Union

from frozendict import frozendict

from bomf.validation.core.types import (
    AsyncValidatorFunction,
    DataSetT,
    SyncValidatorFunction,
    ValidatorFunctionT,
    validation_logger,
)
from bomf.validation.core.utils import optional_field, required_field


class Validator(Generic[DataSetT, ValidatorFunctionT]):
    def __init__(self, validator_func: ValidatorFunctionT):
        validator_signature = inspect.signature(validator_func)
        if len(validator_signature.parameters) == 0:
            raise ValueError("The function must take at least one argument")
        if any(param.kind == param.POSITIONAL_ONLY for param in validator_signature.parameters.values()):
            raise ValueError("The function parameters must not contain positional only parameters")
        if validator_signature.return_annotation not in (None, validator_signature.empty):
            validation_logger.warning(
                "Annotated return type is not None (the return value will be ignored): %s(...) -> %s",
                validator_func.__name__,
                validator_signature.return_annotation,
            )
        for param in validator_signature.parameters.values():
            if param.annotation == param.empty:
                raise ValueError(f"The parameter {param.name} has no annotated type.")
            if isinstance(param.annotation, types.UnionType):
                # This is a little workaround because typeguards check_type function doesn't work with '|' notation
                # but with Union.
                param._annotation = Union[*param.annotation.__args__]

        self.func: ValidatorFunctionT = validator_func
        self.signature = validator_signature
        self.param_names = set(validator_signature.parameters.keys())
        self.required_param_names = {
            param
            for param in self.param_names
            if validator_signature.parameters[param].default == validator_signature.parameters[param].empty
        }
        self.optional_param_names = self.param_names - self.required_param_names
        self.name = validator_func.__name__
        self._is_async = asyncio.iscoroutinefunction(validator_func)
        validation_logger.debug("Created validator: %s", self.name)

    @property
    def is_async(
        self: "Validator[DataSetT, ValidatorFunctionT]",
    ) -> TypeGuard["Validator[DataSetT, AsyncValidatorFunction]"]:
        return self._is_async

    @property
    def is_sync(
        self: "Validator[DataSetT, ValidatorFunctionT]",
    ) -> TypeGuard["Validator[DataSetT, SyncValidatorFunction]"]:
        return not self._is_async

    def __hash__(self):
        return hash(self.func)

    def __eq__(self, other):
        return isinstance(other, Validator) and self.func == other.func

    def __ne__(self, other):
        return not isinstance(other, Validator) or self.func != other.func


class Parameter(Generic[DataSetT]):
    def __init__(self, provider: "ParameterProvider[DataSetT]", name: str, value: Any, param_id: str, provided: bool):
        self.provider = provider
        self.name = name
        self.value = value
        self.id = param_id
        self.provided = provided


class Parameters(frozendict[str, Parameter], Generic[DataSetT]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        provider_set = set(param.provider for param in self.values())
        if len(provider_set) != 1:
            raise ValueError("You cannot add parameters with different providers")
        provider: "ParameterProvider[DataSetT]" = provider_set.pop()
        param_dict: dict[str, Any] = {param.name: param.value for param in self.values() if param.provided}

        self.provider: "ParameterProvider[DataSetT]"
        self.param_dict: dict[str, Any]
        dict.__setattr__(self, "provider", provider)
        dict.__setattr__(self, "param_dict", param_dict)


class ParameterProvider(ABC, Generic[DataSetT]):
    def __init__(self, validator: Validator[DataSetT, ValidatorFunctionT]):
        self.validator = validator
        validation_logger.debug("Created ParameterProvider: %s, %s", self.__class__.__name__, self.validator.name)

    @abstractmethod
    def provide(self, data_set: DataSetT) -> Generator[Parameters[DataSetT] | Exception, None, None]:
        ...


class PathParameterProvider(ParameterProvider[DataSetT]):
    def __init__(self, validator: Validator[DataSetT, ValidatorFunctionT], *param_maps: dict[str, str]):
        super().__init__(validator)
        self.param_maps: list[dict[str, str]] = list(param_maps)
        self._validate_param_maps()

    def _validate_param_maps(self):
        for param_map in self.param_maps:
            mapped_params = set(param_map.keys())
            if not mapped_params <= self.validator.param_names:
                raise ValueError(
                    f"{self.validator.name} has no parameter(s) {mapped_params - self.validator.param_names}"
                )
            if not self.validator.required_param_names <= mapped_params:
                raise ValueError(
                    f"{self.validator.name} misses parameter(s) {self.validator.required_param_names - mapped_params}"
                )

    def provide(self, data_set: DataSetT) -> Generator[Parameters[DataSetT] | Exception, None, None]:
        for param_map in self.param_maps:
            parameter_values: dict[str, Parameter] = {}
            skip = False
            for param_name, attr_path in param_map.items():
                try:
                    value = required_field(
                        data_set, attr_path, self.validator.signature.parameters[param_name].annotation
                    )
                    provided = True
                except (AttributeError, TypeError) as error:
                    if param_name in self.validator.required_param_names:
                        yield error
                        skip = True
                        break
                    value = self.validator.signature.parameters[param_name].default
                    provided = False
                parameter_values[param_name] = Parameter(
                    provider=self,
                    name=param_name,
                    param_id=attr_path,
                    value=value,
                    provided=provided,
                )
            if not skip:
                yield Parameters(**parameter_values)
