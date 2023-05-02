"""
Contains a PathMappedValidator which gets the values from the data set in a very simple way. If you need a more
customizable MappedValidator you may be interested in the `QueryMappedValidator`.
"""
from typing import Any, Generator

from frozendict import frozendict

from bomf.validation.core import MappedValidator, Parameter, Parameters, ValidatorFunctionT, required_field
from bomf.validation.core.types import DataSetT, ValidatorT


class PathMappedValidator(MappedValidator[DataSetT, ValidatorFunctionT]):
    """
    This mapped validator class is for the "every day" usage. It simply queries the data set by the given attribute
    paths.
    """

    def __init__(self, validator: ValidatorT, *param_maps: dict[str, str] | frozendict[str, str]):
        super().__init__(validator)
        self.param_maps: tuple[frozendict[str, str], ...] = tuple(
            param_map if isinstance(param_map, frozendict) else frozendict(param_map) for param_map in param_maps
        )
        self._validate_param_maps()

    def _validate_param_maps(self):
        """
        Checks if the parameter maps match to the validator signature.
        """
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

    def __eq__(self, other):
        return (
            isinstance(other, PathMappedValidator)
            and self.validator == other.validator
            and self.param_maps == other.param_maps
        )

    def __ne__(self, other):
        return (
            not isinstance(other, PathMappedValidator)
            or self.validator != other.validator
            or self.param_maps != other.param_maps
        )

    def __hash__(self):
        return hash(self.param_maps) + hash(self.validator)

    def __repr__(self):
        return f"PathMappedValidator({self.validator.name}, {tuple(dict(param_map) for param_map in self.param_maps)})"

    def provide(self, data_set: DataSetT) -> Generator[Parameters[DataSetT] | Exception, None, None]:
        """
        Provides all parameter maps to the ValidationManager. If a parameter list could not be filled correctly
        an error will be yielded.
        """
        for param_map in self.param_maps:
            parameter_values: dict[str, Parameter] = {}
            skip = False
            for param_name, attr_path in param_map.items():
                try:
                    value: Any = required_field(data_set, attr_path, Any)
                    provided = True
                except AttributeError as error:
                    if param_name in self.validator.required_param_names:
                        query_error = AttributeError(f"{attr_path} not provided")
                        query_error.__cause__ = error
                        yield error
                        skip = True
                        break
                    value = self.validator.signature.parameters[param_name].default
                    provided = False
                parameter_values[param_name] = Parameter(
                    mapped_validator=self,
                    name=param_name,
                    param_id=attr_path,
                    value=value,
                    provided=provided,
                )
            if not skip:
                yield Parameters(self, **parameter_values)
