"""
Contains core functionality to validate arbitrary Bo4eDataSets
"""
import asyncio
import hashlib
import inspect
import logging
import random
import types
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Coroutine, Generic, Optional, TypeAlias, TypeGuard, TypeVar, Union

from bidict import bidict
from frozendict import frozendict
from typeguard import check_type

from bomf.model import Bo4eDataSet

_logger = logging.getLogger(__name__)
DataSetT = TypeVar("DataSetT", bound=Bo4eDataSet)
ValidatorType: TypeAlias = Callable[..., Coroutine[Any, Any, None]]
ParameterMapType: TypeAlias = dict[str, str]
_ParameterMapInternType: TypeAlias = frozendict[str, str]
_ValidatorMapInternIndexType: TypeAlias = tuple[ValidatorType, _ParameterMapInternType]


def _is_validator_type(
    value: ValidatorType | tuple[ValidatorType, ParameterMapType] | _ValidatorMapInternIndexType
) -> TypeGuard[ValidatorType]:
    """
    Returns `True` if the provided value is of type `ValidatorType`. Otherwise, returns `False`.
    """
    try:
        check_type("", value, ValidatorType)
        return True
    except TypeError:
        return False


def format_parameter_infos(
    param_mapping: _ParameterMapInternType,
    params_infos: "dict[str, ValidatorParamInfos]",
    start_indent: str = "",
    indent_step_size: str = "\t",
):
    """
    Nicely formats the parameter information for prettier output.
    """
    output = start_indent + "{"
    for param_name in param_mapping:
        output += f"\n{start_indent}{indent_step_size}{param_name}: {params_infos[param_name].get_summary()}"
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
        _logger.debug("Duplicated ID for %s and %s. Generating new ID...", identifier, _ERROR_ID_MAP.inverse[last_id])
        random.seed(last_id)
    else:
        module_name_hash = int(hashlib.blake2s((identifier[0] + identifier[1]).encode(), digest_size=4).hexdigest(), 16)
        random.seed(module_name_hash + identifier[2])
    # This range has no further meaning, but you have to define it.
    return random.randint(1, 1000000000)


async def _get_error_id(identifier: _IdentifierType) -> _IDType:
    """
    Returns a unique ID for the provided identifier.
    """
    if identifier not in _ERROR_ID_MAP:
        new_error_id = None
        async with asyncio.Lock():
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
        data_set: Bo4eDataSet,
        validator: _ValidatorMapInternIndexType,
        validator_set: "ValidatorSet",
        error_id: _IDType,
    ):
        formatted_param_infos = format_parameter_infos(
            validator[1], validator_set.field_validators[validator].param_infos, start_indent="\t\t"
        )
        message = (
            f"{error_id}: {message_detail}\n"
            f"\tDataSet: {data_set.__class__.__name__}(id={data_set.get_id()})\n"
            f"\tError ID: {error_id}\n"
            f"\tValidator function: {validator[0].__name__}\n"
            f"\tParameter information: \n{formatted_param_infos}"
        )
        super().__init__(message)
        self.__cause__ = cause
        self.data_set = data_set
        self.validator = validator
        self.validator_set = validator_set
        self.error_id = error_id


# pylint: disable=too-few-public-methods
class ErrorHandler:
    """
    This class provides functionality to easily log any occurring error.
    It can save one exception for each validator function.
    """

    def __init__(self, data_set: Bo4eDataSet):
        self.data_set = data_set
        self.excs: dict[_ValidatorMapInternIndexType, Exception] = {}

    # pylint: disable=too-many-arguments
    async def catch(
        self,
        msg: str,
        error: Exception,
        validator: _ValidatorMapInternIndexType,
        validator_set: "ValidatorSet",
        custom_error_id: Optional[int] = None,
    ):
        """
        Logs a new validation error with the defined message. The `error` parameter will be set as `__cause__` of the
        validation error.
        """
        error_id = await _get_error_id(_get_identifier(error)) if custom_error_id is None else custom_error_id
        error_nested = ValidationError(
            msg,
            error,
            self.data_set,
            validator,
            validator_set,
            error_id,
        )
        _logger.exception(
            str(error_nested),
            exc_info=error_nested,
        )
        self.excs[validator] = error_nested


@dataclass
class ValidatorParamInfos:
    """
    Contains some information about a specific parameter.
    """

    param_type: Any
    # Contains the annotated type of the parameter
    attribute_path: list[str]
    # Contains the attribute path defined by `parameter_map` when registering the validator function
    required: bool = True
    # If the parameter has no default value it is considered as required otherwise as optional
    provided: bool = True
    # This field is useful for optional parameters to determine if the parameter value was provided or set to the
    # default value. This resolves the ambiguity occurring when the provided value equals the default value of the
    # parameter.
    # provided is filled with senseful data during validation in _fill_params
    value: Any = None
    # This field will contain the provided (or default value) for the parameter.

    def get_summary(self) -> str:
        """
        Returns a string representation to summarize information about the parameter. Mainly used by error handling.
        """
        return (
            f"value='{str(self.value)}', "
            f"attribute_path='{'.'.join(self.attribute_path)}', "
            f"{'required' if self.required else 'optional'}, "
            f"{'provided' if self.provided else 'unprovided'}"
        )

    def __repr__(self):
        return f"ValidatorParamInfos({self.get_summary()})"


@dataclass
class _ValidatorInfos:
    """
    This dataclass holds information to a registered validator function.
    You can specify dependent validators which will always be completed before executing the validator.
    If the validator can't complete in time (denoted by timeout in seconds) the execution will be interrupted and an
    error will be raised. If the timeout is not specified (`None`), the validator will not be stopped.
    """

    depends_on: list[_ValidatorMapInternIndexType]
    # Contains a list of validator functions on which this validator function depends.
    timeout: Optional[timedelta]
    # Contains the (optional) timeout time after which the validator function will be cancelled.
    param_infos: dict[str, ValidatorParamInfos]
    # Contains infos about all parameters of a validator function. The key indicates the parameter name.
    special_params: dict[str, Any]
    # Contains the name and the annotated type of special parameters used in the validator function.


class ValidatorSet(Generic[DataSetT]):
    """
    This class contains functionality to register and execute a set of validator functions.
    Note that you can define multiple `ValidatorSet`s for a data set just by creating another instance and registering
    other validator functions.
    Note: You have to define the generic data set type. Otherwise, registering functions will raise an AttributeError.
    """

    def __init__(self):
        self.field_validators: dict[_ValidatorMapInternIndexType, _ValidatorInfos] = {}
        self._data_set_type: Optional[type[DataSetT]] = None
        self.special_params: dict[str, Any] = {"_param_infos": dict[str, ValidatorParamInfos]}

    @property
    def data_set_type(self) -> type[DataSetT]:
        """
        Holds the dataset type.
        """
        if self._data_set_type is None:
            if not hasattr(self, "__orig_class__"):
                raise TypeError("You have to use an instance of ValidatorSet and define the generic type.")
            # pylint: disable=no-member
            self._data_set_type = self.__orig_class__.__args__[0]  # type:ignore[attr-defined]
            # If this raises an AttributeError, you probably have forgotten to specify the dataset type as generic
            # argument.
        return self._data_set_type

    def get_map_indices(self, validator_func: ValidatorType) -> list[_ValidatorMapInternIndexType]:
        """
        Find all registered map indices for a given validator function. There can be multiple results if the same
        validator function got registered multiple times but with different parameter mappings.
        """
        matching_indices: list[_ValidatorMapInternIndexType] = []
        for map_index in self.field_validators:
            if map_index[0] == validator_func:
                matching_indices.append(map_index)
        return matching_indices

    def _narrow_supplied_dependencies(
        self, depends_on: list[ValidatorType | tuple[ValidatorType, ParameterMapType] | _ValidatorMapInternIndexType]
    ) -> list[_ValidatorMapInternIndexType]:
        """
        If a dependency has no parameter map explicitly defined, this functions tries to determine the correct
        validator. If the same validator got registered multiple times you have to define the parameter map. Otherwise,
        this function will raise a ValueError. It will also raise a ValueError if the validator function isn't
        registered at all.
        """
        narrowed_depends_on: list[_ValidatorMapInternIndexType] = []
        narrowed_dependency: _ValidatorMapInternIndexType
        for dependency in depends_on:
            if _is_validator_type(dependency):
                possible_deps = self.get_map_indices(dependency)
                if len(possible_deps) == 0:
                    raise ValueError(f"The specified dependency is not registered: {dependency.__name__}")
                if len(possible_deps) > 1:
                    raise ValueError(
                        f"The dependency {dependency.__name__} got registered multiple times. You have "
                        "to define the parameter mapping for the dependency to resolve the ambiguity."
                    )
                narrowed_dependency = possible_deps[0]
            else:
                assert isinstance(dependency, tuple)  # make mypy happy
                if not isinstance(dependency[1], frozendict):
                    narrowed_dependency = (dependency[0], frozendict(dependency[1]))
                else:
                    narrowed_dependency = dependency

            if narrowed_dependency not in self.field_validators:
                raise ValueError(f"The specified dependency is not registered: {narrowed_dependency}")
            narrowed_depends_on.append(narrowed_dependency)
        return narrowed_depends_on

    # pylint: disable=too-many-branches,too-many-locals
    def register(
        self,
        validator_func: ValidatorType,
        parameter_map: ParameterMapType,
        depends_on: Optional[
            list[ValidatorType | tuple[ValidatorType, ParameterMapType] | _ValidatorMapInternIndexType]
        ] = None,
        timeout: Optional[timedelta] = None,
    ) -> None:
        """
        Register a new validator function to call upon running `validate`. It checks if the provided validator function
        is valid in the first place. If the function is invalid a ValueError will be raised.
        The validator function has to by async. The return type is irrelevant and will be ignored.
        The validators arguments are mapped onto the dataset using `parameter_map`. `parameter_map` must be a dictionary
        with all parameter names as the keys of this dictionary. The respective value corresponds to an attribute
        inside the data set in point notation. This means of course that the function must be fully type hinted and the
        type hints of the function arguments should match the type hints of the respective data.

        E.g. if you have a data set with field x of type ClassX and this type
        holds an attribute y of type int, you can reference this with "x.y":
        ```
        async def my_validator_name(my_y: int):
            ...

        validator_set = ValidatorSet[MyDataSet]()
        validator_set.register(my_validator_name, {"my_y", "x.y"})
        ```

        All validator functions will be executed concurrently by default. However, if you want to execute certain
        validators first, you can define this functions in the `depends_on` argument. The validator will then be
        executed when all dependent validators have completed.
        You can also define a timeout. The validator will be cancelled if it doesn't complete in time.
        Note that if you define dependent validators the execution time of these will not be counted
        for the timeout on this validator.

        You can also use special parameters in your validator function. Currently, there is only one:
            _param_infos: dict[str, ValidatorParamInfos]
                This parameter contains information about all parameters of the validator function. See
                ValidatorParamInfos for more details.
        """
        if depends_on is None:
            depends_on = []
        narrowed_depends_on: list[_ValidatorMapInternIndexType] = self._narrow_supplied_dependencies(depends_on)

        if not asyncio.iscoroutinefunction(validator_func):
            raise ValueError("The provided validator function has to be a coroutine (e.g. use async).")
        if any(mapped_param in self.special_params for mapped_param in parameter_map):
            raise ValueError(
                "Special parameters cannot be mapped. "
                f"The following parameters are reserved: {list(self.special_params.keys())}"
            )

        validator_signature = inspect.signature(validator_func)
        validator_params_for_mapping = [
            param for param in validator_signature.parameters if param not in self.special_params
        ]
        if validator_params_for_mapping != list(parameter_map.keys()):
            raise ValueError(
                f"The parameter list of the validator function must match the parameter_map. "
                f"{validator_params_for_mapping} != {list(parameter_map.keys())}"
            )
        if len(validator_params_for_mapping) == 0:
            raise ValueError("The validator function must take at least one argument.")

        validator_param_infos: dict[str, ValidatorParamInfos] = {}
        for param_name, attribute_path in parameter_map.items():
            param_annotation = validator_signature.parameters[param_name].annotation
            if param_annotation == validator_signature.empty:
                raise ValueError(f"The parameter {param_name} has no annotated type.")
            if isinstance(param_annotation, types.UnionType):
                # This is a little workaround because typeguards check_type function doesn't work with '|' notation
                # but with Union.
                param_annotation = Union[*param_annotation.__args__]
            default_value = validator_signature.parameters[param_name].default
            required = default_value == validator_signature.parameters[param_name].empty
            validator_param_infos[param_name] = ValidatorParamInfos(
                attribute_path=attribute_path.split("."),
                required=required,
                param_type=param_annotation,
                value=default_value if not required else None,
            )
        validator_special_params: dict[str, Any] = {}
        for param_name, param in validator_signature.parameters.items():
            if param.annotation == validator_signature.empty:
                raise ValueError(f"The parameter {param_name} has no annotated type.")
            if param_name in self.special_params:
                validator_special_params[param_name] = param.annotation

        _logger.debug("Registered validator function: %s", validator_func.__name__)
        self.field_validators[(validator_func, frozendict(parameter_map))] = _ValidatorInfos(
            depends_on=narrowed_depends_on,
            timeout=timeout,
            param_infos=validator_param_infos,
            special_params=validator_special_params,
        )

    def _fill_params(self, validator: _ValidatorMapInternIndexType, data_set: DataSetT) -> Coroutine[Any, Any, None]:
        """
        This function fills the arguments of the validator function with the respective values in the data set
        and returns the coroutine.
        """
        arguments: dict[str, Any] = {}
        for param_name, param_infos in self.field_validators[validator].param_infos.items():
            current_obj: Any = data_set
            param_infos.provided = True
            for index, attr_name in enumerate(param_infos.attribute_path):
                try:
                    current_obj = getattr(current_obj, attr_name)
                except AttributeError as exc:
                    param_infos.provided = False
                    if param_infos.required:
                        current_path = ".".join([self.data_set_type.__name__, *param_infos.attribute_path[0:index]])
                        raise AttributeError(
                            f"{param_name} is required but not existent in the provided data set. "
                            f"Couldn't find {attr_name} in {current_path}."
                        ) from exc
                    break
            if param_infos.provided:
                arguments[param_name] = current_obj
                check_type(param_name, current_obj, param_infos.param_type)
                param_infos.value = current_obj
        for special_param_name, special_param_type in self.field_validators[validator].special_params.items():
            match special_param_name:
                case "_param_infos":
                    arguments[special_param_name] = self.field_validators[validator].param_infos
                    check_type(
                        special_param_name,
                        self.field_validators[validator].param_infos,
                        self.special_params[special_param_name],
                    )
                case _:
                    raise NotImplementedError(f"No implementation for special parameter {special_param_name}")
            check_type(special_param_name, arguments[special_param_name], special_param_type)
        return validator[0](**arguments)

    async def _prepare_coroutines(
        self, data_set: DataSetT, error_handler: ErrorHandler
    ) -> dict[_ValidatorMapInternIndexType, Coroutine[Any, Any, None]]:
        """
        This function fills the arguments of all validator functions with the respective values in the data set
        and returns the coroutines for each validator function as a dictionary.
        """
        coroutines: dict[_ValidatorMapInternIndexType, Coroutine[Any, Any, None]] = {}
        for validator in self.field_validators:
            try:
                coroutines[validator] = self._fill_params(validator, data_set)
            except (AttributeError, TypeError) as error:
                await error_handler.catch(
                    f"Couldn't fill in parameter: {error}",
                    error,
                    validator,
                    self,
                    custom_error_id=1,
                )
        return coroutines

    # pylint: disable=too-many-arguments
    async def _register_task(
        self,
        validator: _ValidatorMapInternIndexType,
        validator_infos: _ValidatorInfos,
        task_group: asyncio.TaskGroup,
        task_schedule: dict[_ValidatorMapInternIndexType, asyncio.Task[None]],
        coroutines: dict[_ValidatorMapInternIndexType, Coroutine[Any, Any, None]],
        error_handler: ErrorHandler,
    ) -> None:
        """
        This recursive method registers a new task for the given validator function in the task group.
        It has to be recursive to ensure that if the validator function depends on other validators that these have
        already registered tasks - or register them if necessary.
        """
        dep_tasks: dict[_ValidatorMapInternIndexType, asyncio.Task[None]] = {}
        for dep in validator_infos.depends_on:
            if dep not in task_schedule:
                await self._register_task(
                    dep, self.field_validators[dep], task_group, task_schedule, coroutines, error_handler
                )
            dep_tasks[dep] = task_schedule[dep]

        assert len(dep_tasks) == len(validator_infos.depends_on)

        async def _wrapper() -> Any:
            if len(dep_tasks) > 0:
                await asyncio.wait(dep_tasks.values(), return_when=asyncio.ALL_COMPLETED)
            dep_exceptions: dict[_ValidatorMapInternIndexType, Exception] = {
                _dep: error_handler.excs[_dep] for _dep in dep_tasks if _dep in error_handler.excs
            }
            if len(dep_exceptions) > 0:
                await error_handler.catch(
                    "Execution abandoned due to failing dependent validators: "
                    f"{', '.join(_dep[0].__name__ for _dep in dep_exceptions)}",
                    RuntimeError(str(list(dep_exceptions.keys()))),
                    validator,
                    self,
                    custom_error_id=2,
                )
                return
            try:
                return await asyncio.wait_for(
                    coroutines[validator],
                    validator_infos.timeout.total_seconds() if validator_infos.timeout is not None else None,
                )
            except TimeoutError as error:
                assert validator_infos.timeout is not None  # This shouldn't happen, but type checker cries
                await error_handler.catch(
                    f"Timeout ({validator_infos.timeout.total_seconds()}s) during execution.",
                    error,
                    validator,
                    self,
                    custom_error_id=3,
                )
            except Exception as error_in_validator:  # pylint: disable=broad-exception-caught
                await error_handler.catch(
                    str(error_in_validator),
                    error_in_validator,
                    validator,
                    self,
                )

        task_schedule[validator] = task_group.create_task(_wrapper())

    async def validate(self, *data_sets: DataSetT) -> None:
        """
        Validates each of the provided data set instances onto the registered validator functions.
        Any errors occurring during validation will be collected and raised together as ExceptionGroup.
        If a validator depends on other validators which are raising errors, the execution of this validator will be
        abandoned.
        """
        for data_set in data_sets:
            error_handler = ErrorHandler(data_set)
            coroutines = await self._prepare_coroutines(data_set, error_handler)
            task_schedule: dict[_ValidatorMapInternIndexType, asyncio.Task[None]] = {}
            async with asyncio.TaskGroup() as task_group:
                for validator, validator_infos in self.field_validators.items():
                    if validator not in coroutines:
                        # If the coroutine could not be prepared, i.e. if there were problems with the parameters,
                        # it should just be skipped here and raised by the error handler later.
                        continue
                    if validator not in task_schedule:
                        await self._register_task(
                            validator,
                            validator_infos,
                            task_group,
                            task_schedule,
                            coroutines,
                            error_handler,
                        )
            if len(error_handler.excs) > 0:
                raise ExceptionGroup(
                    f"Validation errors for {data_set.__class__.__name__}(id={data_set.get_id()})",
                    list(error_handler.excs.values()),
                )
