"""
Contains core functionality to validate arbitrary Bo4eDataSets
"""
import asyncio
import inspect
import logging
import types
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Coroutine, Generic, Optional, TypeAlias, TypeVar, Union

from typeguard import check_type

from bomf.model import Bo4eDataSet

_logger = logging.getLogger(__name__)
DataSetT = TypeVar("DataSetT", bound=Bo4eDataSet)
ValidatorType: TypeAlias = Callable[..., Coroutine[Any, Any, None]]


class ValidationError(RuntimeError):
    """
    A unified schema for error messages occurring during validation.
    """

    def __init__(self, message_detail: str, data_set: Bo4eDataSet, cause: Exception):
        message = f"Validation error for {data_set.__class__.__name__}(id={data_set.get_id()}): {message_detail}"
        super().__init__(message)
        self.__cause__ = cause


# pylint: disable=too-few-public-methods
class ErrorHandler:
    """
    This class provides functionality to easily log any occurring error.
    It can save one exception for each validator function.
    """

    def __init__(self, data_set: Bo4eDataSet):
        self.data_set = data_set
        self.excs: dict[ValidatorType, Exception] = {}

    def catch(self, msg: str, error: Exception, validator_func: ValidatorType):
        """
        Logs a new validation error with the defined message. The `error` parameter will be set as `__cause__` of the
        validation error.
        """
        error_nested = ValidationError(
            msg,
            self.data_set,
            error,
        )
        _logger.exception(
            str(error_nested),
            exc_info=error_nested,
        )
        self.excs[validator_func] = error_nested


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


@dataclass
class _ValidatorInfos:
    """
    This dataclass holds information to a registered validator function.
    You can specify dependent validators which will always be completed before executing the validator.
    If the validator can't complete in time (denoted by timeout in seconds) the execution will be interrupted and an
    error will be raised. If the timeout is not specified (`None`), the validator will not be stopped.
    """

    depends_on: list[ValidatorType]
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
        self.field_validators: dict[ValidatorType, _ValidatorInfos] = {}
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

    # pylint: disable=too-many-branches
    def register(
        self,
        validator_func: ValidatorType,
        parameter_map: dict[str, str],
        depends_on: Optional[list[ValidatorType]] = None,
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
        for dependency in depends_on:
            if dependency not in self.field_validators:
                raise ValueError(f"The specified dependency is not registered: {dependency.__name__}")
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
            validator_param_infos[param_name] = ValidatorParamInfos(
                attribute_path=attribute_path.split("."),
                required=validator_signature.parameters[param_name].default
                == validator_signature.parameters[param_name].empty,
                param_type=param_annotation,
            )
        validator_special_params: dict[str, Any] = {}
        for param_name, param in validator_signature.parameters.items():
            if param.annotation == validator_signature.empty:
                raise ValueError(f"The parameter {param_name} has no annotated type.")
            if param_name in self.special_params:
                validator_special_params[param_name] = param.annotation

        _logger.debug("Registered validator function: %s", validator_func.__name__)
        self.field_validators[validator_func] = _ValidatorInfos(
            depends_on=depends_on,
            timeout=timeout,
            param_infos=validator_param_infos,
            special_params=validator_special_params,
        )

    def _fill_params(self, validator_func: ValidatorType, data_set: DataSetT) -> Coroutine[Any, Any, None]:
        """
        This function fills the arguments of the validator function with the respective values in the data set
        and returns the coroutine.
        """
        arguments: dict[str, Any] = {}
        for param_name, param_infos in self.field_validators[validator_func].param_infos.items():
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
        for special_param_name, special_param_type in self.field_validators[validator_func].special_params.items():
            match special_param_name:
                case "_param_infos":
                    arguments[special_param_name] = self.field_validators[validator_func].param_infos
                    check_type(
                        special_param_name,
                        self.field_validators[validator_func].param_infos,
                        self.special_params[special_param_name],
                    )
                case _:
                    raise NotImplementedError(f"No implementation for special parameter {special_param_name}")
            check_type(special_param_name, arguments[special_param_name], special_param_type)
        return validator_func(**arguments)

    def _prepare_coroutines(
        self, data_set: DataSetT, error_handler: ErrorHandler
    ) -> dict[ValidatorType, Coroutine[Any, Any, None]]:
        """
        This function fills the arguments of all validator functions with the respective values in the data set
        and returns the coroutines for each validator function as a dictionary.
        """
        coroutines: dict[ValidatorType, Coroutine[Any, Any, None]] = {}
        for validator_func in self.field_validators:
            try:
                coroutines[validator_func] = self._fill_params(validator_func, data_set)
            except AttributeError as error:
                error_handler.catch(
                    f"Couldn't fill in parameter for validator function {validator_func.__name__}: {error}",
                    error,
                    validator_func,
                )
        return coroutines

    # pylint: disable=too-many-arguments
    def _register_task(
        self,
        validator_func: ValidatorType,
        validator_infos: _ValidatorInfos,
        task_group: asyncio.TaskGroup,
        task_schedule: dict[ValidatorType, asyncio.Task[None]],
        coroutines: dict[ValidatorType, Coroutine[Any, Any, None]],
        error_handler: ErrorHandler,
    ) -> None:
        """
        This recursive method registers a new task for the given validator function in the task group.
        It has to be recursive to ensure that if the validator function depends on other validators that these have
        already registered tasks - or register them if necessary.
        """
        dep_tasks: dict[ValidatorType, asyncio.Task[None]] = {}
        for dep in validator_infos.depends_on:
            if dep not in task_schedule:
                self._register_task(
                    dep, self.field_validators[dep], task_group, task_schedule, coroutines, error_handler
                )
            dep_tasks[dep] = task_schedule[dep]

        assert len(dep_tasks) == len(validator_infos.depends_on)

        async def _wrapper() -> Any:
            if len(dep_tasks) > 0:
                await asyncio.wait(dep_tasks.values(), return_when=asyncio.ALL_COMPLETED)
            dep_exceptions: dict[ValidatorType, Exception] = {
                dep: error_handler.excs[dep] for dep in dep_tasks if dep in error_handler.excs
            }
            if len(dep_exceptions) > 0:
                error_handler.catch(
                    "Execution abandoned due to uncaught exceptions in dependent validators: "
                    f"{', '.join(dep.__name__ for dep in dep_exceptions)}",
                    RuntimeError(f"Uncaught exceptions in dependent validators: {list(dep_exceptions.keys())}"),
                    validator_func,
                )
                return
            try:
                return await asyncio.wait_for(
                    coroutines[validator_func],
                    validator_infos.timeout.total_seconds() if validator_infos.timeout is not None else None,
                )
            except TimeoutError as error:
                assert validator_infos.timeout is not None  # This shouldn't happen, but type checker cries
                error_handler.catch(
                    f"Timeout ({validator_infos.timeout.total_seconds()}s) during execution of "
                    f"validator '{validator_func.__name__}'",
                    error,
                    validator_func,
                )
            except Exception as error_in_validator:  # pylint: disable=broad-exception-caught
                error_handler.catch(
                    f"Uncaught exception raised in validator {validator_func.__name__}: {error_in_validator}",
                    error_in_validator,
                    validator_func,
                )

        task_schedule[validator_func] = task_group.create_task(_wrapper())

    async def validate(self, *data_sets: DataSetT) -> None:
        """
        Validates each of the provided data set instances onto the registered validator functions.
        Any errors occurring during validation will be collected and raised together as ExceptionGroup.
        If a validator depends on other validators which are raising errors, the execution of this validator will be
        abandoned.
        """
        for data_set in data_sets:
            error_handler = ErrorHandler(data_set)
            coroutines = self._prepare_coroutines(data_set, error_handler)
            task_schedule: dict[ValidatorType, asyncio.Task[None]] = {}
            async with asyncio.TaskGroup() as task_group:
                for validator_func, validator_infos in self.field_validators.items():
                    if validator_func not in coroutines:
                        # If the coroutine could not be prepared, i.e. if there were problems with the parameters,
                        # it should just be skipped here and raised by the error handler later.
                        continue
                    if validator_func not in task_schedule:
                        self._register_task(
                            validator_func,
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
