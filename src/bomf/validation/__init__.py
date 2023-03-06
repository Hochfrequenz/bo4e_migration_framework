"""
bomf performs validation on the intermediate bo4e data set layer. The main class is `ValidatorSet`.
"""
import asyncio
import inspect
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Coroutine, Generic, Optional, Protocol, TypeAlias, TypeVar

from bomf.model import Bo4eDataSet

_logger = logging.getLogger(__name__)

# pylint:disable=too-few-public-methods
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
class _ValidatorInfos:
    """
    This dataclass holds information to a registered validator function.
    You can specify dependent validators which will always be completed before executing the validator.
    If the validator can't complete in time (denoted by timeout in seconds) the execution will be interrupted and an
    error will be raised. If the timeout is not specified (`None`), the validator will not be stopped.
    """

    depends_on: list[ValidatorType]
    timeout: Optional[timedelta]
    # The timeout time in seconds


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

    @property
    def data_set_type(self) -> type[DataSetT]:
        """
        Holds the dataset type.
        """
        if self._data_set_type is None:
            # pylint: disable=no-member
            self._data_set_type = self.__orig_class__.__args__[0]  # type:ignore[attr-defined]
            # If this raises an AttributeError, you probably have forgotten to specify the dataset type as generic
            # argument.
        return self._data_set_type

    def register(
        self,
        validator_func: ValidatorType,
        depends_on: Optional[list[ValidatorType]] = None,
        timeout: Optional[timedelta] = None,
    ) -> None:
        """
        Register a new validator function to call upon running `validate`. It checks if the provided validator function
        is valid in the first place. If the function is invalid a ValueError will be raised.
        The validator function has to by async. The return type must be `None`.
        The validators arguments have to match the respective field in the dataset (name and type). This means of
        course that the function has to be fully type hinted. E.g. if you want to validate the field `x` of type `str`
        in your dataset, the validator function signature has to look as follows:
        ```
        async def my_validator_name(x: str) -> None:
            ...
        ```

        All validator functions will be executed concurrently by default. However, if you want to execute certain
        validators first, you can define this functions in the `depends_on` argument. The validator will then be
        executed when all dependent validators have completed.
        You can also define a timeout (in seconds, can be float or int). The validator will be cancelled if it doesn't
        complete in time. Note that if you define dependent validators the execution time of these will not be counted
        for the timeout on this validator.
        """
        if depends_on is None:
            depends_on = []
        for dependency in depends_on:
            if dependency not in self.field_validators:
                raise ValueError(f"The specified dependency is not registered: {dependency.__name__}")
        if not asyncio.iscoroutinefunction(validator_func):
            raise ValueError("The provided validator function has to be a coroutine (e.g. use async).")

        validator_argspec = inspect.getfullargspec(validator_func)
        validator_annotations = validator_argspec.annotations
        unannotated_args = [arg for arg in validator_argspec.args if arg not in validator_annotations]
        if len(unannotated_args) > 0:
            raise ValueError(
                f"Incorrectly annotated validator function: Arguments {unannotated_args} have no type annotation."
            )
        dataset_annotations = self.data_set_type.__annotations__

        if "return" not in validator_annotations or validator_annotations["return"] is not None:
            raise ValueError("Incorrectly annotated validator function: The return type must be 'None'.")
        if len(validator_annotations) < 2:
            raise ValueError("Incorrectly annotated validator function: The function must take at least one argument.")
        for name, arg_type in validator_annotations.items():
            if name == "return":
                continue
            if name not in self.data_set_type.__fields__:
                raise ValueError(f"Argument '{name}' does not exist as field in the DataSet '{self.data_set_type}'.")
            if arg_type != dataset_annotations[name]:
                raise ValueError(
                    "Incorrectly annotated validator function: "
                    f"The annotated type of argument '{name}' mismatches the type in the DataSet: "
                    f"'{arg_type}' != '{dataset_annotations[name]}'"
                )

        _logger.debug("Registered validator function: %s", validator_func.__name__)
        self.field_validators[validator_func] = _ValidatorInfos(depends_on=depends_on, timeout=timeout)

    def _fill_params(self, validator_func: ValidatorType, data_set: DataSetT) -> Coroutine[Any, Any, None]:
        """
        This function fills the arguments of the validator function with the respective values in the data set
        and returns the coroutine.
        """
        arguments: dict[str, Any] = {}
        for arg_name in validator_func.__annotations__:
            if arg_name != "return":
                arguments[arg_name] = getattr(data_set, arg_name)
        return validator_func(**arguments)

    def _prepare_coroutines(self, data_set: DataSetT) -> dict[ValidatorType, Coroutine[Any, Any, None]]:
        """
        This function fills the arguments of all validator functions with the respective values in the data set
        and returns the coroutines for each validator function as a dictionary.
        """
        return {validator_func: self._fill_params(validator_func, data_set) for validator_func in self.field_validators}

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
            coroutines = self._prepare_coroutines(data_set)
            task_schedule: dict[ValidatorType, asyncio.Task[None]] = {}
            error_handler = ErrorHandler(data_set)
            async with asyncio.TaskGroup() as task_group:
                for validator_func, validator_infos in self.field_validators.items():
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
