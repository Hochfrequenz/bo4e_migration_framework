"""
bomf performs validation on the intermediate bo4e data set layer.
"""
import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Coroutine, Generic, Optional, Protocol, TypeVar

from bomf.model import Bo4eDataSet

_logger = logging.getLogger(__name__)

# pylint:disable=too-few-public-methods
DataSetType = TypeVar("DataSetType", bound=Bo4eDataSet)


class ValidatorType(Protocol):
    __name__: str

    def __call__(self, **kwargs: Any) -> Coroutine[Any, Any, None]:
        ...


# ValidatorType: TypeAlias = Callable[..., Coroutine[Any, Any, None]]
# ValidatorType: TypeAlias = FunctionType


@dataclass
class _ValidatorInfos:
    depends_on: list[ValidatorType]
    timeout: Optional[int]


class ValidatorSet(Generic[DataSetType]):
    def __init__(self, *args, **kwargs):
        self.field_validators: dict[ValidatorType, _ValidatorInfos] = {}
        self._data_set_type: Optional[type[DataSetType]] = None

    @property
    def data_set_type(self) -> type[DataSetType]:
        if self._data_set_type is None:
            self._data_set_type = self.__orig_class__.__args__[0]  # type:ignore[attr-defined]
        return self._data_set_type

    def register(
        self,
        validator_func: ValidatorType,
        depends_on: Optional[list[ValidatorType]] = None,
        timeout: Optional[int] = None,
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
        executed when all dependant validators have finished.
        You can also define a timeout (in seconds, can be float or int). The validator will be cancelled if it doesn't
        finish in time. Note that if you define dependant validators the execution time of these will not be counted
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
                    f"'{arg_type}' != '{self.data_set_type.__annotations__[name]}'"
                )

        _logger.debug(f"Registered validator function: {validator_func.__name__}")
        self.field_validators[validator_func] = _ValidatorInfos(depends_on=depends_on, timeout=timeout)

    def _fill_params(self, validator_func: ValidatorType, data_set: DataSetType) -> Coroutine[Any, Any, None]:
        """
        This function fills the arguments of the validator function with the respective values in the data set
        and returns the coroutine.
        """
        arguments: dict[str, Any] = {}
        for arg_name in validator_func.__annotations__:
            if arg_name != "return":
                arguments[arg_name] = getattr(data_set, arg_name)
        return validator_func(**arguments)

    def _prepare_coroutines(self, data_set: DataSetType) -> dict[ValidatorType, Coroutine[Any, Any, None]]:
        """
        This function fills the arguments of all validator functions with the respective values in the data set
        and returns the coroutines for each validator function as a dictionary.
        """
        return {validator_func: self._fill_params(validator_func, data_set) for validator_func in self.field_validators}

    def _register_task(
        self,
        validator_func: ValidatorType,
        validator_infos: _ValidatorInfos,
        task_group: asyncio.TaskGroup,
        task_schedule: dict[ValidatorType, asyncio.Task[None]],
        coroutines: dict[ValidatorType, Coroutine[Any, Any, None]],
    ) -> None:
        """
        This recursive method registers a new task for the given validator function in the task group.
        It has to be recursive to ensure that if the validator function depends on other validators that these have
        already registered tasks - or register them if necessary.
        """
        dep_tasks: list[asyncio.Task[None]] = []
        for dep in validator_infos.depends_on:
            if dep not in task_schedule:
                self._register_task(dep, self.field_validators[dep], task_group, task_schedule, coroutines)
            dep_tasks.append(task_schedule[dep])

        assert len(dep_tasks) == len(validator_infos.depends_on)

        async def _wrapper() -> Any:
            if len(dep_tasks) > 0:
                await asyncio.wait(dep_tasks, return_when=asyncio.ALL_COMPLETED)
            try:
                return await asyncio.wait_for(coroutines[validator_func], validator_infos.timeout)
            except TimeoutError as error:
                _logger.exception(
                    f"Timeout ({validator_infos.timeout}s) during execution of "
                    f"validator '{validator_func.__name__}'",
                    exc_info=error,
                )
                raise error

        task_schedule[validator_func] = task_group.create_task(_wrapper())

    async def validate_async(self, *data_sets: DataSetType) -> None:
        """
        Apparently, this function has to be async if we want to use async statements inside it. But I don't want
        the validate function to be async, so I used this little workaround.
        """
        for data_set in data_sets:
            coroutines = self._prepare_coroutines(data_set)
            task_schedule: dict[ValidatorType, asyncio.Task[None]] = {}
            async with asyncio.TaskGroup() as task_group:
                for validator_func, validator_infos in self.field_validators.items():
                    if validator_func not in task_schedule:
                        self._register_task(validator_func, validator_infos, task_group, task_schedule, coroutines)

    def validate(self, *data_sets: DataSetType) -> None:
        """
        Validates the provided data set instances onto the registered validator functions. If any error occures
        """
        asyncio.run(self.validate_async(*data_sets))


# @attrs.define(kw_only=True, auto_attribs=True)
# class _ValidAndInvalidEntities(Generic[DataSetTyp]):
#     """
#     A container type that holds both the invalid and the valid entries.
#     """
#
#     # This class is only used internally and shouldn't be imported elsewhere. That's why its clunky name doesn't matter.
#
#     valid_entries: List[DataSetTyp] = attrs.field(default=attrs.Factory(list))
#     """
#     those entries that are valid and may pass on to the loader
#     """
#     invalid_entries: List[Tuple[DataSetTyp, List[str]]] = attrs.field(default=attrs.Factory(list))
#     """
#     those entries that are invalid together with their respective error messages
#     """
#
#
# @attrs.define(kw_only=True, auto_attribs=True)
# class Bo4eDataSetValidation(ABC, Generic[DataSetTyp]):
#     """
#     A Bo4e Dataset Validation consists of multiple rules that are checked one after another.
#     """
#
#     rules: List[Bo4eDataSetRule[DataSetTyp]] = attrs.field()
#     """
#     the rules which a single data set should obey
#     """
#
#     def validate(self, datasets: Iterable[DataSetTyp]) -> _ValidAndInvalidEntities:
#         """
#         applies all rules to all datasets
#         """
#         _logger = logging.getLogger(self.__module__)
#         result: _ValidAndInvalidEntities[DataSetTyp] = _ValidAndInvalidEntities()
#         for dataset in datasets:
#             dataset_id = dataset.get_id()
#             error_messages: List[str] = []
#             for rule in self.rules:
#                 validation_result: DataSetValidationResult
#                 try:
#                     validation_result = rule.validate(dataset)
#                 except Exception:  # pylint:disable=broad-except # pokemon catcher is intended
#                     error_message = f"Validation of rule '{rule}' on dataset {dataset_id} failed"
#                     _logger.exception(error_message, exc_info=True)
#                     error_messages.append(error_message)
#                     continue
#                 if validation_result.is_valid:
#                     _logger.debug("dataset %s obeys the rule '%s'", dataset_id, str(rule))
#                 else:
#                     _logger.debug("dataset %s does not obey: '%s'", dataset_id, validation_result.error_message)
#                     error_messages.append(validation_result.error_message or "<no error message provided>")
#             if len(error_messages) == 0:
#                 result.valid_entries.append(dataset)
#                 _logger.info("✔ data set %s is valid", dataset_id)
#             else:
#                 result.invalid_entries.append((dataset, error_messages))
#                 _logger.info("❌ data set %s is invalid", dataset_id)
#
#         return result
