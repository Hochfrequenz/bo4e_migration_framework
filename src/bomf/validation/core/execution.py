"""
Here is the main stuff. The ValidationManager bundles several mapped validators and can validate data sets onto
these.
"""
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Generic, Iterator, Optional

import networkx as nx

from bomf.validation.core.analysis import ValidationResult
from bomf.validation.core.errors import ErrorHandler
from bomf.validation.core.types import DataSetT, MappedValidatorT, ValidatorFunction, validation_logger
from bomf.validation.core.validator import MappedValidator, Parameters


class _ExecutionState(StrEnum):
    """
    A validator can be PENDING, RUNNING or FINISHED. If a validator raised an exception it will have the state
    FINISHED as well.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"


@dataclass
class _ExecutionInfo:
    """
    Contains information about a mapped validator supplied when registering the function. It is constant during a
    validation process.
    """

    depends_on: set[MappedValidatorT]
    timeout: Optional[timedelta]


@dataclass
class _RuntimeTaskInfo:
    """
    This class holds information about a single task. It contains the current mapped validator and its provided
    parameters for this task. This is especially interesting for the synchronous validators which will mainly be
    executed by a single task.
    """

    current_mapped_validator: Optional[MappedValidatorT] = None
    current_provided_params: Optional[Parameters[DataSetT]] = None


@dataclass
class _RuntimeExecutionInfo:
    """
    This class contains all runtime information of a validation process. It will contain all running and finished
    tasks and their corresponding _RuntimeTaskInfos.
    This class corresponds to a single data set instance.
    """

    data_set: DataSetT
    error_handler: ErrorHandler[DataSetT]
    states: defaultdict[MappedValidatorT, _ExecutionState]
    tasks: defaultdict[MappedValidatorT, Optional[asyncio.Task[None]]]
    running_tasks: defaultdict[asyncio.Task[None] | None, _RuntimeTaskInfo]
    # The None-element represents the main process which is not enveloped in a task

    @property
    def current_task(self) -> Optional[asyncio.Task[None]]:
        """The current executing task determined by asyncio.current_task()"""
        return asyncio.current_task()

    @property
    def current_state(self) -> _ExecutionState:
        """The state of the current executing task"""
        return self.states[self.running_tasks[self.current_task].current_mapped_validator]

    @current_state.setter
    def current_state(self, new_state: _ExecutionState):
        """The state of the current executing task"""
        self.states[self.running_tasks[self.current_task].current_mapped_validator] = new_state

    @property
    def current_mapped_validator(self) -> Optional[MappedValidatorT]:
        """The mapped validator of the current executing task"""
        return self.running_tasks[self.current_task].current_mapped_validator

    @current_mapped_validator.setter
    def current_mapped_validator(self, new_validator_index: MappedValidatorT):
        """The mapped validator of the current executing task"""
        self.running_tasks[self.current_task].current_mapped_validator = new_validator_index

    @current_mapped_validator.deleter
    def current_mapped_validator(self):
        """The mapped validator of the current executing task"""
        self.running_tasks[self.current_task].current_mapped_validator = None

    @property
    def current_provided_params(self) -> Optional[Parameters[DataSetT]]:
        """The currently provided params of the current executing task"""
        return self.running_tasks[self.current_task].current_provided_params

    @current_provided_params.setter
    def current_provided_params(self, new_provided_params: Parameters[DataSetT]):
        """The currently provided params of the current executing task"""
        self.running_tasks[self.current_task].current_provided_params = new_provided_params


class DependencyGraph(nx.DiGraph):
    """
    A directed graph representing the dependency network of the validators. By design of the registration function
    it is impossible to have loops in this graph.
    Maybe here happens something more in the future. Plan:
    Replace ValidationManager.validators by node attributes.
    """

    pass


class ValidationManager(Generic[DataSetT]):
    """
    The ValidationManager bundles several mapped validators and can validate data sets onto these. The validators
    can be executed asynchronously by creating tasks or synchronously. The execution order is not defined. However,
    you can define dependencies for a validator. Then, the validator will always be executed after the dependencies
    have finished.
    You can also define a timeout for a validator function after which the execution will be cancelled.
    """

    def __init__(self):
        self.dependency_graph: DependencyGraph = DependencyGraph()
        # self.validator_search_index: dict[ValidatorT, list[MappedValidator[DataSetT]]] = {}
        self.validators: dict[MappedValidatorT, _ExecutionInfo] = {}
        self._runtime_execution_info: Optional[_RuntimeExecutionInfo] = None

    @property
    def info(self) -> _RuntimeExecutionInfo:
        assert self._runtime_execution_info is not None
        return self._runtime_execution_info

    def _dependency_graph_edges(
        self,
        mapped_validator: MappedValidatorT,
        depends_on: Optional[set[MappedValidator[DataSetT, ValidatorFunction]]],
    ) -> list[tuple[MappedValidatorT, MappedValidator[DataSetT, ValidatorFunction]]]:
        """
        Creates a list of edges to add to the dependency network
        """
        if depends_on is None:
            depends_on = set()
        dependency_graph_edges: list[tuple[MappedValidatorT, MappedValidatorT]] = []
        for dependency in depends_on:
            if dependency not in self.validators:
                raise ValueError(f"The specified dependency is not registered: {dependency.name}")

            dependency_graph_edges.append((mapped_validator, dependency))
        return dependency_graph_edges

    def register(
        self,
        mapped_validator: MappedValidatorT,
        depends_on: Optional[set[MappedValidator[DataSetT, ValidatorFunction]]] = None,
        timeout: Optional[timedelta] = None,
    ):
        """
        Register a mapped validator to call upon running `validate`.
        All validator functions will be executed concurrently by default. However, if you want to execute certain
        validators first, you can define this functions in the `depends_on` argument. The validator will then be
        executed when all dependent validators have completed.
        You can also define a timeout. The validator will be cancelled if it doesn't complete in time.
        Note that if you define dependent validators the execution time of these will not be counted
        for the timeout on this validator.
        """
        dependency_graph_edges = self._dependency_graph_edges(mapped_validator, depends_on)
        self.validators[mapped_validator] = _ExecutionInfo(
            depends_on=depends_on if depends_on is not None else set(), timeout=timeout
        )
        self.dependency_graph.add_node(mapped_validator)
        self.dependency_graph.add_edges_from(dependency_graph_edges)
        validation_logger.debug("Registered validator: %s", repr(mapped_validator))

    async def _dependency_errored(self, current_mapped_validator: MappedValidatorT) -> bool:
        """
        Checks if a dependency has completed with errors. If so, this function returns True which will cause the
        current validator to be cancelled.
        """
        dep_exceptions: dict[MappedValidatorT, Exception] = {
            _dep: self.info.error_handler.excs[_dep]
            for _dep in self.validators[current_mapped_validator].depends_on
            if _dep in self.info.error_handler.excs
        }
        if len(dep_exceptions) > 0:
            await self.info.error_handler.catch(
                "Execution abandoned due to failing dependent validators: "
                f"{', '.join(_dep.name for _dep in dep_exceptions)}",
                RuntimeError("Errors in depending validators"),
                current_mapped_validator,
                self,
                custom_error_id=2,
            )
            return True
        return False

    async def _execute_async_validator(
        self,
        mapped_validator: MappedValidatorT,
        running_dependencies: set[MappedValidatorT],
    ):
        """
        This function will be executed by a task and is used to execute validators asynchronously. It will wait for
        dependent validators to finish and will be cancelled if these exited with errors. The validator function
        itself will be executed within a timeout (if defined). Any raising errors will be caught by the error handler.
        The return value will always be ignored.
        """
        if len(running_dependencies) > 0:
            await asyncio.wait(
                [self.info.tasks[dep] for dep in running_dependencies],
                return_when=asyncio.ALL_COMPLETED,
            )
        if await self._dependency_errored(mapped_validator):
            return
        for params_or_exc in mapped_validator.provide(self.info.data_set):
            if isinstance(params_or_exc, Exception):
                await self.info.error_handler.catch(
                    str(params_or_exc), params_or_exc, mapped_validator, self, custom_error_id=1
                )
                continue
            self.info.running_tasks[self.info.tasks[mapped_validator]].current_provided_params = params_or_exc

            async with self.info.error_handler.pokemon_catcher(mapped_validator, self):
                if self.validators[mapped_validator].timeout is not None:
                    async with asyncio.timeout(self.validators[mapped_validator].timeout.total_seconds()):
                        if mapped_validator.is_async:
                            await mapped_validator.validator.func(**params_or_exc.param_dict)
                        else:
                            mapped_validator.validator.func(**params_or_exc.param_dict)
                else:
                    if mapped_validator.is_async:
                        await mapped_validator.validator.func(**params_or_exc.param_dict)
                    else:
                        mapped_validator.validator.func(**params_or_exc.param_dict)
        self.info.states[mapped_validator] = _ExecutionState.FINISHED

    async def _execute_sync_validator(self, mapped_validator: MappedValidatorT):
        """
        This function is used to execute validators synchronously. It reduces a bit of the overhead from
        `_execute_async_validator`.
        The validator function itself will be executed within a timeout (if defined).
        Any raising errors will be caught by the error handler. The return value will always be ignored.
        """
        if await self._dependency_errored(mapped_validator):
            return
        execution_info = self.validators[mapped_validator]
        for params_or_exc in mapped_validator.provide(self.info.data_set):
            if isinstance(params_or_exc, Exception):
                await self.info.error_handler.catch(
                    str(params_or_exc), params_or_exc, mapped_validator, self, custom_error_id=1
                )
                continue
            self.info.current_provided_params = params_or_exc

            async with self.info.error_handler.pokemon_catcher(mapped_validator, self):
                if execution_info.timeout is not None:
                    async with asyncio.timeout(execution_info.timeout.total_seconds()):
                        mapped_validator.validator.func(**params_or_exc.param_dict)
                else:
                    mapped_validator.validator.func(**params_or_exc.param_dict)
        self.info.states[mapped_validator] = _ExecutionState.FINISHED

    async def _execute_validators(
        self,
        execution_order: Iterator[MappedValidatorT],
        task_group: Optional[asyncio.TaskGroup] = None,
    ):
        """
        Executes all registered validators in the defined execution order. The execution order must be built such
        that validators with dependencies occur after their dependencies.
        If the validation manager has not a single asynchronous validator registered, the task group will be None
        and not used.
        A validator will be executed asynchronously iff:
            - The validator is async
            - The validator has dependencies which are not finished yet
        """
        for mapped_validator in execution_order:
            self.info.current_mapped_validator = mapped_validator
            self.info.states[mapped_validator] = _ExecutionState.RUNNING
            dependencies = self.validators[mapped_validator].depends_on
            running_dependencies = {
                dependency for dependency in dependencies if self.info.states[dependency] == _ExecutionState.RUNNING
            }
            assert all(
                self.info.states[dependency] != _ExecutionState.PENDING for dependency in dependencies
            ), "Somehow the execution order is not working"

            if mapped_validator.is_async or len(running_dependencies) > 0:
                assert (
                    task_group is not None
                ), f"Something wrong here. {mapped_validator.name} should be run async but there is no task group"
                self.info.tasks[mapped_validator] = task_group.create_task(
                    self._execute_async_validator(mapped_validator, running_dependencies)
                )
            else:
                self.info.tasks[mapped_validator] = self.info.current_task
                await self._execute_sync_validator(mapped_validator)

    async def validate(self, *data_sets: DataSetT) -> ValidationResult:
        """
        Validates each of the provided data set instances onto the registered validators.
        Any errors occurring during validation will be collected the validation process will not be cancelled.
        The returned `ValidationSummary` object supports several analytical methods - most importantly the property
        `succeeded_data_sets` to retrieve the positively validated data sets (without errors).
        """
        error_handlers: dict[DataSetT, ErrorHandler[DataSetT]] = {}
        for data_set in data_sets:
            self._runtime_execution_info = _RuntimeExecutionInfo(
                data_set=data_set,
                error_handler=ErrorHandler(data_set),
                states=defaultdict(lambda: _ExecutionState.PENDING),
                tasks=defaultdict(lambda: None),
                running_tasks=defaultdict(
                    lambda: _RuntimeTaskInfo(current_mapped_validator=None, current_provided_params=None)
                ),
            )
            error_handlers[data_set] = self.info.error_handler
            validator_execution_order: Iterator[MappedValidatorT] = reversed(
                list(nx.topological_sort(self.dependency_graph))
            )
            if any(mapped_validator.is_async for mapped_validator in self.validators):
                async with asyncio.TaskGroup() as task_group:
                    await self._execute_validators(validator_execution_order, task_group=task_group)
            else:
                await self._execute_validators(validator_execution_order)

        return ValidationResult(self, error_handlers)
