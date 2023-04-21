import asyncio
import inspect
from collections import defaultdict
from dataclasses import Field, dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any, Generic, Iterator, Optional

import networkx as nx

from bomf.validation.core2.errors import ErrorHandler
from bomf.validation.core2.types import (
    AsyncValidatorFunction,
    DataSetT,
    SyncValidatorFunction,
    ValidatorFunctionT,
    ValidatorGeneric,
    ValidatorIndex,
    validation_logger,
)
from bomf.validation.core2.validator import Parameter, ParameterProvider, Parameters, Validator


class _ExecutionState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"


@dataclass
class _ExecutionInfo:
    # param_provider: ParameterProvider[DataSetT]
    depends_on: set[ValidatorIndex]
    timeout: Optional[timedelta]


@dataclass
class _RuntimeTaskInfo:
    current_validator_index: Optional[ValidatorIndex] = None
    current_provided_params: Optional[Parameters[DataSetT]] = None

    @property
    def current_validator(self) -> Optional[ValidatorGeneric]:
        return self.current_validator_index[0] if self.current_validator_index is not None else None

    @property
    def current_param_provider(self) -> Optional[ParameterProvider[DataSetT]]:
        return self.current_validator_index[1] if self.current_validator_index is not None else None


@dataclass
class _RuntimeExecutionInfo:
    data_set: DataSetT
    error_handler: ErrorHandler[DataSetT]
    states: defaultdict[ValidatorIndex, _ExecutionState]
    tasks: defaultdict[ValidatorIndex, Optional[asyncio.Task[None]]]
    running_tasks: defaultdict[asyncio.Task[None] | None, _RuntimeTaskInfo]
    # The None-element represents the main process which is not enveloped in a task

    @property
    def current_task(self) -> Optional[asyncio.Task[None]]:
        return asyncio.current_task()

    @property
    def current_state(self) -> _ExecutionState:
        return self.states[self.running_tasks[self.current_task].current_validator_index]

    @current_state.setter
    def current_state(self, new_state: _ExecutionState):
        self.states[self.running_tasks[self.current_task].current_validator_index] = new_state

    @property
    def current_validator_index(self) -> Optional[ValidatorIndex]:
        return self.running_tasks[self.current_task].current_validator_index

    @current_validator_index.setter
    def current_validator_index(self, new_validator_index: ValidatorIndex):
        self.running_tasks[self.current_task].current_validator_index = new_validator_index

    @property
    def current_provided_params(self) -> Optional[Parameters[DataSetT]]:
        return self.running_tasks[self.current_task].current_provided_params

    @current_provided_params.setter
    def current_provided_params(self, new_provided_params: Parameters[DataSetT]):
        self.running_tasks[self.current_task].current_provided_params = new_provided_params

    @current_validator_index.deleter
    def current_validator_index(self):
        self.running_tasks[self.current_task].current_validator_index = None

    @property
    def current_validator(self) -> Optional[ValidatorGeneric]:
        return (
            self.running_tasks[self.current_task].current_validator_index[0]
            if self.running_tasks[self.current_task].current_validator_index is not None
            else None
        )

    @property
    def current_param_provider(self) -> Optional[ParameterProvider[DataSetT]]:
        return (
            self.running_tasks[self.current_task].current_validator_index[1]
            if self.running_tasks[self.current_task].current_validator_index is not None
            else None
        )


class DependencyGraph(nx.DiGraph):
    pass


class ValidationManager(Generic[DataSetT]):
    def __init__(self):
        self.dependency_graph: DependencyGraph = DependencyGraph()
        self.validator_search_index: dict[ValidatorGeneric, list[ParameterProvider[DataSetT]]] = {}
        self.validators: dict[ValidatorIndex, _ExecutionInfo] = {}
        self._runtime_execution_info: Optional[_RuntimeExecutionInfo] = None

    @property
    def info(self) -> _RuntimeExecutionInfo:
        assert self._runtime_execution_info is not None
        return self._runtime_execution_info

    def _dependency_graph_edges(
        self,
        validator: ValidatorGeneric,
        param_provider: ParameterProvider[DataSetT],
        depends_on: Optional[set["ValidatorGeneric | tuple[ValidatorGeneric, ParameterProvider[DataSetT]]"]],
    ) -> tuple[set[ValidatorIndex], list[tuple[ValidatorIndex, ValidatorIndex]]]:
        if depends_on is None:
            depends_on = set()
        dependency_graph_edges: list[tuple[ValidatorIndex, ValidatorIndex]] = []
        real_dependencies: set[ValidatorIndex] = set()
        for dependency in depends_on:
            if isinstance(dependency, Validator) and dependency not in self.validator_search_index:
                raise ValueError(f"The specified dependency is not registered: {dependency.name}")
            if isinstance(dependency, tuple) and dependency not in self.validators:
                raise ValueError(f"The specified dependency is not registered: {dependency[0].name}")
            if isinstance(dependency, Validator) and len(self.validator_search_index[dependency]) > 1:
                raise ValueError(
                    f"The dependency {dependency.name} got registered multiple times. You have "
                    "to define the parameter provider for the dependency to resolve the ambiguity."
                )
            real_dependency: tuple[ValidatorGeneric, ParameterProvider[DataSetT]]
            if isinstance(dependency, Validator):
                real_dependency = (dependency, self.validator_search_index[dependency][0])
            else:
                real_dependency = dependency
            dependency_graph_edges.append(((validator, param_provider), real_dependency))
            real_dependencies.add(real_dependency)
        return real_dependencies, dependency_graph_edges

    def register(
        self,
        validator: ValidatorGeneric,
        param_provider: ParameterProvider[DataSetT],
        depends_on: Optional[set["ValidatorGeneric | ValidatorIndex"]] = None,
        timeout: Optional[timedelta] = None,
    ):
        real_dependencies, dependency_graph_edges = self._dependency_graph_edges(validator, param_provider, depends_on)
        self.validators[(validator, param_provider)] = _ExecutionInfo(depends_on=real_dependencies, timeout=timeout)
        if validator not in self.validator_search_index:
            self.validator_search_index[validator] = []
        self.validator_search_index[validator].append(param_provider)
        self.dependency_graph.add_node((validator, param_provider))
        self.dependency_graph.add_edges_from(dependency_graph_edges)
        validation_logger.debug("Registered validator: %s", validator.name)

    async def _execute_async_validator(
        self,
        validator_index: ValidatorIndex,
        running_dependencies: set[ValidatorIndex],
    ):
        validator = validator_index[0]
        if len(running_dependencies) > 0:
            await asyncio.wait(
                iter(self.info.tasks[dep] for dep in running_dependencies),
                return_when=asyncio.ALL_COMPLETED,
            )
        dep_exceptions: dict[ValidatorIndex, Exception] = {
            _dep: self.info.error_handler.excs[_dep]
            for _dep in running_dependencies
            if _dep in self.info.error_handler.excs
        }
        if len(dep_exceptions) > 0:
            await self.info.error_handler.catch(
                "Execution abandoned due to failing dependent validators: "
                f"{', '.join(_dep[0].name for _dep in dep_exceptions)}",
                RuntimeError("Errors in depending validators"),
                validator_index,
                self,
                custom_error_id=2,
            )
            return
        for params_or_exc in validator_index[1].provide(self.info.data_set):
            if isinstance(params_or_exc, Exception):
                await self.info.error_handler.catch(
                    str(params_or_exc), params_or_exc, validator_index, self, custom_error_id=1
                )
                continue
            self.info.running_tasks[self.info.tasks[validator_index]].current_provided_params = params_or_exc

            async with self.info.error_handler.pokemon_catcher(validator_index, self):
                if self.validators[validator_index].timeout is not None:
                    async with asyncio.timeout(self.validators[validator_index].timeout.total_seconds()):
                        if validator.is_async:
                            await validator.func(**params_or_exc.param_dict)
                        else:
                            validator.func(**params_or_exc.param_dict)
                else:
                    if validator.is_async:
                        await validator.func(**params_or_exc.param_dict)
                    else:
                        validator.func(**params_or_exc.param_dict)
        self.info.states[validator_index] = _ExecutionState.FINISHED

    async def _execute_sync_validator(self, validator_index: ValidatorIndex):
        execution_info = self.validators[validator_index]
        for params_or_exc in validator_index[1].provide(self.info.data_set):
            if isinstance(params_or_exc, Exception):
                await self.info.error_handler.catch(
                    str(params_or_exc), params_or_exc, validator_index, self, custom_error_id=1
                )
                continue
            self.info.current_provided_params = params_or_exc

            async with self.info.error_handler.pokemon_catcher(validator_index, self):
                if execution_info.timeout is not None:
                    async with asyncio.timeout(execution_info.timeout.total_seconds()):
                        validator_index[0].func(**params_or_exc.param_dict)
                else:
                    validator_index[0].func(**params_or_exc.param_dict)
        self.info.states[validator_index] = _ExecutionState.FINISHED

    async def _execute_validators(
        self,
        execution_order: Iterator[ValidatorIndex],
        task_group: Optional[asyncio.TaskGroup] = None,
    ):
        for validator, param_provider in execution_order:
            validator_index = (validator, param_provider)
            self.info.current_validator_index = validator_index
            self.info.states[validator_index] = _ExecutionState.RUNNING
            dependencies = self.validators[validator_index].depends_on
            running_dependencies = {
                dependency for dependency in dependencies if self.info.states[dependency] == _ExecutionState.RUNNING
            }
            assert all(
                self.info.states[dependency] != _ExecutionState.PENDING for dependency in dependencies
            ), "Somehow the execution order is not working"

            if validator.is_async or len(running_dependencies) > 0:
                assert (
                    task_group is not None
                ), f"Something wrong here. {validator.name} should be run async but there is no task group"
                self.info.tasks[validator_index] = task_group.create_task(
                    self._execute_async_validator(validator_index, running_dependencies)
                )
            else:
                await self._execute_sync_validator(validator_index)

    async def validate(self, *data_sets: DataSetT) -> None:
        for data_set in data_sets:
            self._runtime_execution_info = _RuntimeExecutionInfo(
                data_set=data_set,
                error_handler=ErrorHandler(data_set),
                states=defaultdict(lambda: _ExecutionState.PENDING),
                tasks=defaultdict(lambda: None),
                running_tasks=defaultdict(
                    lambda: _RuntimeTaskInfo(current_validator_index=None, current_provided_params=None)
                ),
            )
            validator_execution_order: Iterator[ValidatorIndex] = reversed(
                list(nx.topological_sort(self.dependency_graph))
            )
            if any(validator[0].is_async for validator in self.validators):
                async with asyncio.TaskGroup() as task_group:
                    await self._execute_validators(validator_execution_order, task_group=task_group)
            else:
                await self._execute_validators(validator_execution_order)

    @classmethod
    def param(cls, param: str) -> Parameter:
        call_stack = inspect.stack()
        # call_stack[0] -> this function
        # call_stack[1] -> must be the validator function
        # call_stack[2] -> should be either `_execute_sync_validator` or `_execute_async_validator`
        try:
            validation_manager: ValidationManager = call_stack[2].frame.f_locals["self"]
        except KeyError:
            raise RuntimeError(
                "You can call this function only directly from inside a function"
                "which is executed by the validation framework"
            )

        provided_params: Parameters = validation_manager.info.current_provided_params
        assert provided_params is not None, "This shouldn't happen"
        if param not in provided_params:
            raise RuntimeError(
                f"Parameter provider {validation_manager.info.current_param_provider} "
                f"did not provide parameter information for parameter '{param}'"
            )
        return provided_params[param] if param in provided_params else None
