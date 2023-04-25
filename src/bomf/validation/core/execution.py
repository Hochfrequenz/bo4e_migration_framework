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
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"


@dataclass
class _ExecutionInfo:
    depends_on: set[MappedValidatorT]
    timeout: Optional[timedelta]


@dataclass
class _RuntimeTaskInfo:
    current_mapped_validator: Optional[MappedValidatorT] = None
    current_provided_params: Optional[Parameters[DataSetT]] = None


@dataclass
class _RuntimeExecutionInfo:
    data_set: DataSetT
    error_handler: ErrorHandler[DataSetT]
    states: defaultdict[MappedValidatorT, _ExecutionState]
    tasks: defaultdict[MappedValidatorT, Optional[asyncio.Task[None]]]
    running_tasks: defaultdict[asyncio.Task[None] | None, _RuntimeTaskInfo]
    # The None-element represents the main process which is not enveloped in a task

    @property
    def current_task(self) -> Optional[asyncio.Task[None]]:
        return asyncio.current_task()

    @property
    def current_state(self) -> _ExecutionState:
        return self.states[self.running_tasks[self.current_task].current_mapped_validator]

    @current_state.setter
    def current_state(self, new_state: _ExecutionState):
        self.states[self.running_tasks[self.current_task].current_mapped_validator] = new_state

    @property
    def current_mapped_validator(self) -> Optional[MappedValidatorT]:
        return self.running_tasks[self.current_task].current_mapped_validator

    @current_mapped_validator.setter
    def current_mapped_validator(self, new_validator_index: MappedValidatorT):
        self.running_tasks[self.current_task].current_mapped_validator = new_validator_index

    @current_mapped_validator.deleter
    def current_mapped_validator(self):
        self.running_tasks[self.current_task].current_mapped_validator = None

    @property
    def current_provided_params(self) -> Optional[Parameters[DataSetT]]:
        return self.running_tasks[self.current_task].current_provided_params

    @current_provided_params.setter
    def current_provided_params(self, new_provided_params: Parameters[DataSetT]):
        self.running_tasks[self.current_task].current_provided_params = new_provided_params


class DependencyGraph(nx.DiGraph):
    pass


class ValidationManager(Generic[DataSetT]):
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
        dependency_graph_edges = self._dependency_graph_edges(mapped_validator, depends_on)
        self.validators[mapped_validator] = _ExecutionInfo(
            depends_on=depends_on if depends_on is not None else set(), timeout=timeout
        )
        self.dependency_graph.add_node(mapped_validator)
        self.dependency_graph.add_edges_from(dependency_graph_edges)
        validation_logger.debug("Registered validator: %s", repr(mapped_validator))

    async def _dependency_errored(self, current_mapped_validator: MappedValidatorT) -> bool:
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
