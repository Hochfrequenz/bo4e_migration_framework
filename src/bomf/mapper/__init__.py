"""
mappers convert from source data model to BO4E and from BO4E to a target data model
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Awaitable, Generic, Optional, TypeVar

from bomf.model import Bo4eDataSet

TargetDataModel = TypeVar("TargetDataModel")
"""
Target data model is the data model of the target (meaning: the data model of the system to which you'd like to migrate)
"""

IntermediateDataSet = TypeVar("IntermediateDataSet", bound=Bo4eDataSet)
"""
Intermediate data set is the BO4E based layer between source and target.
It is based on BO4E.
"""


class PaginationNotSupportedException(NotImplementedError):
    """
    an exception that indicates, that paginating the data sets is not support at the moment
    """


# pylint:disable=too-few-public-methods
class SourceToBo4eDataSetMapper(ABC, Generic[IntermediateDataSet]):
    """
    A mapper that maps one or multiple sources into Bo4eDataSets
    """

    # the inheriting class is free to combine and bundle the source data as it wants.
    # the only thing it has to provide is a method to create_data_sets (in bo4e).
    # we don't care from where it gets them in the first place

    async def create_data_sets(
        self, offset: Optional[int] = None, limit: Optional[int] = None
    ) -> list[IntermediateDataSet]:
        """
        Apply the mapping to all the provided source data sets.
        If an offset and limit are provided (not None), then the implementing method should
        * either raise a PaginationNotSupportedException
        * or return max limit items starting from offset
        """
        raise NotImplementedError("The inheriting class has to implement this method")


# pylint:disable=too-few-public-methods
class Bo4eDataSetToTargetMapper(ABC, Generic[TargetDataModel, IntermediateDataSet]):
    """
    A mapper that transforms data from the intermediate bo4e model to the target data model
    """

    @abstractmethod
    async def create_target_model(self, dataset: IntermediateDataSet) -> TargetDataModel:
        """
        maps the given source data model into an intermediate data set
        """

    async def create_target_models(self, datasets: list[IntermediateDataSet]) -> list[TargetDataModel]:
        """
        apply the mapping to all the provided dataset
        """
        # here we could use some error handling in the future
        tasks = [self.create_target_model(dataset=dataset) for dataset in datasets]
        return await asyncio.gather(*tasks)


_Source = TypeVar("_Source")
_Target = TypeVar("_Target")


def convert_single_mapping_task_into_list_mapping_task_with_single_pokemon_catchers(
    map_single: Callable[[_Source], Awaitable[_Target]],
    logger: logging.Logger,  # the logger must not be none, because errors must never pass silently.
) -> Callable[[list[_Source]], Awaitable[list[_Target]]]:
    """
    Assume there's an async f(x)->y.
    Now imagine you wanted to use the function but automatically catch all the Exceptions that a single await f(x) may
    throw.
    This function does the overhead for you.
    """

    async def _call(single: _Source) -> Optional[_Target]:
        try:
            return await map_single(single)
        except Exception as error:  # pylint:disable=broad-exception-caught
            # errors should never pass silently unless explicitly silenced. this is no explicit silence.
            logger.error(
                "Error while calling %s on %s: %s", map_single.__name__, str(single), str(error), exc_info=error
            )
            return None

    async def result_func(multiple: list[_Source]) -> list[_Target]:
        tasks: list[Awaitable[Optional[_Target]]] = [_call(x) for x in multiple]
        results = [x for x in await asyncio.gather(*tasks) if x is not None]
        return results

    return result_func


def convert_single_mapping_into_list_mapping_with_single_pokemon_catchers(
    map_single: Callable[[_Source], _Target],
    logger: logging.Logger,  # the logger must not be none, because errors must never pass silently.
) -> Callable[[list[_Source]], list[_Target]]:
    """
    Assume there's an sync f(x)->y.
    Now imagine you wanted to use the function but automatically catch all the Exceptions that a single f(x) may
    throw.
    This function does the overhead for you.
    """

    def _call(single: _Source) -> Optional[_Target]:
        try:
            return map_single(single)
        except Exception as error:  # pylint:disable=broad-exception-caught
            # errors should never pass silently unless explicitly silenced. this is no explicit silence.
            logger.error(
                "Error while calling %s on %s: %s", map_single.__name__, str(single), str(error), exc_info=error
            )
            return None

    def result_func(multiple: list[_Source]) -> list[_Target]:
        results_and_nones: list[Optional[_Target]] = [_call(x) for x in multiple]
        results = [x for x in results_and_nones if x is not None]
        return results

    return result_func
