"""
mappers convert from source data model to BO4E and from BO4E to a target data model
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar

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


# pylint:disable=too-few-public-methods
class SourceToBo4eDataSetMapper(ABC, Generic[IntermediateDataSet]):
    """
    A mapper that maps one or multiple sources into Bo4eDataSets
    """

    # the inheriting class is free to combine and bundle the source data as it wants.
    # the only thing it has to provide is a method to create_data_sets (in bo4e).
    # we don't care from where it gets them in the first place

    async def create_data_sets(self) -> List[IntermediateDataSet]:
        """
        apply the mapping to all the provided source data sets.

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

    async def create_target_models(self, datasets: List[IntermediateDataSet]) -> List[TargetDataModel]:
        """
        apply the mapping to all the provided dataset
        """
        # here we could use some error handling in the future
        tasks = [self.create_target_model(dataset=dataset) for dataset in datasets]
        return await asyncio.gather(*tasks)
