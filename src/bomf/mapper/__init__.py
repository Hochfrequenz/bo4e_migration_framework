"""
mappers convert from source data model to BO4E and from BO4E to a target data model
"""
from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar

from bomf.model import Bo4eDataSet

SourceDataModel = TypeVar("SourceDataModel")
"""
source data model is the data model of the source (meaning: the data model of the system from which the data originate)
"""

TargetDataModel = TypeVar("TargetDataModel")
"""
target data model is the data model of the target (meaning: the data model of the system to which you'd like to migrate)
"""

IntermediateDataSet = TypeVar("IntermediateDataSet", bound=Bo4eDataSet)
"""
intermediate data set is the BO4E based layer between source and target
"""


# pylint:disable=too-few-public-methods
class SourceToBo4eDataSetMapper(ABC, Generic[SourceDataModel, IntermediateDataSet]):
    """
    A mapper that loads data from a source into a Bo4eDataSet
    """

    @abstractmethod
    def create_data_set(self, source: SourceDataModel) -> IntermediateDataSet:
        """
        maps the given source data model into an intermediate data set
        """

    def create_data_sets(self, sources: List[SourceDataModel]) -> List[IntermediateDataSet]:
        """
        apply the mapping to all the provided dataset
        """
        # here we could use some error handling in the future
        return [self.create_data_set(source=source_data_model) for source_data_model in sources]


# pylint:disable=too-few-public-methods
class Bo4eDataSetToTargetMapper(ABC, Generic[TargetDataModel, IntermediateDataSet]):
    """
    A mapper that transforms data from the intermediate bo4e model to the target data model
    """

    @abstractmethod
    def create_target_model(self, dataset: IntermediateDataSet) -> TargetDataModel:
        """
        maps the given source data model into an intermediate data set
        """

    def create_target_models(self, datasets: List[IntermediateDataSet]) -> List[TargetDataModel]:
        """
        apply the mapping to all the provided dataset
        """
        # here we could use some error handling in the future
        return [self.create_target_model(dataset=dataset) for dataset in datasets]
