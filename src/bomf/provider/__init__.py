"""
providers provide data
"""
from abc import ABC, abstractmethod
from typing import Generic, Iterable

from bomf.mapper import SourceDataModel


# pylint:disable=too-few-public-methods
class SourceDataProvider(ABC, Generic[SourceDataModel]):
    """
    A source data provider provides entities from the source data system.
    The source data provider is thought to encapsulate data access behind a unified interface.
    """

    @abstractmethod
    def get_data(self) -> Iterable[SourceDataModel]:
        """
        Returns all available entities from the source data model.
        They will be filtered in a SourceDataModel Filter ("Preselect")
        """
