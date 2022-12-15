"""
providers provide data
"""
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Generic, Iterable, List, Union

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


class JsonFileSourceDataProvider(SourceDataProvider[SourceDataModel], Generic[SourceDataModel]):
    """
    a source data model provider that is based on a JSON file
    """

    def __init__(
        self,
        json_file_path: Path,
        data_selector: Callable[[Union[dict, list]], List[SourceDataModel]],
        encoding="utf-8",
    ):
        """
        initialize by providing a filepath to the json file and an accessor that describes the position of the data
        within the file.
        """
        with open(json_file_path, "r", encoding=encoding) as json_file:
            raw_data = json.load(json_file)
        self._source_data_models: List[SourceDataModel] = data_selector(raw_data)

    def get_data(self) -> Iterable[SourceDataModel]:
        return self._source_data_models
