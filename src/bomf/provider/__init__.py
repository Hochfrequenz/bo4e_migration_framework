"""
providers provide data
"""
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Generic, List, Mapping, Optional, TypeVar, Union

SourceDataModel = TypeVar("SourceDataModel")
"""
Source data model is the data model of the source (meaning: the data model of the system from which the data originate).
"""

KeyTyp = TypeVar("KeyTyp")
"""
The type of the key used as "primary key" for the source data model
"""
# pylint:disable=too-few-public-methods
class SourceDataProvider(ABC, Generic[SourceDataModel, KeyTyp]):
    """
    A source data provider provides entities from the source data system.
    The source data provider is thought to encapsulate data access behind a unified interface.
    """

    @abstractmethod
    def get_data(self) -> List[SourceDataModel]:
        """
        Returns all available entities from the source data model.
        They will be filtered in a SourceDataModel Filter ("Preselect")
        """

    @abstractmethod
    def get_entry(self, key: KeyTyp) -> Optional[SourceDataModel]:
        """
        returns the source data model which has key as key or None if not found
        """


class JsonFileSourceDataProvider(SourceDataProvider[SourceDataModel, KeyTyp], Generic[SourceDataModel, KeyTyp]):
    """
    a source data model provider that is based on a JSON file
    """

    def __init__(
        self,
        json_file_path: Path,
        data_selector: Callable[[Union[dict, list]], List[SourceDataModel]],
        key_selector: Callable[[SourceDataModel], KeyTyp],
        encoding="utf-8",
    ):
        """
        initialize by providing a filepath to the json file and an accessor that describes the position of the data
        within the file.
        """
        with open(json_file_path, "r", encoding=encoding) as json_file:
            raw_data = json.load(json_file)
        self._source_data_models: List[SourceDataModel] = data_selector(raw_data)
        self._key_to_data_model_mapping: Mapping[KeyTyp, SourceDataModel] = {
            key_selector(sdm): sdm for sdm in self._source_data_models
        }

    def get_data(self) -> List[SourceDataModel]:
        return self._source_data_models

    def get_entry(self, key: KeyTyp) -> Optional[SourceDataModel]:
        if key in self._key_to_data_model_mapping:
            return self._key_to_data_model_mapping[key]
        return None
