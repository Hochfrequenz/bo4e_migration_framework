"""
providers provide data
"""
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Generic, List, Mapping, Optional, Protocol, TypeVar, Union

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
    def get_entry(self, key: KeyTyp) -> SourceDataModel:
        """
        returns the source data model which has key as key.
        raises an error if the key is unknown
        """


class ListBasedSourceDataProvider(SourceDataProvider[SourceDataModel, KeyTyp]):
    """
    A source data provider that is instantiated with a list of source data models
    """

    def __init__(self, source_data_models: List[SourceDataModel], key_selector: Callable[[SourceDataModel], KeyTyp]):
        """
        instantiate it by providing a list of source data models
        """
        self._models: List[SourceDataModel] = source_data_models
        self._models_dict: Mapping[KeyTyp, SourceDataModel] = {key_selector(m): m for m in source_data_models}
        logging.getLogger(self.__module__).info(
            "Read %i records from %s", len(self._models_dict), str(source_data_models)
        )
        self.key_selector = key_selector

    def get_entry(self, key: KeyTyp) -> SourceDataModel:
        return self._models_dict[key]

    def get_data(self) -> List[SourceDataModel]:
        return self._models


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
        self.key_selector = key_selector

    def get_data(self) -> List[SourceDataModel]:
        return self._source_data_models

    def get_entry(self, key: KeyTyp) -> SourceDataModel:
        return self._key_to_data_model_mapping[key]
