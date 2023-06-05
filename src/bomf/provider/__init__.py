"""
providers provide data
"""
import json
import logging
from abc import ABC, abstractmethod
from itertools import groupby
from pathlib import Path
from typing import Callable, Generic, Mapping, TypeVar, Union

from bomf import PaginationNotSupportedException

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
    async def get_data(self) -> list[SourceDataModel]:
        """
        Returns all available entities from the source data model.
        They will be filtered in a SourceDataModel Filter ("Preselect")
        """

    async def get_paginated_data(self, offset: int, limit: int) -> list[SourceDataModel]:
        """
        Returns source data models in the range [offset, offset+limit]
        """
        # This method is not abstract, meaning: the inheriting classes do not have to implement it.
        # It raises an error by default which is ok.
        raise PaginationNotSupportedException(f"The source data provider {self.__class__} does not support pagination")

    @abstractmethod
    async def get_entry(self, key: KeyTyp) -> SourceDataModel:
        """
        returns the source data model which has key as key.
        raises an error if the key is unknown
        """

    async def close(self) -> None:
        """
        close the session of the loader, by default (no override) does nothing
        """


class ListBasedSourceDataProvider(SourceDataProvider[SourceDataModel, KeyTyp]):
    """
    A source data provider that is instantiated with a list of source data models
    """

    def __init__(self, source_data_models: list[SourceDataModel], key_selector: Callable[[SourceDataModel], KeyTyp]):
        """
        instantiate it by providing a list of source data models
        """
        self._models: list[SourceDataModel] = source_data_models
        logger = logging.getLogger(self.__module__)
        for key, key_models in groupby(source_data_models, key=key_selector):
            affected_entries_count = len(list(key_models))
            if affected_entries_count > 1:
                logger.warning(
                    "There are %i>1 entries for the key '%s'. You might miss entries because the key is not unique.",
                    affected_entries_count,
                    str(key),
                )
        self._models_dict: Mapping[KeyTyp, SourceDataModel] = {key_selector(m): m for m in source_data_models}
        logging.getLogger(self.__module__).info(
            "Read %i records from %s", len(self._models_dict), str(source_data_models)
        )
        self.key_selector = key_selector

    async def get_entry(self, key: KeyTyp) -> SourceDataModel:
        return self._models_dict[key]

    async def get_data(self) -> list[SourceDataModel]:
        return self._models

    async def get_paginated_data(self, offset: int, limit: int) -> list[SourceDataModel]:
        if offset > len(self._models):
            return []
        return self._models[offset : offset + limit]


class JsonFileSourceDataProvider(SourceDataProvider[SourceDataModel, KeyTyp], Generic[SourceDataModel, KeyTyp]):
    """
    a source data model provider that is based on a JSON file
    """

    def __init__(
        self,
        json_file_path: Path,
        data_selector: Callable[[Union[dict, list]], list[SourceDataModel]],
        key_selector: Callable[[SourceDataModel], KeyTyp],
        encoding="utf-8",
    ):
        """
        initialize by providing a filepath to the json file and an accessor that describes the position of the data
        within the file.
        """
        with open(json_file_path, "r", encoding=encoding) as json_file:
            raw_data = json.load(json_file)
        self._source_data_models: list[SourceDataModel] = data_selector(raw_data)
        self._key_to_data_model_mapping: Mapping[KeyTyp, SourceDataModel] = {
            key_selector(sdm): sdm for sdm in self._source_data_models
        }
        self.key_selector = key_selector

    async def get_data(self) -> list[SourceDataModel]:
        return self._source_data_models

    async def get_paginated_data(self, offset: int, limit: int) -> list[SourceDataModel]:
        if offset > len(self._source_data_models):
            return []
        return self._source_data_models[offset : offset + limit]

    async def get_entry(self, key: KeyTyp) -> SourceDataModel:
        return self._key_to_data_model_mapping[key]
