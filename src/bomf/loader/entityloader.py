"""
entity loaders load entities into the target system
"""

import asyncio
import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Generic, Optional, TypeVar

from generics import get_filled_type
from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError  # pylint:disable=no-name-in-module

_TargetEntity = TypeVar("_TargetEntity")


class EntityLoadingResult(BaseModel):  # pylint:disable=too-few-public-methods
    """
    Information gathered while loading a _TargetEntity into the target system.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id_in_target_system: Optional[str] = None
    """
    the optional ID of the entity in the target system (e.g. if a new (GU)ID is generated upon loading)
    """
    polling_task: Optional[Awaitable] = None
    """
    If this task is awaited it means, that the target system is done with processing the request.
    A possible use case is that the target system responds with something like an event ID which can be used to poll
    an endpoint until it returns the expected result.
    """


class LoadingSummary(BaseModel, ABC, Generic[_TargetEntity]):  # pylint:disable=too-few-public-methods
    """
    Each instance of _TargetEntity that is loaded to the target system results in a LoadingSummary.
    It is a summary that reports to calling code.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    was_loaded_successfully: bool
    """
    true iff the instance has been loaded successfully
    """
    loaded_at: Optional[datetime] = None
    """
    point in time at which the loading (without verification) has completed; if not None
    """
    verified_at: Optional[datetime] = None
    """
    point in time at which the loading of this entity has been verified (or None if not)
    """
    id_in_target_system: Optional[str] = None
    """
    the optional ID of the entity in the target system (e.g. if a new (GU)ID is generated upon loading)
    """
    loading_error: Optional[Exception] = None


class EntityLoader(ABC, Generic[_TargetEntity]):  # pylint:disable=too-few-public-methods
    """
    An instance of an entity loader loads data into a target system.
    There is one loader class per target system entity.
    """

    @abstractmethod
    async def load_entity(self, entity: _TargetEntity) -> Optional[EntityLoadingResult]:
        """
        Load the given entity into the target system.
        This method shall contain the code that accesses the target system.
        If the method raises no exception the loading is interpreted to be successful.
        The method may return an EntityLoadingResult if it can provide the information contained therein or None.
        """

    @abstractmethod
    async def verify(self, entity: _TargetEntity, id_in_target_system: Optional[str] = None) -> bool:
        """
        Verify that the given entity has been successfully loaded into the target system.
        Returns true iff the target system knows the given entity (meaning: the loading was successful).
        """

    # pylint:disable=unused-argument
    def sanitize(self, entity: _TargetEntity) -> None:
        """
        sanitize the given entity, by default (no override) does nothing
        """
        return

    async def close(self) -> None:
        """
        close the session of the loader, by default (no override) does nothing
        """

    async def load(self, entity: _TargetEntity) -> LoadingSummary:
        """
        Loads the given entity into the target system and verifies it has been loaded.
        """

        self.sanitize(entity)
        entity_loading_result: EntityLoadingResult
        loaded_at: datetime
        try:
            entity_loading_result_or_none = await self.load_entity(entity)
            if entity_loading_result_or_none is None:
                # default to a entity loading result that contains no information
                entity_loading_result = EntityLoadingResult(id_in_target_system=None, polling_task=None)
            else:
                entity_loading_result = entity_loading_result_or_none
            loaded_at = datetime.utcnow()
        except Exception as loading_error:  # pylint:disable=broad-except
            # this block is intended to be a Pokemon catcher
            return LoadingSummary(
                id_in_target_system=None,
                was_loaded_successfully=False,
                loading_error=loading_error,
                loaded_at=None,
            )
        if entity_loading_result.polling_task is not None:
            await entity_loading_result.polling_task
        verification_result = await self.verify(
            entity=entity, id_in_target_system=entity_loading_result.id_in_target_system
        )
        return LoadingSummary(
            was_loaded_successfully=verification_result,
            verified_at=datetime.utcnow(),
            loaded_at=loaded_at,
            loading_error=None,
        )

    async def load_entities(self, entities: list[_TargetEntity]) -> list[LoadingSummary]:
        """
        load all the given entities into the target system
        """
        # here we could use some error handling in the future
        tasks: list[Awaitable[LoadingSummary]] = [self.load(entity) for entity in entities]
        result = await asyncio.gather(*tasks)
        return list(result)


class JsonFileEntityLoader(EntityLoader[_TargetEntity], Generic[_TargetEntity]):
    """
    an entity loader that produces a json file as result. This is specifically useful in unit tests
    """

    async def verify(self, entity: _TargetEntity, id_in_target_system: Optional[str] = None) -> bool:
        return True

    def __init__(self, file_path: Path, list_encoder: Callable[[list[_TargetEntity]], list[dict]]):
        """provide a path to a json file (will be created if not exists and overwritten if exists)"""
        self._file_path = file_path
        self._list_encoder = list_encoder

    async def load_entity(self, entity: _TargetEntity) -> Optional[EntityLoadingResult]:
        await self.load_entities([entity])
        return None

    async def load_entities(self, entities: list[_TargetEntity]) -> list[LoadingSummary]:
        new_content = self._list_encoder(entities)
        if self._file_path.exists() and self._file_path.stat().st_size > 0:
            with open(self._file_path, "r+", encoding="utf-8") as json_file:
                old_content = json.load(json_file)
                assert isinstance(old_content, list), "json file must be a list"
                new_content.extend(old_content)
                json_file.seek(0)
                json_file.truncate()
                json.dump(new_content, json_file, ensure_ascii=False, indent=2)
        else:
            with open(self._file_path, "w+", encoding="utf-8") as outfile:
                json.dump(new_content, outfile, ensure_ascii=False, indent=2)

        return [LoadingSummary(was_loaded_successfully=True, loaded_at=datetime.utcnow())] * len(new_content)


_PydanticTargetModel = TypeVar("_PydanticTargetModel", bound=BaseModel)


class PydanticJsonFileEntityLoader(EntityLoader[_PydanticTargetModel], Generic[_PydanticTargetModel]):
    """
    A json file entity loader specifically for pydantic models
    """

    def __init__(self, file_path: Path):
        """provide a file path"""
        self._file_path = file_path
        self._model: type[_PydanticTargetModel] = get_filled_type(self, PydanticJsonFileEntityLoader, 0)
        self._list_type_adapter: TypeAdapter[list[_PydanticTargetModel]] = TypeAdapter(
            list[self._model]  # type:ignore[name-defined]
        )

    async def load_entity(self, entity: _PydanticTargetModel) -> Optional[EntityLoadingResult]:
        await self.load_entities([entity])
        return None

    async def load_entities(self, entities: list[_PydanticTargetModel]) -> list[LoadingSummary]:
        if self._file_path.exists() and self._file_path.stat().st_size > 0:
            with open(self._file_path, "r+b") as json_file:
                try:
                    existing_list = self._list_type_adapter.validate_json(json_file.read())
                except ValidationError as error:
                    raise ValueError(f"json file must be a list of {self._model}") from error
                existing_list.extend(entities)
                json_file.seek(0)
                json_file.truncate()
                json_file.write(self._list_type_adapter.dump_json(existing_list, indent=2, by_alias=True))
        else:
            with open(self._file_path, "w+b") as json_file:
                json_file.write(self._list_type_adapter.dump_json(entities, indent=2, by_alias=True))

        return [LoadingSummary(was_loaded_successfully=True)] * len(entities)

    async def verify(self, entity: _PydanticTargetModel, id_in_target_system: Optional[str] = None) -> bool:
        return True
