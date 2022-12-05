"""
entity loaders load entities into the target system
"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Awaitable, Generic, Optional, TypeVar

import attrs

_TargetEntity = TypeVar("_TargetEntity")


@attrs.define(auto_attribs=True, kw_only=True)
class EntityLoadingResult:  # pylint:disable=too-few-public-methods
    """
    Information gathered while loading a _TargetEntity into the target system.
    """

    id_in_target_system: Optional[str] = attrs.field(
        validator=attrs.validators.optional(attrs.validators.instance_of(str)), default=None
    )
    """
    the optional ID of the entity in the target system (e.g. if a new (GU)ID is generated upon loading)
    """
    polling_task: Optional[Awaitable] = attrs.field(default=None)
    """
    If this task is awaited it means, that the target system is done with processing the request.
    A possible use case is that the target system responds with something like an event ID which can be used to poll
    an endpoint until it returns the expected result.
    """


@attrs.define(auto_attribs=True, kw_only=True)
class LoadingSummary(ABC, Generic[_TargetEntity]):  # pylint:disable=too-few-public-methods
    """
    Each instance of _TargetEntity that is loaded to the target system results in a LoadingSummary.
    It is a summary that reports to calling code.
    """

    was_loaded_successfully: bool = attrs.field(validator=attrs.validators.instance_of(bool))
    """
    true iff the instance has been loaded successfully
    """
    loaded_at: Optional[datetime] = attrs.field(
        validator=attrs.validators.optional(attrs.validators.instance_of(datetime)), default=None
    )
    """
    point in time at which the loading (without verification) has completed; if not None
    """
    verified_at: Optional[datetime] = attrs.field(
        validator=attrs.validators.optional(attrs.validators.instance_of(datetime)), default=None
    )
    """
    point in time at which the loading of this entity has been verified (or None if not)
    """
    id_in_target_system: Optional[str] = attrs.field(
        validator=attrs.validators.optional(attrs.validators.instance_of(str)), default=None
    )
    """
    the optional ID of the entity in the target system (e.g. if a new (GU)ID is generated upon loading)
    """
    loading_error: Optional[Exception] = attrs.field(default=None)


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
