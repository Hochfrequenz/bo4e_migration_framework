"""
BOMF stands for BO4E Migration Framework.
"""
import asyncio
import logging
from abc import ABC
from typing import Generic, Optional

import attrs
from injector import inject

from bomf.filter import Filter
from bomf.loader.entityloader import EntityLoader, LoadingSummary
from bomf.mapper import (
    Bo4eDataSetToTargetMapper,
    IntermediateDataSet,
    PaginationNotSupportedException,
    SourceToBo4eDataSetMapper,
    TargetDataModel,
)
from bomf.provider import KeyTyp, SourceDataProvider
from bomf.validation import ValidationManager


# pylint:disable=too-few-public-methods
def _get_success_failure_count(summaries: list[LoadingSummary]) -> tuple[int, int]:
    success_count = sum(1 for x in summaries if x.was_loaded_successfully)
    failure_count = sum(1 for x in summaries if not x.was_loaded_successfully)
    return success_count, failure_count


class MigrationStrategy(ABC, Generic[IntermediateDataSet, TargetDataModel]):
    """
    A migration strategy describes the whole migration flow of datasets from a source to a target system
    """

    @inject
    def __init__(
        self,
        source_data_to_bo4e_mapper: SourceToBo4eDataSetMapper,
        bo4e_to_target_mapper: Bo4eDataSetToTargetMapper,
        target_loader: EntityLoader,
        validation_manager: Optional[ValidationManager] = None,
    ):
        self.source_data_to_bo4e_mapper: SourceToBo4eDataSetMapper[IntermediateDataSet] = source_data_to_bo4e_mapper
        """
        A mapper that transforms source data models into data sets that consist of bo4e objects
        """
        self.validation_manager: Optional[ValidationManager[IntermediateDataSet]] = validation_manager
        """
        a set of validation rules that are applied to the bo4e data sets
        """
        self.bo4e_to_target_mapper: Bo4eDataSetToTargetMapper[
            TargetDataModel, IntermediateDataSet
        ] = bo4e_to_target_mapper
        """
        a mapper that transforms bo4e data sets to a structure that suits the target system
        """
        self.target_loader: EntityLoader[TargetDataModel] = target_loader
        """
        The target loader moves the target entities into the actual target system.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        """
        Class logger
        """

    async def _map_to_target_validate_and_load(self, bo4e_datasets: list[IntermediateDataSet]) -> list[LoadingSummary]:
        """
        This method encapsulates the steps:
        1. validation
        2. mapping intermediate models to target
        3. load to target system.
        They have been encapsulated because they're used by both the migrate and migrate_paginated methods.
        """
        if self.validation_manager is not None:
            self.logger.info("Applying validation rules to %i bo4e data sets", len(bo4e_datasets))
            validation_result = await self.validation_manager.validate(*bo4e_datasets)
            self.logger.info(
                "Creating target models from those %i datasets that passed the validation",
                len(validation_result.succeeded_data_sets),
            )
            target_data_models = await self.bo4e_to_target_mapper.create_target_models(
                validation_result.succeeded_data_sets
            )
        else:
            self.logger.warning("No validation set; skipping validation")
            self.logger.info("Creating target models from all %i datasets", len(bo4e_datasets))
            target_data_models = await self.bo4e_to_target_mapper.create_target_models(bo4e_datasets)
        self.logger.info("Loading %i target models into target system", len(target_data_models))
        loading_summaries = await self.target_loader.load_entities(target_data_models)
        await self.target_loader.close()
        success_count, failure_count = _get_success_failure_count(loading_summaries)
        self.logger.info("Loaded %i entities successfully, %i failed", success_count, failure_count)
        return loading_summaries

    async def migrate(self) -> list[LoadingSummary]:
        """
        run the entire migration flow from source to target which includes:
        1. create bo4e data source using the source_data_set_to_bo4e_mapper
        2. checking that all the bo4e data sets obey the validation rules
        3. mapping from bo4e to the target data model
        4. loading the target data models into the target system.
        """
        # todo: here we should add some logging and statistics stuff
        self.logger.info("Starting migration %s (w/o pagination)", self.__class__.__name__)
        bo4e_datasets = await self.source_data_to_bo4e_mapper.create_data_sets()
        loading_summaries = await self._map_to_target_validate_and_load(bo4e_datasets)
        return loading_summaries

    async def migrate_paginated(
        self, chunk_size: int, initial_offset: int = 0, upper_bound: Optional[int] = None
    ) -> list[LoadingSummary]:
        """
        This is similar to migrate, but it loads the data in chunks of chunk_size.
        Therefore, the source_data_to_bo4e_mapper must support pagination.
        You can specify an offset on where to start (initial_offset).
        The upper_bound does not cap the number of entries you want to migrate but instead is a number up to which the
        offset will definitely be incremented. This allows you to paginate over empty pages (because there's no matching
        entry within the range covered by one page). If you don't specify an upper_bound, the migration will stop on the
        first empty page.
        """
        self.logger.info(
            "Starting migration %s (with page size %i and initial offset %i)",
            self.__class__.__name__,
            chunk_size,
            initial_offset,
        )
        offset = initial_offset
        loading_summaries = []
        while True:
            self.logger.info("Processing offset=%i, limit=%i", offset, chunk_size)
            try:
                bo4e_datasets = await self.source_data_to_bo4e_mapper.create_data_sets(
                    limit=chunk_size, offset=offset
                )  # this might raise an PaginationNotSupportedException
            except TypeError as type_error:
                error_message = str(type_error)
                if (
                    "got an unexpected keyword argument 'limit'" in error_message
                    or "got an unexpected keyword argument 'offset'" in error_message
                ):
                    # this case should be prevented by the type checker already
                    raise PaginationNotSupportedException() from type_error
                raise
            self.logger.debug("Received %i datasets (limit was %i)", len(bo4e_datasets), chunk_size)
            if (upper_bound is None or offset > upper_bound) and len(bo4e_datasets) == 0:
                if upper_bound is not None:
                    self.logger.info("Reached first empty page after upper bound %i; Stopping", upper_bound)
                else:
                    self.logger.info("Received no more datasets (first empty page; no upper bound defined); Stopping")
                break
            chunk_loading_summaries = await self._map_to_target_validate_and_load(bo4e_datasets)
            loading_summaries.extend(chunk_loading_summaries)
            await asyncio.sleep(1)  # give the system 1s some time to breathe
            offset += chunk_size
        success_count, failure_count = _get_success_failure_count(loading_summaries)
        self.logger.info(
            "Finished paginated migration. In total we loaded %i entities out of which %i succeeded and %i failed",
            len(loading_summaries),
            success_count,
            failure_count,
        )
        return loading_summaries
