"""
BOMF stands for BO4E Migration Framework.
"""
import asyncio
import logging
from abc import ABC
from typing import Generic

import attrs

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


@attrs.define(kw_only=True, auto_attribs=True)
class MigrationStrategy(ABC, Generic[IntermediateDataSet, TargetDataModel]):
    """
    A migration strategy describes the whole migration flow of datasets from a source to a target system
    """

    source_data_to_bo4e_mapper: SourceToBo4eDataSetMapper[IntermediateDataSet]
    """
    A mapper that transforms source data models into data sets that consist of bo4e objects
    """
    validation: ValidationManager[IntermediateDataSet]
    """
    a set of validation rules that are applied to the bo4e data sets
    """
    bo4e_to_target_mapper: Bo4eDataSetToTargetMapper[TargetDataModel, IntermediateDataSet]
    """
    a mapper that transforms bo4e data sets to a structure that suits the target system
    """
    target_loader: EntityLoader[TargetDataModel]
    """
    The target loader moves the target entities into the actual target system.
    """

    async def _map_to_target_validate_and_load(self, bo4e_datasets: list[IntermediateDataSet]) -> list[LoadingSummary]:
        """
        This method encapsulates the steps:
        1. validation
        2. mapping intermediate models to target
        3. load to target system.
        They have been encapsulated because they're used by both the migrate and migrate_paginated methods.
        """
        logger = logging.getLogger(self.__class__.__name__)
        if hasattr(self, "validation") and self.validation is not None:
            logger.info("Applying validation rules to %i bo4e data sets", len(bo4e_datasets))
            validation_result = await self.validation.validate(*bo4e_datasets)
            logger.info(
                "Creating target models from those %i datasets that passed the validation",
                len(validation_result.succeeded_data_sets),
            )
            target_data_models = await self.bo4e_to_target_mapper.create_target_models(
                validation_result.succeeded_data_sets
            )
        else:
            logger.warning("No validation set; skipping validation")
            logger.info("Creating target models from all %i datasets", len(bo4e_datasets))
            target_data_models = await self.bo4e_to_target_mapper.create_target_models(bo4e_datasets)
        logger.info("Loading %i target models into target system", len(target_data_models))
        loading_summaries = await self.target_loader.load_entities(target_data_models)
        success_count, failure_count = _get_success_failure_count(loading_summaries)
        logger.info("Loaded %i entities successfully, %i failed", success_count, failure_count)
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
        logger = logging.getLogger(self.__class__.__name__)
        logger.info("Starting migration %s (w/o pagination)", self.__class__.__name__)
        bo4e_datasets = await self.source_data_to_bo4e_mapper.create_data_sets()
        loading_summaries = await self._map_to_target_validate_and_load(bo4e_datasets)
        return loading_summaries

    async def migrate_paginated(self, chunk_size: int) -> list[LoadingSummary]:
        """
        This is similar to migrate, but it loads the data in chunks of chunk_size.
        Therefore, the source_data_to_bo4e_mapper must support pagination.
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.info("Starting migration %s (with page size %i)", self.__class__.__name__, chunk_size)
        offset = 0
        loading_summaries = []
        while True:
            logger.info("Processing offset=%i, limit=%i", offset, chunk_size)
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
            logger.debug("Received %i datasets (limit was %i)", len(bo4e_datasets), chunk_size)
            if len(bo4e_datasets) == 0:
                logger.info("Received no more datasets; Stopping")
                break
            chunk_loading_summaries = await self._map_to_target_validate_and_load(bo4e_datasets)
            loading_summaries.extend(chunk_loading_summaries)
            await asyncio.sleep(1)  # give the system 1s some time to breathe
            offset += chunk_size
        success_count, failure_count = _get_success_failure_count(loading_summaries)
        logger.info(
            "Finished paginated migration. In total we loaded %i entities out of which %i succeeded and %i failed",
            len(loading_summaries),
            success_count,
            failure_count,
        )
        return loading_summaries
