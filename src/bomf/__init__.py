"""
BOMF stands for BO4E Migration Framework.
"""
from abc import ABC
from typing import Generic, List

import attrs

from bomf.filter import Filter
from bomf.loader.entityloader import EntityLoader, LoadingSummary
from bomf.mapper import (
    Bo4eDataSetToTargetMapper,
    IntermediateDataSet,
    SourceDataModel,
    SourceToBo4eDataSetMapper,
    TargetDataModel,
)
from bomf.provider import KeyTyp, SourceDataProvider
from bomf.validation import Bo4eDataSetValidation


# pylint:disable=too-few-public-methods
@attrs.define(kw_only=True, auto_attribs=True)
class MigrationStrategy(ABC, Generic[SourceDataModel, KeyTyp, IntermediateDataSet, TargetDataModel]):
    """
    A migration strategy describes the whole migration flow of datasets from a source to a target system
    """

    source_data_provider: SourceDataProvider[SourceDataModel, KeyTyp]
    """
    a source from where data shall be migrated
    """
    preselect_filter: Filter[SourceDataModel]
    """
    A 'Preselect' is a filter on the source model which only lets those entities pass, that match its predicate.
    """
    source_to_bo4e_mapper: SourceToBo4eDataSetMapper[SourceDataModel, IntermediateDataSet]
    """
    A mapper that transforms source data models into data sets that consist of bo4e objects
    """
    validation: Bo4eDataSetValidation[IntermediateDataSet]
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

    async def migrate(self) -> List[LoadingSummary]:
        """
        run the entire migration flow from source to target which includes:
        1. retrieving data from the source_data_provider
        2. applying the preselect_filter
        3. running the source_to_bo4e_mapper
        4. checking that all the bo4e data sets obey the validation rules
        5. mapping from bo4e to the target data model
        6. loading the target data models into the target system.
        """
        # todo: here we should add some logging and statistics stuff
        source_data_models = self.source_data_provider.get_data()
        if not isinstance(source_data_models, list):
            source_data_models = list(source_data_models)
        filter_survivors = await self.preselect_filter.apply(source_data_models)
        bo4e_datasets = self.source_to_bo4e_mapper.create_data_sets(filter_survivors)
        valid_entries = self.validation.validate(bo4e_datasets).valid_entries
        target_data_models = self.bo4e_to_target_mapper.create_target_models(valid_entries)
        loading_summaries = await self.target_loader.load_entities(target_data_models)
        return loading_summaries
