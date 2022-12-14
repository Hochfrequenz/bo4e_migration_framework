"""
BOMF stands for BO4E Migration Framework.
"""
from abc import ABC
from typing import Generic

import attrs

from bomf.filter import Filter
from bomf.loader.entityloader import EntityLoader
from bomf.mapper import (
    Bo4eDataSetToTargetMapper,
    IntermediateDataSet,
    SourceDataModel,
    SourceToBo4eDataSetMapper,
    TargetDataModel,
)


@attrs.define(kw_only=True, auto_attribs=True)
class MigrationStrategy(ABC, Generic[SourceDataModel, IntermediateDataSet, TargetDataModel]):
    """
    A migration strategy describes the whole migration flow of datasets from a source to a target system
    """

    preselect_filter: Filter[SourceDataModel]
    """
    A 'Preselect' is a filter on the source model which only lets those entities pass, that match its predicate
    """
    source_to_bo4e_mapper: SourceToBo4eDataSetMapper[SourceDataModel, IntermediateDataSet]
    """
    A mapper that transforms source data models into data sets that consist of bo4e objects
    """
    bo4e_to_target_mapper: Bo4eDataSetToTargetMapper[TargetDataModel, IntermediateDataSet]
    """
    A mapper that transforms bo4e data sets to a structure that suits the target system
    """
    target_loader: EntityLoader[TargetDataModel]
    """
    The target loader moves the target entities into the actual target system.
    """
