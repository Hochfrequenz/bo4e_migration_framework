"""
Tests the overall data flow using bomf.
"""
from typing import Dict, Iterable, List, Optional, Type

import attrs

from bomf import (
    Bo4eDataSetToTargetMapper,
    Bo4eDataSetValidation,
    EntityLoader,
    Filter,
    MigrationStrategy,
    SourceDataModel,
    SourceDataProvider,
    SourceToBo4eDataSetMapper,
)
from bomf.loader.entityloader import EntityLoadingResult, _TargetEntity
from bomf.model import Bo4eDataSet, Bo4eTyp, BusinessObjectRelation
from bomf.validation import Bo4eDataSetRule, DataSetValidationResult

_MySourceDataModel = Dict[str, str]
_MyTargetDataModel = List[str]


@attrs.define(auto_attribs=True, kw_only=True)
class _MyIntermediateDataModel(Bo4eDataSet):
    def get_relations(self) -> Iterable[BusinessObjectRelation]:
        raise NotImplementedError("not relevant for the test")

    def get_business_object(self, bo_type: Type[Bo4eTyp], specification: Optional[str] = None) -> Bo4eTyp:
        raise NotImplementedError("not relevant for the test")

    data: Dict[str, str]

    def get_id(self) -> str:
        return "12345"


class _MySourceDataProvider(SourceDataProvider[_MySourceDataModel]):
    def get_data(self) -> Iterable[_MySourceDataModel]:
        return [
            {"foo": "bar"},
            {"FOO": "BAR"},
            {"Foo": "Bar"},
            {"remove by filter": "should not pass the filter"},
            {"invalid": "doesn't matter"},
        ]


class _MyFilter(Filter[_MySourceDataModel]):
    async def predicate(self, candidate: _MySourceDataModel) -> bool:
        return "remove by filter" not in candidate


class _MyToBo4eMapper(SourceToBo4eDataSetMapper[_MySourceDataModel, _MyIntermediateDataModel]):
    def create_data_set(self, source: _MySourceDataModel) -> _MyIntermediateDataModel:
        return _MyIntermediateDataModel(data=source)


class _MyRule(Bo4eDataSetRule[_MyIntermediateDataModel]):
    def validate(self, dataset: _MyIntermediateDataModel) -> DataSetValidationResult:
        return DataSetValidationResult(is_valid="invalid" not in dataset.data)


class _MyValidation(Bo4eDataSetValidation):
    def __init__(self):
        super().__init__(rules=[_MyRule()])


class _MyToTargetMapper(Bo4eDataSetToTargetMapper[_MyTargetDataModel, _MyIntermediateDataModel]):
    def create_target_model(self, dataset: _MyIntermediateDataModel) -> _MyTargetDataModel:
        my_dict = dataset.data
        for my_key, my_value in my_dict.items():
            return [my_key, my_value]
        return ["doesnt", "matter"]


class _MyTargetLoader(EntityLoader):
    async def load_entity(self, entity: _MyTargetDataModel) -> Optional[EntityLoadingResult]:
        async def polling():
            return True

        return EntityLoadingResult(id_in_target_system="Fooooo", polling_task=polling())

    async def verify(self, entity: _MyTargetDataModel, id_in_target_system: Optional[str] = None) -> bool:
        return True


class MyMigrationStrategy(MigrationStrategy):
    def __init__(self):
        super().__init__(
            source_data_provider=_MySourceDataProvider(),
            preselect_filter=_MyFilter(),
            source_to_bo4e_mapper=_MyToBo4eMapper(),
            validation=_MyValidation(),
            bo4e_to_target_mapper=_MyToTargetMapper(),
            target_loader=_MyTargetLoader(),
        )


class TestMigrationStrategy:
    """
    This is more of an integration than a unit test. All the single components come together here.
    """

    async def test_happy_path(self):
        strategy = MyMigrationStrategy()
        result = await strategy.migrate()
        assert result is not None
        assert len(result) == 3  # = source models -1(filter) -1(validation)
