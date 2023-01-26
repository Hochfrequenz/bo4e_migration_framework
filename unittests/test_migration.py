"""
Tests the overall data flow using bomf.
"""
from typing import Dict, List, Optional

import attrs

from bomf import (
    Bo4eDataSetToTargetMapper,
    Bo4eDataSetValidation,
    EntityLoader,
    Filter,
    MigrationStrategy,
    SourceDataProvider,
    SourceToBo4eDataSetMapper,
)
from bomf.loader.entityloader import EntityLoadingResult
from bomf.model import Bo4eDataSet
from bomf.provider import KeyTyp, SourceDataModel
from bomf.validation import Bo4eDataSetRule, DataSetValidationResult

_MySourceDataModel = Dict[str, str]
_MyKeyTyp = str
_MyTargetDataModel = List[str]


@attrs.define(auto_attribs=True, kw_only=True)
class _MyIntermediateDataModel(Bo4eDataSet):
    data: Dict[str, str]

    def get_id(self) -> str:
        return "12345"


class _MySourceDataProvider(SourceDataProvider[_MySourceDataModel, _MyKeyTyp]):
    def get_entry(self, key: KeyTyp) -> _MySourceDataModel:
        raise NotImplementedError("Not relevant for the test")

    def get_data(self) -> List[_MySourceDataModel]:
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


class _MyToBo4eMapper(SourceToBo4eDataSetMapper[_MyIntermediateDataModel]):
    def __init__(self, what_ever_you_like: List[_MySourceDataModel]):
        # what_ever_you_like is a place holde for all the relation magic that may happen
        self._source_models = what_ever_you_like

    def create_data_sets(self) -> List[_MyIntermediateDataModel]:
        return [_MyIntermediateDataModel(data=source) for source in self._source_models]


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
    pass


class TestMigrationStrategy:
    """
    This is more of an integration than a unit test. All the single components come together here.
    """

    async def test_happy_path(self):
        # here's some pre-processing, you can read some data, you can create relations, whatever
        raw_data = _MySourceDataProvider().get_data()
        survivors = await _MyFilter().apply(raw_data)
        to_bo4e_mapper = _MyToBo4eMapper(what_ever_you_like=survivors)
        strategy = MyMigrationStrategy(
            source_data_set_to_bo4e_mapper=to_bo4e_mapper,
            validation=_MyValidation(),
            bo4e_to_target_mapper=_MyToTargetMapper(),
            target_loader=_MyTargetLoader(),
        )
        result = await strategy.migrate()
        assert result is not None
        assert len(result) == 3  # = source models -1(filter) -1(validation)
