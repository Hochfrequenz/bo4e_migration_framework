"""
Tests the overall data flow using bomf.
"""
from typing import Optional

from bomf import (
    Bo4eDataSetToTargetMapper,
    EntityLoader,
    Filter,
    MigrationStrategy,
    SourceDataProvider,
    SourceToBo4eDataSetMapper,
    ValidationManager,
)
from bomf.loader.entityloader import EntityLoadingResult
from bomf.model import Bo4eDataSet
from bomf.provider import KeyTyp
from bomf.validation import Validator
from bomf.validation.core import SyncValidatorFunction
from bomf.validation.path_map import PathMappedValidator

_MySourceDataModel = dict[str, str]
_MyKeyTyp = str
_MyTargetDataModel = list[str]


class _MyIntermediateDataModel(Bo4eDataSet):
    data: dict[str, str]

    def get_id(self) -> str:
        return "12345"


class _MySourceDataProvider(SourceDataProvider[_MySourceDataModel, _MyKeyTyp]):
    async def get_entry(self, key: KeyTyp) -> _MySourceDataModel:
        raise NotImplementedError("Not relevant for the test")

    async def get_data(self) -> list[_MySourceDataModel]:
        return [
            {"foo": "bar"},
            {"FOO": "BAR"},
            {"Foo": "Bar"},
            {"remove by filter": "should not pass the filter"},
            # {"invalid": "doesn't matter"},
        ]


class _MyFilter(Filter[_MySourceDataModel]):
    async def predicate(self, candidate: _MySourceDataModel) -> bool:
        return "remove by filter" not in candidate


class _MyToBo4eMapper(SourceToBo4eDataSetMapper[_MyIntermediateDataModel]):
    def __init__(self, what_ever_you_like: list[_MySourceDataModel]):
        # what_ever_you_like is a place holde for all the relation magic that may happen
        self._source_models = what_ever_you_like

    async def create_data_sets(
        self, offset: Optional[int] = None, limit: Optional[int] = None
    ) -> list[_MyIntermediateDataModel]:
        if offset is not None and limit is not None:
            return [_MyIntermediateDataModel(data=source) for source in self._source_models[offset : offset + limit]]
        return [_MyIntermediateDataModel(data=source) for source in self._source_models]


def _my_rule(data: dict[str, str]):
    if "invalid" in data:
        raise ValueError("'invalid' in data")


_my_mapped_validator: PathMappedValidator[_MyIntermediateDataModel, SyncValidatorFunction] = PathMappedValidator(
    Validator(_my_rule), {"data": "data"}
)
_my_validation = ValidationManager[_MyIntermediateDataModel]()
_my_validation.register(_my_mapped_validator)


class _MyToTargetMapper(Bo4eDataSetToTargetMapper[_MyTargetDataModel, _MyIntermediateDataModel]):
    async def create_target_model(self, dataset: _MyIntermediateDataModel) -> _MyTargetDataModel:
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


class MyMigrationStrategy(MigrationStrategy[_MyIntermediateDataModel, _MyTargetDataModel]):
    pass


class TestMigrationStrategy:
    """
    This is more of an integration than a unit test. All the single components come together here.
    """

    async def test_happy_path(self):
        # here's some pre-processing, you can read some data, you can create relations, whatever
        raw_data = await _MySourceDataProvider().get_data()
        survivors = await _MyFilter().apply(raw_data)
        to_bo4e_mapper = _MyToBo4eMapper(what_ever_you_like=survivors)
        strategy = MyMigrationStrategy(
            source_data_to_bo4e_mapper=to_bo4e_mapper,
            validation=_my_validation,
            bo4e_to_target_mapper=_MyToTargetMapper(),
            target_loader=_MyTargetLoader(),
        )
        result = await strategy.migrate()
        assert result is not None
        assert len(result) == 3  # = source models -1(filter) -1(validation)

    async def test_happy_path_paginated(self):
        # here's some pre-processing, you can read some data, you can create relations, whatever
        raw_data = await _MySourceDataProvider().get_data()
        survivors = await _MyFilter().apply(raw_data)
        to_bo4e_mapper = _MyToBo4eMapper(what_ever_you_like=survivors)
        strategy = MyMigrationStrategy(
            source_data_to_bo4e_mapper=to_bo4e_mapper,
            validation=_my_validation,
            bo4e_to_target_mapper=_MyToTargetMapper(),
            target_loader=_MyTargetLoader(),
        )
        result = await strategy.migrate_paginated(1)  # the chunk_size arg here is the only difference to the other test
        assert result is not None
        assert len(result) == 3  # = source models -1(filter) -1(validation)
