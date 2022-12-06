from typing import Dict, Iterable, List, Optional, Type

import attrs
import pytest  # type:ignore[import]
from bo4e.bo.marktlokation import Marktlokation
from bo4e.bo.messlokation import Messlokation

from bomf.mapper import Bo4eDataSetToTargetMapper, IntermediateDataSet, SourceDataModel, SourceToBo4eDataSetMapper
from bomf.model import Bo4eDataSet, BusinessObjectRelation, SpecificBusinessObject


class _NotImplementedBo4eDataSetMixin:
    """
    a mixin to inherit from if you'd like to have correct types but don't care about the logic
    """

    def get_relations(self) -> Iterable[BusinessObjectRelation]:
        raise NotImplementedError("Not relevant for this test")

    def get_business_object(
        self, bo_type: Type[SpecificBusinessObject], specification: Optional[str] = None
    ) -> SpecificBusinessObject:
        raise NotImplementedError("Not relevant for this test")


@attrs.define(kw_only=True, auto_attribs=True)
class _ExampleDataSet(_NotImplementedBo4eDataSetMixin):
    malo: Marktlokation = attrs.field()
    melo: Messlokation = attrs.field()

    def get_business_object(
        self, bo_type: Type[SpecificBusinessObject], specification: Optional[str] = None
    ) -> SpecificBusinessObject:
        # pyling:disable=fixme
        # todo: find out how to allow the static type checker to not complain about the "dynamic" type
        if bo_type == Marktlokation:
            return self.malo  # type:ignore[return-value]
        if bo_type == Messlokation:
            return self.melo  # type:ignore[return-value]
        raise NotImplementedError(f"The bo type {bo_type} is not implemented")


class _DictToExampleDataSetMapper(SourceToBo4eDataSetMapper):
    def create_data_set(self, source: Dict[str, str]) -> _ExampleDataSet:
        return _ExampleDataSet(
            melo=Messlokation.construct(messlokations_id=source["meloId"]),
            malo=Marktlokation.construct(marktlokations_id=source["maloId"]),
        )


class _ExampleDataSetToListMapper(Bo4eDataSetToTargetMapper):
    def create_target_model(self, dataset: _ExampleDataSet) -> List[str]:
        return [
            dataset.get_business_object(Marktlokation).marktlokations_id,
            dataset.get_business_object(Messlokation).messlokations_id,
        ]


class TestMapper:
    def test_source_to_intermediate_mapper(self):
        mapper = _DictToExampleDataSetMapper()
        actual = mapper.create_data_set({"maloId": "54321012345", "meloId": "DE000111222333"})
        assert actual == _ExampleDataSet(
            melo=Messlokation.construct(messlokations_id="DE000111222333"),
            malo=Marktlokation.construct(marktlokations_id="54321012345"),
        )

    def test_intermediate_to_target_mapper(self):
        mapper = _ExampleDataSetToListMapper()
        actual = mapper.create_target_model(
            _ExampleDataSet(
                melo=Messlokation.construct(messlokations_id="DE000111222333"),
                malo=Marktlokation.construct(marktlokations_id="54321012345"),
            )
        )
        assert actual == ["54321012345", "DE000111222333"]
