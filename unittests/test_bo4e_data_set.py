from typing import Iterable, Optional, Type

import pytest  # type:ignore[import]
from bo4e.bo.marktlokation import Marktlokation
from bo4e.bo.messlokation import Messlokation

from bomf.model import Bo4eDataSet, BusinessObjectRelation, SpecificBusinessObject


class _ExampleDataSet:
    def __init__(self):
        self.malo = Marktlokation.construct(marktlokations_id="54321012345")
        self.melo = Messlokation.construct(messlokations_id="DE000001111122222333334444455555666667")

    def get_relations(self) -> Iterable[BusinessObjectRelation]:
        return [BusinessObjectRelation(relation_type="malomelo", relation_part_a=self.malo, relation_part_b=self.melo)]

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


class TestBo4eDataSet:
    async def test_example_data_set(self):
        dataset: Bo4eDataSet = _ExampleDataSet()
        assert len(list(dataset.get_relations())) == 1
        assert isinstance(dataset.get_business_object(Marktlokation), Marktlokation)
        assert isinstance(dataset.get_business_object(Messlokation), Messlokation)
