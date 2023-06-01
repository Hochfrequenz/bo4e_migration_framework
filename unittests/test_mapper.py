from typing import Optional, Type

import attrs
import pytest  # type:ignore[import]
from bo4e.bo.marktlokation import Marktlokation
from bo4e.bo.messlokation import Messlokation

from bomf.mapper import Bo4eDataSetToTargetMapper, PaginationNotSupportedException, SourceToBo4eDataSetMapper
from bomf.model import Bo4eTyp


class _NotImplementedBo4eDataSetMixin:
    """
    a mixin to inherit from if you'd like to have correct types but don't care about the logic
    """


@attrs.define(kw_only=True, auto_attribs=True)
class _MaLoAndMeLo(_NotImplementedBo4eDataSetMixin):
    malo: Marktlokation = attrs.field()
    melo: Messlokation = attrs.field()

    def get_business_object(self, bo_type: Type[Bo4eTyp], specification: Optional[str] = None) -> Bo4eTyp:
        # pyling:disable=fixme
        # todo: find out how to allow the static type checker to not complain about the "dynamic" type
        if bo_type == Marktlokation:
            return self.malo  # type:ignore[return-value]
        if bo_type == Messlokation:
            return self.melo  # type:ignore[return-value]
        raise NotImplementedError(f"The bo type {bo_type} is not implemented")


# in these tests we assume, that:
# - the source data model is a dictionary
# - the intermediate data model are BO4E MaLo and MeLo
# - the target data model is a list of string
# This is just to demonstrate the mapping structures.


class _DictToMaLoMeLoMapper(SourceToBo4eDataSetMapper):
    async def create_data_sets(self, offset: Optional[int] = None, limit: Optional[int] = None) -> list[_MaLoAndMeLo]:
        if limit is not None or offset is not None:
            raise PaginationNotSupportedException()
        return [
            _MaLoAndMeLo(
                melo=Messlokation.construct(messlokations_id=source["meloId"]),
                malo=Marktlokation.construct(marktlokations_id=source["maloId"]),
            )
            for source in [{"maloId": "54321012345", "meloId": "DE000111222333"}]
        ]


class _MaLoMeLoToListMapper(Bo4eDataSetToTargetMapper):
    async def create_target_model(self, dataset: _MaLoAndMeLo) -> list[str]:
        return [
            dataset.get_business_object(Marktlokation).marktlokations_id,
            dataset.get_business_object(Messlokation).messlokations_id,
        ]


class TestMapper:
    async def test_source_to_intermediate_mapper_batch(self):
        mapper = _DictToMaLoMeLoMapper()
        actual = await mapper.create_data_sets()
        assert actual == [
            _MaLoAndMeLo(
                melo=Messlokation.construct(messlokations_id="DE000111222333"),
                malo=Marktlokation.construct(marktlokations_id="54321012345"),
            )
        ]

    async def test_intermediate_to_target_mapper(self):
        """
        tests the single data set mapping
        """
        mapper = _MaLoMeLoToListMapper()
        actual = await mapper.create_target_model(
            _MaLoAndMeLo(
                melo=Messlokation.construct(messlokations_id="DE000111222333"),
                malo=Marktlokation.construct(marktlokations_id="54321012345"),
            )
        )
        assert actual == ["54321012345", "DE000111222333"]

    async def test_intermediate_to_target_mapper_batch(self):
        """
        test the batch mapping
        """
        mapper = _MaLoMeLoToListMapper()
        actual = await mapper.create_target_models(
            [
                _MaLoAndMeLo(
                    melo=Messlokation.construct(messlokations_id="DE000111222333"),
                    malo=Marktlokation.construct(marktlokations_id="54321012345"),
                )
            ]
        )
        assert actual == [["54321012345", "DE000111222333"]]
