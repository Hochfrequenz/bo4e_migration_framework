import enum
from typing import Iterable, Optional, Type

import pytest  # type:ignore[import]
from bo4e.bo.geschaeftspartner import Geschaeftspartner
from bo4e.com.adresse import Adresse

from bomf.model import Bo4eDataSet, Bo4eTyp, BusinessObjectRelation


class _GeschaeftspartnerAdresseRelation(enum.Enum):
    HAS_LIEFERANSCHRIFT = 1
    HAS_RECHNUNGSANSCHRIFT = 2
    HAS_GEBURTSORT = 3


class _ExampleDataSet(Bo4eDataSet):
    business_partner: Geschaeftspartner = Geschaeftspartner.construct(name1="Müller", name2="Klaus")
    address: Adresse = Adresse.construct(strasse="Rechnungsstrasse", hausnummer="5")

    def get_relations(self) -> Iterable[BusinessObjectRelation]:
        return [
            BusinessObjectRelation(
                relation_type=_GeschaeftspartnerAdresseRelation.HAS_LIEFERANSCHRIFT,
                relation_part_a=self.business_partner,
                relation_part_b=self.address,
            )
        ]

    def get_business_object(self, bo_type: Type[Bo4eTyp], specification: Optional[str] = None) -> Bo4eTyp:
        # pyling:disable=fixme
        # todo: find out how to allow the static type checker to not complain about the "dynamic" type
        if bo_type == Geschaeftspartner:
            return self.business_partner  # type:ignore[return-value]
        if bo_type == Adresse:
            return self.address  # type:ignore[return-value]
        raise NotImplementedError(f"The bo type {bo_type} is not implemented")


class TestBo4eDataSet:
    async def test_example_data_set(self):
        dataset: _ExampleDataSet = _ExampleDataSet()
        assert len(list(dataset.get_relations())) == 1
        assert isinstance(dataset.get_business_object(Geschaeftspartner), Geschaeftspartner)
        assert isinstance(dataset.get_business_object(Adresse), Adresse)
        assert dataset.get_id() is not None
