import logging
from itertools import cycle
from typing import Iterable, Optional, Type

import pytest  # type:ignore[import]
from bo4e.bo.geschaeftspartner import Geschaeftspartner
from bo4e.bo.marktlokation import Marktlokation
from bo4e.enum.verbrauchsart import Verbrauchsart

from bomf.model import Bo4eDataSet, Bo4eTyp, BusinessObjectRelation
from bomf.validation import Bo4eDataSetRule, Bo4eDataSetValidation, DataSetTyp, DataSetValidationResult


class GeschaeftspartnerMesslokatinDataSet(Bo4eDataSet):
    """
    a dummy data set which contains a Geschaeftspartner and a Messlokation
    """

    def __init__(self, geschaeftspartner: Geschaeftspartner, marktlokation: Marktlokation):
        super().__init__()
        self._gp = geschaeftspartner
        self._malo = marktlokation

    def get_business_object(self, bo_type: Type[Bo4eTyp], specification: Optional[str] = None) -> Bo4eTyp:
        if bo_type == Geschaeftspartner:
            return self._gp
        if bo_type == Marktlokation:
            return self._malo
        raise NotImplementedError("Not relevant for this test")

    def get_relations(self) -> Iterable[BusinessObjectRelation]:
        raise NotImplementedError("Not relevant for this test")

    def get_id(self) -> str:
        return self._malo.marktlokations_id


class DontMigrateCustomersWithVornameKlausAndWaermenutzung(Bo4eDataSetRule):
    """
    This test should show that the validations act on a whole data set that consists of multiple objects.
    The validations are thought to catch inconsitencies between the objects while the (source data) filters should
    be responsible for single objects only.
    """

    def validate(self, dataset: GeschaeftspartnerMesslokatinDataSet) -> DataSetValidationResult:
        customer: Geschaeftspartner = dataset.get_business_object(Geschaeftspartner)
        malo: Marktlokation = dataset.get_business_object(Marktlokation)
        forbidden_verbrauchsarts_for_Klaus = (Verbrauchsart.W, Verbrauchsart.WS, Verbrauchsart.KLW, Verbrauchsart.KLWS)
        if customer.name2 == "Klaus" and malo.verbrauchsart in forbidden_verbrauchsarts_for_Klaus:
            return DataSetValidationResult(is_valid=False, error_message="hier die fehlermeldung")
        return DataSetValidationResult(is_valid=True)

    def __str__(self):
        return "Kein Klaus mit Wärme"


class ARuleThatCrashesInOneOutOfFourTimesForDemoPurposes(Bo4eDataSetRule):
    def __init__(self):
        self._should_crash = cycle([False, False, True, False])

    def validate(self, dataset: DataSetTyp) -> DataSetValidationResult:
        should_crash = next(self._should_crash)
        if should_crash:
            raise Exception("something went terribly wrong")
        return DataSetValidationResult(is_valid=True)

    def __str__(self):
        return "schlecht programmiert"


class MyValidation(Bo4eDataSetValidation):
    """
    all the validation rules I'd like the datasets to obey
    """

    def __init__(self):
        super().__init__(
            rules=[
                DontMigrateCustomersWithVornameKlausAndWaermenutzung(),
                ARuleThatCrashesInOneOutOfFourTimesForDemoPurposes(),
            ]
        )


class TestValidation:
    def test_validation(self, caplog):
        candidates = [
            GeschaeftspartnerMesslokatinDataSet(
                # valid, because verbrauchsart is no Wärme (W)
                marktlokation=Marktlokation.construct(marktlokations_id="53502368955", verbrauchsart=Verbrauchsart.KL),
                geschaeftspartner=Geschaeftspartner.construct(name2="Klaus"),
            ),
            GeschaeftspartnerMesslokatinDataSet(
                # invalid, because verbrauchsart is also Wärme (W)
                marktlokation=Marktlokation.construct(marktlokations_id="87301147632", verbrauchsart=Verbrauchsart.KLW),
                geschaeftspartner=Geschaeftspartner.construct(name2="Klaus"),
            ),
            GeschaeftspartnerMesslokatinDataSet(
                # valid, because vorname is not Klaus but crashes in the second rule
                marktlokation=Marktlokation.construct(marktlokations_id="78192756766", verbrauchsart=Verbrauchsart.W),
                geschaeftspartner=Geschaeftspartner.construct(name2="Günther"),
            ),
            GeschaeftspartnerMesslokatinDataSet(
                # valid, because vorname is not Klaus - no crash
                marktlokation=Marktlokation.construct(marktlokations_id="18410127695", verbrauchsart=Verbrauchsart.W),
                geschaeftspartner=Geschaeftspartner.construct(name2="Klause"),
            ),
        ]
        caplog.set_level(logging.DEBUG, logger=self.__module__)
        my_validation = MyValidation()
        validation_result = my_validation.validate(candidates)
        assert len(validation_result.valid_entries) == 2
        assert len(validation_result.invalid_entries) == 2
        assert "dataset 53502368955 obeys the rule 'Kein Klaus mit Wärme'" in caplog.messages
        assert "✔ data set 53502368955 is valid" in caplog.messages
        assert "dataset 87301147632 does not obey: 'hier die fehlermeldung'" in caplog.messages
        assert "❌ data set 78192756766 is invalid" in caplog.messages
        assert "Validation of rule 'schlecht programmiert' on dataset 78192756766 failed" in caplog.messages
        assert "❌ data set 78192756766 is invalid" in caplog.messages
