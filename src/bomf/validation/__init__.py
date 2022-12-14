"""
bomf performs validation on the intermediate bo4e data set layer.
"""
import logging
from abc import ABC, abstractmethod
from typing import Generic, Iterable, List, Optional, Tuple, TypeVar

import attrs

from bomf.model import Bo4eDataSet

DataSetTyp = TypeVar("DataSetTyp", bound=Bo4eDataSet)


@attrs.define(kw_only=True, auto_attribs=True)
class DataSetValidationResult:
    """
    a dataset validation result is the outcome of one atomic validation
    """

    is_valid: bool = attrs.field(validator=attrs.validators.instance_of(bool))
    """
    true iff the data set passed the validation
    """
    error_message: Optional[str] = attrs.field(validator=attrs.validators.optional(attrs.validators.instance_of(str)))
    """
    The error message should be not None and contain a descriptive error message if is_valid is False.
    """


class Bo4eDataSetRule(ABC, Generic[DataSetTyp]):
    """
    A bo4e dataset rule performs exactly one atomic check on a dataset.
    """

    @abstractmethod
    def validate(self, dataset: DataSetTyp) -> DataSetValidationResult:
        """
        checks if the given dataset is valid
        """
        raise NotImplementedError("The inheriting class shall implement this method")


@attrs.define(kw_only=True, auto_attribs=True)
class ValidAndInvalidEntities(Generic[DataSetTyp]):
    """
    a container type that holds both the invalid and the valid entries
    """

    valid_entities: List[DataSetTyp] = attrs.field(
        validator=attrs.validators.deep_iterable(member_validator=attrs.validators.instance_of(DataSetTyp))
    )
    """
    those entries that are valid and may pass on to the loader
    """
    invalid_entities: List[Tuple[DataSetTyp, str]] = attrs.field(
        validator=attrs.validators.deep_iterable(member_validator=attrs.validators.instance_of(Tuple[DataSetTyp, str]))
    )
    """
    those entries that are invalid together with their respective error messages
    """


@attrs.define(kw_only=True, auto_attribs=True)
class Bo4eDataSetValidation(Generic[DataSetTyp]):
    """
    A Bo4e Dataset Validation consists of multiple rules that are checked one after another.
    """

    rules: List[Bo4eDataSetRule[DataSetTyp]] = attrs.field(
        validator=attrs.validators.deep_iterable(
            member_validator=attrs.validators.instance_of(Bo4eDataSetRule),
            iterable_validator=attrs.validators.min_len(1),
        )
    )
    """
    the rules which a single data set should obey
    """

    def validate(self, datasets: Iterable[DataSetTyp]) -> ValidAndInvalidEntities:
        result = ValidAndInvalidEntities(valid_entities=list(), invalid_entities=list())
        for dataset in datasets:
            for rule in self.rules:
                validation_result = rule.validate(dataset)
                if validation_result.is_valid:
                    result.valid_entities.append(dataset)
                else:
                    result.valid_entities.append((dataset, validation_result.error_message))
        return result
