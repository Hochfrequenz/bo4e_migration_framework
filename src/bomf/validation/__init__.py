"""
bomf performs validation on the intermediate bo4e data set layer.
"""
import logging
from abc import ABC, abstractmethod
from typing import Generic, Iterable, List, Optional, Tuple, TypeVar

import attrs

from bomf.model import Bo4eDataSet

# pylint:disable=too-few-public-methods
DataSetTyp = TypeVar("DataSetTyp", bound=Bo4eDataSet)


@attrs.define(kw_only=True, auto_attribs=True)
class DataSetValidationResult:
    """
    a dataset validation result is the outcome of the evaluation of one atomic validation rule
    """

    is_valid: bool = attrs.field(validator=attrs.validators.instance_of(bool))
    """
    true iff the data set passed the validation
    """
    error_message: Optional[str] = attrs.field(
        validator=attrs.validators.optional(attrs.validators.instance_of(str)), default=None
    )
    """
    The error message should be not None and contain a descriptive error message if is_valid is False.
    """


class Bo4eDataSetRule(ABC, Generic[DataSetTyp]):
    """
    A bo4e dataset rule performs exactly one atomic check on a dataset.
    It is thought to ensure the consistency within a data set (in contrast to the self-consistency of the single objects
    inside the data set which should be part of the filters on the source data models (aka "pre-select")).
    """

    @abstractmethod
    def validate(self, dataset: DataSetTyp) -> DataSetValidationResult:
        """
        checks if the given dataset is valid
        """
        raise NotImplementedError("The inheriting class shall implement this method")

    # inheriting classes should overwrite __str__ for pretty log messages


@attrs.define(kw_only=True, auto_attribs=True)
class _ValidAndInvalidEntities(Generic[DataSetTyp]):
    """
    A container type that holds both the invalid and the valid entries.
    """

    # This class is only used internally and shouldn't be imported elsewhere. That's why its clunky name doesn't matter.

    valid_entries: List[DataSetTyp] = attrs.field(default=attrs.Factory(list))
    """
    those entries that are valid and may pass on to the loader
    """
    invalid_entries: List[Tuple[DataSetTyp, List[str]]] = attrs.field(default=attrs.Factory(list))
    """
    those entries that are invalid together with their respective error messages
    """


@attrs.define(kw_only=True, auto_attribs=True)
class Bo4eDataSetValidation(ABC, Generic[DataSetTyp]):
    """
    A Bo4e Dataset Validation consists of multiple rules that are checked one after another.
    """

    rules: List[Bo4eDataSetRule[DataSetTyp]] = attrs.field()
    """
    the rules which a single data set should obey
    """

    def validate(self, datasets: Iterable[DataSetTyp]) -> _ValidAndInvalidEntities:
        """
        applies all rules to all datasets
        """
        _logger = logging.getLogger(self.__module__)
        result: _ValidAndInvalidEntities[DataSetTyp] = _ValidAndInvalidEntities()
        for dataset in datasets:
            dataset_id = dataset.get_id()
            error_messages: List[str] = []
            for rule in self.rules:
                validation_result: DataSetValidationResult
                try:
                    validation_result = rule.validate(dataset)
                except Exception:  # pylint:disable=broad-except # pokemon catcher is intended
                    error_message = f"Validation of rule '{rule}' on dataset {dataset_id} failed"
                    _logger.exception(error_message, exc_info=True)
                    error_messages.append(error_message)
                    continue
                if validation_result.is_valid:
                    _logger.debug("dataset %s obeys the rule '%s'", dataset_id, str(rule))
                else:
                    _logger.debug("dataset %s does not obey: '%s'", dataset_id, validation_result.error_message)
                    error_messages.append(validation_result.error_message or "<no error message provided>")
            if len(error_messages) == 0:
                result.valid_entries.append(dataset)
                _logger.info("✔ data set %s is valid", dataset_id)
            else:
                result.invalid_entries.append((dataset, error_messages))
                _logger.info("❌ data set %s is invalid", dataset_id)

        return result
