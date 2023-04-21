from bomf.validation.core2.errors import ErrorHandler, ValidationError
from bomf.validation.core2.types import DataSetT


class ValidationResult:
    def __init__(self, data_sets: list[DataSetT], error_handlers: list[ErrorHandler[DataSetT]]):
        self.succeeded_data_sets: list[DataSetT] = []
        "List of data sets which got validated without any errors"
        self.data_set_errors: dict[DataSetT, list[ValidationError]] = {}
        "Maps data sets in which errors got raised to a list of ValidationErrors"
        for data_set, error_handler in error_handlers.items():
            if len(error_handler.excs) > 0:
                self.data_set_errors[data_set] = list(error_handler.excs.values())
            else:
                self.succeeded_data_sets.append(data_set)

        self.total = len(error_handlers)
        "Number of all validated data sets"
        self.num_succeeds = len(self.succeeded_data_sets)
        "Number of positively validated data sets (equivalent to `len(self.succeeded_data_sets)`)"
        self.num_fails = len(self.data_set_errors)
        "Number of negatively validated data sets (equivalent to `len(self.data_set_errors)`)"
