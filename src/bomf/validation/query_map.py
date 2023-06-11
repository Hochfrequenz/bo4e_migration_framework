"""
Contains logic to retrieve the data from the data sets in a more complex and more general manner than the
PathMappedValidator. With this it is e.g. possible to iterate through lists and execute the validator for each element.
"""
import itertools
from collections import OrderedDict
from typing import Any, Callable, Generator, Iterable, Iterator, Optional, Self, TypeAlias

from frozendict import frozendict

from bomf.validation.core import MappedValidator, Parameter, Parameters, required_field
from bomf.validation.core.types import DataSetT, ValidatorFunctionT, ValidatorT

IteratorReturnType: TypeAlias = tuple[Any, str]
IteratorReturnTypeWithException: TypeAlias = Exception | IteratorReturnType


class _QueryIterable(Iterable[IteratorReturnTypeWithException]):
    """
    This class is used to wrap a function which returns an iterator (which will always be the same if provided with
    the same inputs) into an Iterable. This is needed to use itertools.product.
    """

    def __init__(
        self,
        data_set: DataSetT,
        iterator_function: Callable[[DataSetT], Iterator[IteratorReturnTypeWithException]],
        include_exceptions: bool,
    ):
        self.data_set = data_set
        self.iterator_generator = iterator_function
        self.include_exceptions = include_exceptions
        self.cur_iter: Optional[Iterator[IteratorReturnTypeWithException]] = None
        self.cur_exceptions: Optional[list[Exception]] = None

    def __iter__(self) -> Iterator[IteratorReturnTypeWithException]:
        self.cur_iter = self.iterator_generator(self.data_set)
        self.cur_exceptions = []
        return self

    def __next__(self) -> IteratorReturnTypeWithException:
        if self.cur_iter is None or self.cur_exceptions is None:
            raise AttributeError("Iterator not initialized")
        next_el = next(self.cur_iter)
        if not self.include_exceptions:
            while isinstance(next_el, Exception):
                self.cur_exceptions.append(next_el)
                next_el = next(self.cur_iter)
        elif isinstance(next_el, Exception):
            self.cur_exceptions.append(next_el)

        return next_el


class Query:
    """
    This class (together with the QueryMappedValidator below) enables some sort of building more or less dynamic queries
    to retrieve data from a data set. The key feature here is that it supports iterating through e.g. lists.
    Example:
        Let's say your data set has a list of Zaehler in attribute `zaehler`. But you want to execute a validator
        for the zaehlernummer for every Zaehler in this list. Then, you can do the following:

        ```
        def list_iterator(list_to_iter: list[Any]) -> tuple[Any, str]:
            return ((el, f"[{index}]") for index, el in enumerate(list_to_iter))

        QueryMappedValidator(
            check_zaehlernummer, {"zaehlernummer": Query().path("zaehler").iter(list_iterator).path("zaehlernummer")}
        )
        ```
    """

    def __init__(self):
        self._function_stack: list[Callable[[DataSetT], Iterator[IteratorReturnTypeWithException]]] = []

    def path(self, attr_path: str) -> Self:
        """
        Adds the provided attribute path to the query
        """
        parent = self._function_stack[-1] if len(self._function_stack) > 0 else None

        def _iter_func(data_set: DataSetT) -> Iterator[IteratorReturnTypeWithException]:
            if parent is not None:
                for parent_el in parent(data_set):
                    if isinstance(parent_el, Exception):
                        yield parent_el
                        continue
                    try:
                        sub_el: Any = required_field(parent_el[0], attr_path, Any)
                        yield sub_el, f"{parent_el[1]}.{attr_path}"
                    except AttributeError as error:
                        query_error = AttributeError(f"{parent_el[1]}.{attr_path} not provided")
                        query_error.__cause__ = error
                        yield query_error
            else:
                try:
                    sub_el = required_field(data_set, attr_path, Any)
                    yield sub_el, attr_path
                except AttributeError as error:
                    query_error = AttributeError(f"{attr_path} not provided")
                    query_error.__cause__ = error
                    yield query_error

        self._function_stack.append(_iter_func)
        return self

    def iter(self, iter_func: Callable[[Any], Iterator[IteratorReturnType]]) -> Self:
        """
        Adds the provided iterator function to the query. When querying the respective object the function will be
        called and provided with this object. The function must return an iterator of tuples of the value and its
        corresponding ID. The ID is arbitrary it is used for a better error output.
        """
        parent = self._function_stack[-1] if len(self._function_stack) > 0 else None

        def _iter_func(data_set: DataSetT) -> Iterator[IteratorReturnTypeWithException]:
            if parent is not None:
                for parent_el in parent(data_set):
                    if isinstance(parent_el, Exception):
                        yield parent_el
                        continue
                    for child_el in iter_func(parent_el[0]):
                        yield child_el[0], parent_el[1] + child_el[1]
            else:
                for child in iter_func(data_set):
                    yield child

        self._function_stack.append(_iter_func)
        return self

    def iterable(self, data_set: DataSetT, include_exceptions: bool = False) -> _QueryIterable:
        """
        Returns an Iterable. When iterating through it, the query will be used to obtain all elements from the data set.
        If an exception is raised during the process, the iterator yields the exception instead of a parameter value.
        This is a little workaround because the iterator would break if an exception is raised.
        If the parameter to which this query corresponds to is optional, these errors will be ignored. Otherwise,
        they will be handled by the error handler.
        """
        return _QueryIterable(data_set, self._function_stack[-1], include_exceptions)


class QueryMappedValidator(MappedValidator[DataSetT, ValidatorFunctionT]):
    """
    This mapped validator class is for more complex use cases. It queries the data set by the given queries. Each
    query corresponds to one parameter of the validator. To learn more about Queries see `Query` above.
    """

    def __init__(self, validator: ValidatorT, param_map: dict[str, Query] | frozendict[str, Query]):
        super().__init__(validator)
        self.param_map: frozendict[str, Query] = frozendict(param_map) if isinstance(param_map, dict) else param_map

    def provide(self, data_set: DataSetT) -> Generator[Parameters[DataSetT] | Exception, None, None]:
        """
        Provides the parameter map to the ValidationManager. For each parameter in the defined parameter mapping the
        query will be evaluated against the data set. Generally, these queries can return more than one value (for
        each parameter). The validator will be executed against all possible combinations of all parameter values.
        If a parameter list could not be filled correctly an error will be yielded instead.
        """
        param_iterables = {
            param_name: query.iterable(data_set, include_exceptions=param_name in self.validator.optional_param_names)
            for param_name, query in self.param_map.items()
        }
        # I want to exclude exceptions from the iterables for required parameters because in those cases this function
        # will yield an exception, and we don't have to explore all the combinations with these exceptions.
        # I.e. if a tuple from the product below contains an exception, this should not be yielded because the
        # parameter is optional. Instead, it will just be filled with the default value and treated as "not provided".
        for parameters in self.param_sets(param_iterables):
            parameter_dict: dict[str, Parameter] = {}
            if isinstance(parameters, Exception):
                yield parameters
                continue
            for param_name, param_value in parameters.items():
                if isinstance(param_value, Exception):
                    assert param_name in self.validator.optional_param_names, (
                        "If the parameter is required but not supplied you should yield an exception "
                        "in `paran_sets` directly. The dictionary of parameters should only contain exceptions if"
                        "they are negligible aka the parameter is optional."
                    )
                    parameter_dict[param_name] = Parameter(
                        mapped_validator=self,
                        name=param_name,
                        param_id="None",
                        value=self.validator.signature.parameters[param_name].default,
                        provided=False,
                    )
                else:
                    parameter_dict[param_name] = Parameter(
                        mapped_validator=self,
                        name=param_name,
                        param_id=param_value[1],
                        value=param_value[0],
                        provided=True,
                    )
            yield Parameters(self, **parameter_dict)

    def param_sets(self, param_iterables: dict[str, _QueryIterable]) -> Iterator[dict[str, Any] | Exception]:
        """
        Gets for each parameter an iterable of all possible values. This method defines how those iterables are
        combined to parameter sets to call the validator with.
        By standard this method returns the cartesian product of all iterables i.e. every possible combination.
        You can override this method to change this behavior.
        """
        ordered_params = OrderedDict(param_iterables)
        for param_tuple in itertools.product(*ordered_params.values()):
            yield dict(zip(ordered_params.keys(), param_tuple))
        for param_name, iterable in param_iterables.items():
            if param_name in self.validator.optional_param_names:
                continue
            assert iterable.cur_exceptions is not None
            for exception in iterable.cur_exceptions:
                yield exception

    def __eq__(self, other):
        return isinstance(other, QueryMappedValidator) and self.param_map == other.param_map

    def __ne__(self, other):
        return not isinstance(other, QueryMappedValidator) or self.param_map != other.param_map

    def __hash__(self):
        return hash(self.param_map)
