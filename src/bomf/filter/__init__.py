"""
filters can be used to consider only those objects for a migration that meet certain conditions
"""

# pylint:disable=too-few-public-methods

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Awaitable, Callable, Generic, TypeVar

Candidate = TypeVar("Candidate")  #: an arbitrary but fixed type on which the filter operates


class Filter(ABC, Generic[Candidate]):
    """
    A filter takes objects from candidates and returns only those that are relevant for its use case.
    The advantage over inlining the filtering is that we can easily test these and log in the well-defined filters.
    """

    def __init__(self):
        self._logger = logging.getLogger(self.__module__)

    @abstractmethod
    async def predicate(self, candidate: Candidate) -> bool:
        """
        Returns true iff the candidate shall pass the filter.

        You might wonder why this method is async: The plan is that this allows for building filters that do not only
        depend on the data that you want to filter itself but on other things as well. The most obvious "other thing"
        is the data situation in the target system to which we migrate.
        If we migrate from system A to system B, then one possible filter is to check "does system B know my record"?
        These kind of questions are generally answered in an async way.
        """
        raise NotImplementedError("The inheriting class has to implement this method")

    async def apply(self, candidates: list[Candidate]) -> list[Candidate]:
        """
        apply this filter on the candidates
        """
        tasks: list[Awaitable[bool]] = [self.predicate(c) for c in candidates]
        self._logger.info("%s created %i predicate tasks; Awaiting them all", str(self), len(tasks))
        predicate_results = await asyncio.gather(*tasks)
        self._logger.info("%s awaited %i tasks", str(self), len(tasks))
        result = [
            c for c, predicate_match in zip(candidates, predicate_results, strict=True) if predicate_match is True
        ]
        candidates_removed = sum(1 for pr in predicate_results if pr is False)
        self._logger.info(
            "%i out of %i candidates have been removed by the filter", candidates_removed, len(candidates)
        )
        return result


Aggregate = TypeVar("Aggregate")
"""
Aggregate is a type that enhances the candidate with some additional information used by a Filter on the aggregate.
It has to be possible to extract the candidate from the aggregate.
"""


class AggregateFilter(ABC, Generic[Candidate, Aggregate]):
    """
    A filter that takes objects from candidates and returns only those that are relevant for its use case.
    The difference to the plain Filter is, that the conditions can be defined on groups of candidates instead of
    isolated single candidates. Still it (other than e.g. a usual groupby+filter) logs those entries that do not pass
    the filter.
    """

    def __init__(self, base_filter: Filter[Aggregate]):
        """
        Instantiate by providing a filter that is applied on the aggregate
        """
        self._logger = logging.getLogger(self.__module__)
        self._base_filter = base_filter

    @abstractmethod
    async def aggregate(self, candidates: list[Candidate]) -> list[Aggregate]:
        """
        Create aggregates which are then passed to the base filter that works on the aggregate.
        The method is async so that you can do complex (and e.g. network based) aggregations.
        """
        raise NotImplementedError("The inheriting class has to implement this method")

    @abstractmethod
    def disaggregate(self, aggregate: Aggregate) -> Candidate:
        """
        extract a single candidate from the aggregate that passed the filter
        """
        raise NotImplementedError("The inheriting class has to implement this method")

    async def apply(self, candidates: list[Candidate]) -> list[Candidate]:
        """
        If aggregate and disaggregate and the base_filter are properly setup, then apply will filter the list of
        candidates based on the aggregate base filter.
        """
        aggregates = await self.aggregate(candidates)
        self._logger.info("There are %i candidates and %i aggregates", len(candidates), len(aggregates))
        filtered_aggregates = await self._base_filter.apply(aggregates)
        self._logger.info("There are %i filtered aggregates left", len(filtered_aggregates))
        return [self.disaggregate(fa) for fa in filtered_aggregates]


CandidateProperty = TypeVar("CandidateProperty")


class HardcodedFilter(Filter[Candidate], ABC, Generic[Candidate, CandidateProperty]):
    """
    a harcoded filter filters on a hardcoded list of allowed/blocked values (formerly known as white- and blacklist)
    """

    def __init__(self, criteria_selector: Callable[[Candidate], CandidateProperty], values: set[CandidateProperty]):
        """
        instantiate by providing a criteria selector that returns a property on which we can filter and a set of values.
        Whether the values are used as allowed or not allowed (block) depends on the inheriting class
        """
        super().__init__()
        self._criteria_selector = criteria_selector
        self._values = values


class BlocklistFilter(HardcodedFilter[Candidate, CandidateProperty]):
    """
    remove those candidates whose property is in the provided blocklist
    """

    async def predicate(self, candidate: Candidate) -> bool:
        candidate_property: CandidateProperty = self._criteria_selector(candidate)
        result = candidate_property not in self._values
        if result is False:
            self._logger.debug("'%s' is in the blocklist", candidate_property)
        return result


class AllowlistFilter(HardcodedFilter[Candidate, CandidateProperty]):
    """
    let those candidates pass, whose property is in the provided allowlist
    """

    async def predicate(self, candidate: Candidate) -> bool:
        candidate_property: CandidateProperty = self._criteria_selector(candidate)
        result = candidate_property in self._values
        if result is False:
            self._logger.debug("'%s' is not in the allowlist", candidate_property)
        return result
