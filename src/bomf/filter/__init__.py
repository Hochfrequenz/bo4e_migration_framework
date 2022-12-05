"""
filters can be used to consider only those objects for a migration that meet certain conditions
"""

# pylint:disable=too-few-public-methods

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Awaitable, Generic, List, TypeVar

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

    async def apply(self, candidates: List[Candidate]) -> List[Candidate]:
        """
        apply this filter on the candidates
        """
        tasks: List[Awaitable[bool]] = [self.predicate(c) for c in candidates]
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
