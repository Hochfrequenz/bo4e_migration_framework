import dataclasses
import logging
from itertools import groupby
from typing import List

import pytest  # type:ignore[import]

from bomf.filter import AggregateFilter, Filter


class _FooFilter(Filter):
    async def predicate(self, candidate: dict) -> bool:
        return "foo" in candidate and candidate["foo"] == "bar"


class TestFilter:
    @pytest.mark.parametrize(
        "filter_under_test,candidates,survivors",
        [
            pytest.param(
                _FooFilter(),
                [{"foo": "baz"}, {"foo": "bar"}],
                [{"foo": "bar"}],
            ),
        ],
    )
    async def test_filter(self, filter_under_test: Filter, candidates: List[dict], survivors: List[dict], caplog):
        caplog.set_level(logging.DEBUG, logger=self.__module__)
        actual = await filter_under_test.apply(candidates)
        assert actual == survivors
        assert "1 out of 2 candidates have been removed by the filter" in caplog.messages


@dataclasses.dataclass
class _MyCandidate:
    number: int
    string: str


@dataclasses.dataclass
class _MyAggregate:
    group_key: str
    max_number_for_key: int
    candidate: _MyCandidate


class _BarFilter(AggregateFilter):
    """
    An Aggregate Filter that groups _MyCandidates by their string attribute and keeps only those entries that have the
    highest number (attribute) in their respective group.
    It's basically a show-case test that allows to understand how the aggregate filters are supposed to be used.
    """

    def __init__(self):
        class _BaseFilter(Filter[_MyAggregate]):
            async def predicate(self, candidate: _MyAggregate) -> bool:
                return candidate.max_number_for_key == candidate.candidate.number

        base_filter = _BaseFilter()
        super(_BarFilter, self).__init__(base_filter)

    async def aggregate(self, candidates: List[_MyCandidate]) -> List[_MyAggregate]:
        result: List[_MyAggregate] = []
        for group_key, group in groupby(sorted(candidates, key=lambda c: c.string), lambda c: c.string):
            group_items = list(group)
            max_number_in_group = max(group_item.number for group_item in group_items)
            for group_item in group_items:
                result.append(
                    _MyAggregate(group_key=group_key, max_number_for_key=max_number_in_group, candidate=group_item)
                )
        return result

    def disaggregate(self, aggregate: _MyAggregate) -> _MyCandidate:
        return aggregate.candidate


class TestAggregateFilter:
    @pytest.mark.parametrize(
        "filter_under_test,candidates,survivors",
        [
            pytest.param(
                _BarFilter(),
                [
                    _MyCandidate(number=1, string="foo"),
                    _MyCandidate(number=19, string="bar"),
                    _MyCandidate(number=2, string="foo"),
                    _MyCandidate(number=17, string="bar"),
                ],
                [_MyCandidate(number=19, string="bar"), _MyCandidate(number=2, string="foo")],
            ),
        ],
    )
    async def test_aggregate_filter(
        self, filter_under_test: AggregateFilter, candidates: List[dict], survivors: List[dict], caplog
    ):
        caplog.set_level(logging.DEBUG, logger=self.__module__)
        actual = await filter_under_test.apply(candidates)
        assert actual == survivors
        assert "There are 4 candidates and 4 aggregates" in caplog.messages
        assert "There are 2 filtered aggregates left" in caplog.messages
