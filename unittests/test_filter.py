import dataclasses
import logging
from itertools import groupby

import pytest  # type:ignore[import]

from bomf.filter import AggregateFilter, AllowlistFilter, BlocklistFilter, Filter
from bomf.filter.sourcedataproviderfilter import SourceDataProviderFilter
from bomf.provider import ListBasedSourceDataProvider, SourceDataProvider


class _FooFilter(Filter[dict]):
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
    async def test_filter(self, filter_under_test: Filter, candidates: list[dict], survivors: list[dict], caplog):
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

    async def aggregate(self, candidates: list[_MyCandidate]) -> list[_MyAggregate]:
        result: list[_MyAggregate] = []
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
        self, filter_under_test: AggregateFilter, candidates: list[dict], survivors: list[dict], caplog
    ):
        caplog.set_level(logging.DEBUG, logger=self.__module__)
        actual = await filter_under_test.apply(candidates)
        assert actual == survivors
        assert "There are 4 candidates and 4 aggregates" in caplog.messages
        assert "There are 2 filtered aggregates left" in caplog.messages


class TestBlockAndAllowlistFilter:
    async def test_allowlist_filter(self):
        allowlist = {"A", "B", "C"}
        candidates: list[dict[str, str]] = [{"foo": "A"}, {"foo": "B"}, {"foo": "Z"}]
        allowlist_filter: AllowlistFilter[dict[str, str], str] = AllowlistFilter(lambda c: c["foo"], allowlist)
        actual = await allowlist_filter.apply(candidates)
        assert actual == [{"foo": "A"}, {"foo": "B"}]

    async def test_blocklist_filter(self):
        blocklist = {"A", "B", "C"}
        candidates: list[dict[str, str]] = [{"foo": "A"}, {"foo": "B"}, {"foo": "Z"}]
        blocklist_filter: BlocklistFilter[dict[str, str], str] = BlocklistFilter(lambda c: c["foo"], blocklist)
        actual = await blocklist_filter.apply(candidates)
        assert actual == [{"foo": "Z"}]


class TestSourceDataProviderFilter:
    @pytest.mark.parametrize(
        "candidate_filter,candidates,survivors",
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
    async def test_source_data_provider_filter(
        self,
        candidate_filter: Filter[_MyCandidate],
        candidates: list[_MyCandidate],
        survivors: list[_MyCandidate],
        caplog,
    ):
        my_provider: ListBasedSourceDataProvider[_MyCandidate, int] = ListBasedSourceDataProvider(
            candidates, key_selector=lambda mc: mc.number
        )
        sdp_filter: SourceDataProviderFilter[_MyCandidate, int] = SourceDataProviderFilter(candidate_filter)
        caplog.set_level(logging.DEBUG, logger=self.__module__)
        filtered_provider = await sdp_filter.apply(my_provider)
        assert isinstance(filtered_provider, SourceDataProvider)
        actual = await filtered_provider.get_data()
        assert actual == survivors
        assert "There are 4 candidates and 4 aggregates" in caplog.messages
        assert "There are 2 filtered aggregates left" in caplog.messages

    async def test_source_data_provider_filter_error(self):
        my_provider: ListBasedSourceDataProvider[dict, str] = ListBasedSourceDataProvider(
            [{"foo": "bar"}, {"foo": "notbar"}], key_selector=lambda d: d["foo"]
        )
        del my_provider.key_selector
        sdp_filter: SourceDataProviderFilter[dict, str] = SourceDataProviderFilter(_FooFilter())
        with pytest.raises(AttributeError):
            await sdp_filter.apply(my_provider)
