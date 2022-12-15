from typing import Iterable

import pytest  # type:ignore[import]

from bomf.provider import SourceDataProvider


class LegacyDataSystemDataProvider(SourceDataProvider):
    """
    a dummy for access to a legacy system from which we want to migrate data
    """

    def get_data(self) -> Iterable[str]:
        return ["foo", "bar", "baz"]


class TestSourceDataProvider:
    def test_provider(self):
        # this is a pretty dumb test
        provider_under_test = LegacyDataSystemDataProvider()
        assert isinstance(provider_under_test.get_data(), list)
