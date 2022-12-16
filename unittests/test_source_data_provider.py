from pathlib import Path
from typing import Iterable

import pytest  # type:ignore[import]

from bomf.provider import JsonFileSourceDataProvider, SourceDataProvider


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

    @pytest.mark.datafiles("./unittests/example_source_data.json")
    def test_json_file_provider(self, datafiles):
        file_path = datafiles / Path("example_source_data.json")
        example_json_data_provider = JsonFileSourceDataProvider(
            file_path, lambda d: d["data"]  # type:ignore[call-overload]
        )
        assert example_json_data_provider.get_data() == [{"asd": "fgh"}, {"qwe": "rtz"}]
