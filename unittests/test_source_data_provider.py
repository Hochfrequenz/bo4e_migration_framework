from pathlib import Path
from typing import List, Optional

import pytest  # type:ignore[import]

from bomf.provider import JsonFileSourceDataProvider, KeyTyp, SourceDataProvider


class LegacyDataSystemDataProvider(SourceDataProvider):
    """
    a dummy for access to a legacy system from which we want to migrate data
    """

    def get_entry(self, key: KeyTyp) -> Optional[str]:
        raise NotImplementedError("Not relevant for this test")

    def get_data(self) -> List[str]:
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
            file_path,
            data_selector=lambda d: d["data"],  # type:ignore[call-overload]
            key_selector=lambda d: d["myKey"],  # type:ignore[index]
        )
        assert example_json_data_provider.get_data() == [
            {"myKey": "hello", "asd": "fgh"},
            {"myKey": "world", "qwe": "rtz"},
        ]
        assert example_json_data_provider.get_entry("world") == {"myKey": "world", "qwe": "rtz"}
        assert example_json_data_provider.get_entry("something unknown") is None
