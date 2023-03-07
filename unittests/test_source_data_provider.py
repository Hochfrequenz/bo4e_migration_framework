import logging
from pathlib import Path
from typing import List

import pytest  # type:ignore[import]

from bomf.provider import JsonFileSourceDataProvider, KeyTyp, ListBasedSourceDataProvider, SourceDataProvider


class LegacyDataSystemDataProvider(SourceDataProvider):
    """
    a dummy for access to a legacy system from which we want to migrate data
    """

    def get_entry(self, key: KeyTyp) -> str:
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
        with pytest.raises(KeyError):
            _ = example_json_data_provider.get_entry("something unknown")


class TestListBasedSourceDataProvider:
    def test_list_based_provider(self, caplog):
        caplog.set_level(logging.DEBUG, logger=ListBasedSourceDataProvider.__module__)
        my_provider = ListBasedSourceDataProvider(["foo", "bar", "baz"], key_selector=lambda x: x)
        assert len(my_provider.get_data()) == 3
        assert my_provider.get_entry("bar") == "bar"
        assert "Read 3 records from ['foo', 'bar', 'baz']" in caplog.messages

    def test_list_based_provider_key_warning(self, caplog):
        caplog.set_level(logging.WARNING, logger=ListBasedSourceDataProvider.__module__)
        my_provider = ListBasedSourceDataProvider(["fooy", "fooz" "bar", "baz"], key_selector=lambda x: x[0:3])
        assert len(my_provider.get_data()) == 3
        assert (
            "There are 2>1 entries for the key 'foo'. You might miss entries because the key is not unique."
            in caplog.messages
        )
