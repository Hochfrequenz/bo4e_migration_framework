import asyncio
import json
import tempfile
from pathlib import Path
from typing import Optional, Type

import pytest
from pydantic import BaseModel, RootModel
from typing_extensions import deprecated

from bomf.loader.entityloader import (
    EntityLoader,
    EntityLoadingResult,
    JsonFileEntityLoader,
    PydanticJsonFileEntityLoader,
)


class _ExampleEntity:
    pass


class TestEntityLoader:
    class _ExampleEntityLoader(EntityLoader):
        def __init__(self):
            self.sanitize_called: bool = False
            self.verification_called: bool = False
            self.loading_called: bool = False
            self.polling_called: bool = False

        def sanitize(self, entity: _ExampleEntity) -> None:
            assert entity is not None
            self.sanitize_called = True

        async def verify(self, entity: _ExampleEntity, id_in_target_system: Optional[str] = None) -> bool:
            self.verification_called = True
            return True

        async def load_entity(self, entity: _ExampleEntity) -> Optional[EntityLoadingResult]:
            self.loading_called = True
            return EntityLoadingResult(id_in_target_system="foo", polling_task=self.polling_callback("foo"))

        async def polling_callback(self, entity_id: str):
            assert entity_id == "foo"
            self.polling_called = True

    async def test_all_overrides_are_called(self):
        example_loader = TestEntityLoader._ExampleEntityLoader()
        result = await example_loader.load(_ExampleEntity())
        assert example_loader.sanitize_called is True
        assert example_loader.loading_called is True
        assert example_loader.polling_called is True
        assert example_loader.verification_called is True

        assert result.was_loaded_successfully is True
        assert result.loaded_at is not None
        assert result.verified_at is not None
        assert result.verified_at >= result.loaded_at
        assert result.loading_error is None

    async def test_all_overrides_are_called_batch(self):
        example_loader = TestEntityLoader._ExampleEntityLoader()
        result = await example_loader.load_entities([_ExampleEntity()])
        assert example_loader.sanitize_called is True
        assert example_loader.loading_called is True
        assert example_loader.polling_called is True
        assert example_loader.verification_called is True

        assert result[0].was_loaded_successfully is True
        assert result[0].loaded_at is not None
        assert result[0].verified_at is not None
        assert result[0].verified_at >= result[0].loaded_at
        assert result[0].loading_error is None

    async def test_there_is_a_default_sanitize_step(self):
        class _ExampleEntityLoaderWithOutSanitize(EntityLoader):
            # no def sanitize()
            async def verify(self, entity: _ExampleEntity, id_in_target_system: Optional[str] = None) -> bool:
                return True

            async def load_entity(self, entity: _ExampleEntity) -> Optional[EntityLoadingResult]:
                return None

        example_loader = _ExampleEntityLoaderWithOutSanitize()
        result = await example_loader.load(_ExampleEntity())  # must not crash

        assert result.was_loaded_successfully is True
        assert result.loaded_at is not None
        assert result.verified_at is not None
        assert result.verified_at >= result.loaded_at
        assert result.loading_error is None

    async def test_all_overrides_are_called_on_error(self):
        class _ExampleEntityLoaderThatCrashesOnLoad(EntityLoader):
            def __init__(self):
                self.sanitize_called: bool = False
                self.loading_called: bool = False

            def sanitize(self, entity: _ExampleEntity) -> None:
                assert entity is not None
                self.sanitize_called = True

            async def verify(self, entity: _ExampleEntity, id_in_target_system: Optional[str] = None) -> bool:
                raise NotImplementedError()

            async def load_entity(self, entity: _ExampleEntity) -> Optional[EntityLoadingResult]:
                self.loading_called = True
                raise ValueError("Something is wrong")

        example_loader = _ExampleEntityLoaderThatCrashesOnLoad()
        result = await example_loader.load(_ExampleEntity())
        assert example_loader.sanitize_called is True
        assert example_loader.loading_called is True

        assert result.was_loaded_successfully is False
        assert result.loaded_at is None
        assert result.verified_at is None
        assert isinstance(result.loading_error, ValueError) is True


class MyPydanticClass(BaseModel):
    foo: str
    bar: int


class MyPydanticOnlyLoader(PydanticJsonFileEntityLoader[MyPydanticClass]):
    """entity loader for my pydantic class; does not use any json.load/dump functions"""


@deprecated("use PydanticJsonFileEntityLoader instead; this is just here to keep the coverage of JsonFileEntityLoader")
class LegacyPydanticJsonFileEntityLoader(JsonFileEntityLoader[MyPydanticClass]):
    """
    A json file entity loader specifically for pydantic models (legacy code)
    """

    def __init__(self, file_path: Path):
        """provide a file path"""
        super().__init__(
            file_path=file_path,
            list_encoder=lambda x: [y.model_dump() for y in RootModel[list[MyPydanticClass]](root=x).root],
        )


class TestPydanticJsonFileEntityLoader:
    @pytest.mark.parametrize("number_of_models", [2, 20, 2000])
    @pytest.mark.parametrize(
        "loader_class", [pytest.param(MyPydanticOnlyLoader), pytest.param(LegacyPydanticJsonFileEntityLoader)]
    )
    async def test_dumping_to_file_via_load_entities(
        self, number_of_models: int, loader_class: Type[EntityLoader[MyPydanticClass]], tmp_path
    ):
        my_entities = [MyPydanticClass(foo="asd", bar=x) for x in range(number_of_models)]
        file_path = Path(tmp_path) / Path("foo.json")
        my_loader = loader_class(file_path)  # type:ignore[call-arg]
        await my_loader.load_entities(my_entities)
        del my_loader
        with open(file_path, "r", encoding="utf-8") as infile:
            json_body = json.load(infile)
        assert len(json_body) == number_of_models
        assert json_body == [{"foo": "asd", "bar": x} for x in range(number_of_models)]

    @pytest.mark.parametrize("number_of_models", [2, 20, 2000])
    @pytest.mark.parametrize(
        "loader_class", [pytest.param(MyPydanticOnlyLoader), pytest.param(LegacyPydanticJsonFileEntityLoader)]
    )
    async def test_dumping_to_file_via_load_entity(
        self, number_of_models: int, loader_class: Type[EntityLoader[MyPydanticClass]], tmp_path
    ):
        my_entities = [MyPydanticClass(foo="asd", bar=x) for x in range(number_of_models)]
        file_path = Path(tmp_path) / Path("foo.json")
        my_loader = loader_class(file_path)  # type:ignore[call-arg]
        loading_tasks = [my_loader.load_entity(x) for x in my_entities]
        await asyncio.gather(*loading_tasks)
        del my_loader
        with open(file_path, "r", encoding="utf-8") as infile:
            json_body = json.load(infile)
        assert len(json_body) == number_of_models
        # we cannot guarantee the order of the entities

    @pytest.mark.parametrize("load_multiple", [True, False])
    @pytest.mark.parametrize(
        "loader_class", [pytest.param(MyPydanticOnlyLoader), pytest.param(LegacyPydanticJsonFileEntityLoader)]
    )
    async def test_loader_doesnt_crash_for_empty_file(
        self, loader_class: Type[EntityLoader[MyPydanticClass]], load_multiple: bool
    ):
        json_file_path: Path
        try:
            with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp_file:
                json_file_path = Path(tmp_file.name)
                assert json_file_path.exists()
                json_file_loader = loader_class(json_file_path)  # type:ignore[call-arg]
                if load_multiple:
                    _ = await json_file_loader.load_entities([])
                else:
                    _ = await json_file_loader.load_entity(MyPydanticClass(foo="asd", bar=123))
        finally:
            json_file_path.unlink()
