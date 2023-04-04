import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from bomf.loader.entityloader import EntityLoader, EntityLoadingResult, PydanticJsonFileEntityLoader


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


class MyLoader(PydanticJsonFileEntityLoader[MyPydanticClass]):
    """entity loader fo my pydantic class"""


class TestJsonFileEntityLoader:
    async def test_dumping_to_file(self, tmp_path):
        my_entities = [MyPydanticClass(foo="asd", bar=123), MyPydanticClass(foo="qwe", bar=456)]
        file_path = Path(tmp_path) / Path("foo.json")
        my_loader = MyLoader(file_path)
        await my_loader.load_entities(my_entities)
        del my_loader
        with open(file_path, "r", encoding="utf-8") as infile:
            json_body = json.load(infile)
        assert len(json_body) == 2
        assert json_body == [{"foo": "asd", "bar": 123}, {"foo": "qwe", "bar": 456}]
