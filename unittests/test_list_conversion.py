import logging

from bomf.mapper import convert_single_mapping_task_into_list_mapping_task_with_single_pokemon_catchers


class TestListMappingConversion:
    async def test_conversion(self, caplog):
        async def mapping_func(x: int) -> str:
            if x == 3:
                raise Exception("Fatal crash")
            return str(x)

        caplog.set_level(logging.ERROR, "foo")
        logger = logging.getLogger("foo")
        actual = convert_single_mapping_task_into_list_mapping_task_with_single_pokemon_catchers(mapping_func, logger)
        test_result = await actual([1, 2, 3, 4, 5])
        assert test_result == ["1", "2", "4", "5"]
        assert caplog.messages[0] == "Error while calling mapping_func on 3: Fatal crash"
