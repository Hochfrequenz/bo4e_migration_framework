import asyncio
from datetime import timedelta
from typing import Optional

import pytest
from pydantic import BaseModel, Required

from bomf import ValidatorSet
from bomf.model import Bo4eDataSet
from bomf.validation.core import ValidatorParamInfos, ValidatorType


class Wrapper(BaseModel):
    x: str
    z: Optional[str] = Required


class DataSetTest(Bo4eDataSet):
    x: str
    y: int
    z: Wrapper
    a: int = -1


dataset_instance = DataSetTest(x="lo16", y=16, z=Wrapper.construct(x="Hello"))
finishing_order: list[ValidatorType]


async def check_x_expensive(x: str) -> None:
    await asyncio.sleep(0.3)
    finishing_order.append(check_x_expensive)


async def check_y_positive(y: int) -> None:
    if y < 0:
        raise ValueError("y is not positive")
    finishing_order.append(check_y_positive)


async def check_xy_ending(x: str, y: int) -> None:
    if not x.endswith(str(y)):
        raise ValueError("x does not end with y")
    finishing_order.append(check_xy_ending)


async def check_required_and_optional(zx: str, zz: Optional[str] = None) -> None:
    assert zx == "Hello"
    assert zz is None


async def check_with_param_info(x: str, _param_infos: dict[str, ValidatorParamInfos], zz: str = "test"):
    assert len(_param_infos) == 2
    assert "x" in _param_infos
    assert "zz" in _param_infos
    assert str == _param_infos["x"].param_type
    assert str == _param_infos["zz"].param_type
    assert _param_infos["x"].required
    assert _param_infos["x"].provided
    assert not _param_infos["zz"].required
    assert not _param_infos["zz"].provided
    assert ["x"] == _param_infos["x"].attribute_path
    assert ["z", "z"] == _param_infos["zz"].attribute_path


async def unprovided_but_required(zz: str):
    pass


async def check_fail(x: str) -> None:
    raise ValueError("I failed (on purpose! :O)")


async def check_fail2(y: int) -> None:
    raise ValueError("I failed (on purpose! :O - again :OOO)")


async def check_fail3(y: int) -> None:
    raise ValueError("This shouldn't be raised")


async def no_params():
    pass


async def special_param_type_check_fail(x: str, _param_infos: dict[int, float]):
    pass


async def missing_annotation_y(x: str, y) -> None:
    pass


async def unmapped_param_rofl(x: str, rofl: str) -> None:
    pass


async def type_check_fail_y(x: str, y: str) -> None:
    pass


def not_async(x: str) -> None:
    pass


class TestValidation:
    async def test_generic_type(self):
        """
        This test ensures, that the data_set_type property works as expected.
        """
        validator_set = ValidatorSet[DataSetTest]()
        assert validator_set.data_set_type == DataSetTest

    async def test_async_validation(self):
        """
        This test checks if the validation functions run concurrently by just ensuring that the expensive task
        (simulated with sleep) will finish always at last.
        """
        global finishing_order
        finishing_order = []
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_x_expensive, {"x": "x"})
        validator_set.register(check_y_positive, {"y": "y"})
        await validator_set.validate(dataset_instance)
        assert finishing_order == [check_y_positive, check_x_expensive]

    async def test_depend_validation(self):
        """
        This test checks if the feature to define a dependent check works properly.
        This is achieved by setting up a validation function depending on an expensive task. The expensive task
        should finish first.
        """
        global finishing_order
        finishing_order = []
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_x_expensive, {"x": "x"})
        validator_set.register(check_xy_ending, {"x": "x", "y": "y"}, depends_on=[check_x_expensive])
        await validator_set.validate(dataset_instance)
        assert finishing_order == [check_x_expensive, check_xy_ending]

    async def test_depend_and_async_validation(self):
        """
        This test is a mix of the previous two and checks if the finishing order is as expected.
        """
        global finishing_order
        finishing_order = []
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_x_expensive, {"x": "x"})
        validator_set.register(check_y_positive, {"y": "y"})
        validator_set.register(check_xy_ending, {"x": "x", "y": "y"}, depends_on=[check_x_expensive, check_y_positive])
        await validator_set.validate(dataset_instance)
        assert finishing_order == [check_y_positive, check_x_expensive, check_xy_ending]

    async def test_failing_validation(self):
        """
        Tests if a failing validation behaves as expected.
        """
        global finishing_order
        finishing_order = []
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_y_positive, {"y": "y"})
        validator_set.register(check_fail, {"x": "x"})
        validator_set.register(check_fail2, {"y": "y"})
        validator_set.register(check_fail3, {"y": "y"}, depends_on=[check_fail])
        with pytest.raises(ExceptionGroup) as error_group:
            await validator_set.validate(dataset_instance)

        sub_exception_msgs = {str(exception) for exception in error_group.value.exceptions}
        assert any("I failed (on purpose! :O)" in sub_exception_msg for sub_exception_msg in sub_exception_msgs)
        assert any(
            "I failed (on purpose! :O - again :OOO)" in sub_exception_msg for sub_exception_msg in sub_exception_msgs
        )
        assert any(
            "Execution abandoned due to uncaught exceptions in dependent validators" in sub_exception_msg
            for sub_exception_msg in sub_exception_msgs
        )
        assert len(sub_exception_msgs) == 3

    @pytest.mark.parametrize(
        ["validator_func", "param_map", "expected_error"],
        [
            pytest.param(
                missing_annotation_y,
                {"x": "x", "y": "y"},
                "The parameter y has no annotated type.",
                id="Missing parameter type annotation",
            ),
            pytest.param(
                unmapped_param_rofl,
                {"x": "x"},
                "The parameter list of the validator function must match the parameter_map. " "['x', 'rofl'] != ['x']",
                id="Unmapped parameter",
            ),
            pytest.param(
                not_async,
                {"x": "x"},
                "The provided validator function has to be a coroutine (e.g. use async).",
                id="Function not async",
            ),
            pytest.param(no_params, {}, "The validator function must take at least one argument.", id="No params"),
        ],
    )
    async def test_illegal_validator_functions(
        self, validator_func: ValidatorType, param_map: dict[str, str], expected_error: str
    ):
        validator_set = ValidatorSet[DataSetTest]()
        with pytest.raises(ValueError) as error:
            validator_set.register(validator_func, param_map)

        assert str(error.value) == expected_error

    @pytest.mark.parametrize(
        ["validator_func", "param_map", "expected_error"],
        [
            pytest.param(
                special_param_type_check_fail,
                {"x": "x"},
                "type of keys of _param_infos must be int; got str instead",
                id="Special param wrong type",
            ),
            pytest.param(
                type_check_fail_y,
                {"x": "x", "y": "y"},
                "type of y must be str; got int instead",
                id="Wrong parameter type",
            ),
        ],
    )
    async def test_type_error(self, validator_func: ValidatorType, param_map: dict[str, str], expected_error: str):
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(validator_func, param_map)
        with pytest.raises(TypeError) as error:
            await validator_set.validate(dataset_instance)

        assert str(error.value) == expected_error

    async def test_timeout(self):
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_x_expensive, {"x": "x"}, timeout=timedelta(milliseconds=100))
        with pytest.raises(ExceptionGroup) as error_group:
            await validator_set.validate(dataset_instance)
        sub_exception_msgs = [str(exception) for exception in error_group.value.exceptions]
        assert len(sub_exception_msgs) == 1
        assert "Timeout (0.1s) during execution of validator 'check_x_expensive'" in sub_exception_msgs[0]

    async def test_unprovided_but_required(self):
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(unprovided_but_required, {"zz": "z.z"})
        with pytest.raises(ExceptionGroup) as error_group:
            await validator_set.validate(dataset_instance)

        assert len(error_group.value.exceptions) == 1
        assert "zz is required but not existent in the provided data set. Couldn't find z in DataSetTest.z." in str(
            error_group.value.exceptions[0]
        )

    async def test_map_special_param(self):
        validator_set = ValidatorSet[DataSetTest]()
        with pytest.raises(ValueError) as error:
            validator_set.register(check_with_param_info, {"x": "x", "zz": "z.z", "_param_infos": "y"})

        assert "Special parameters cannot be mapped." in str(error.value)

    async def test_required_and_optional(self):
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_required_and_optional, {"zx": "z.x", "zz": "z.z"})
        await validator_set.validate(dataset_instance)

    async def test_param_info(self):
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_with_param_info, {"x": "x", "zz": "z.z"})
        await validator_set.validate(dataset_instance)
