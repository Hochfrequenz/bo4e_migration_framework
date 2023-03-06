import asyncio
from datetime import timedelta

import pytest

from bomf.model import Bo4eDataSet
from bomf.validation import ValidatorSet, ValidatorType


class DataSetTest(Bo4eDataSet):
    x: str
    y: int


dataset_instance = DataSetTest(x="lo16", y=16)
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


async def check_fail(x: str) -> None:
    raise ValueError("I failed (on purpose! :O)")


async def check_fail2(y: int) -> None:
    raise ValueError("I failed (on purpose! :O - again :OOO)")


async def check_fail3(y: int) -> None:
    raise ValueError("This shouldn't be raised")


async def incorrectly_annotated(x: str):
    pass


async def incorrectly_annotated2(x: str, y) -> None:
    pass


async def incorrectly_annotated3(x: str, z: str) -> None:
    pass


async def incorrectly_annotated4(x: str, y: str) -> None:
    pass


def not_async(x: str) -> None:
    pass


class TestValidation:
    async def test_async_validation(self):
        """
        This test checks if the validation functions run concurrently by just ensuring that the expensive task
        (simulated with sleep) will finish always at last.
        """
        global finishing_order
        finishing_order = []
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_x_expensive)
        validator_set.register(check_y_positive)
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
        validator_set.register(check_x_expensive)
        validator_set.register(check_xy_ending, depends_on=[check_x_expensive])
        await validator_set.validate(dataset_instance)
        assert finishing_order == [check_x_expensive, check_xy_ending]

    async def test_depend_and_async_validation(self):
        """
        This test is a mix of the previous two and checks if the finishing order is as expected.
        """
        global finishing_order
        finishing_order = []
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_x_expensive)
        validator_set.register(check_y_positive)
        validator_set.register(check_xy_ending, depends_on=[check_x_expensive, check_y_positive])
        await validator_set.validate(dataset_instance)
        assert finishing_order == [check_y_positive, check_x_expensive, check_xy_ending]

    async def test_failing_validation(self):
        """
        Tests if a failing validation behaves as expected.
        """
        global finishing_order
        finishing_order = []
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_y_positive)
        validator_set.register(check_fail)
        validator_set.register(check_fail2)
        validator_set.register(check_fail3, depends_on=[check_fail])
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
        ["validator_func", "expected_error"],
        [
            pytest.param(
                incorrectly_annotated,
                "Incorrectly annotated validator function: The return type must be 'None'.",
                id="Missing return type",
            ),
            pytest.param(
                incorrectly_annotated2,
                "Incorrectly annotated validator function: Arguments ['y'] have no type annotation.",
                id="Missing 1/2 argument type",
            ),
            pytest.param(
                incorrectly_annotated3,
                "Argument 'z' does not exist as field in the DataSet "
                "'<class 'unittests.test_validation.DataSetTest'>'.",
                id="Argument does not exist in dataset",
            ),
            pytest.param(
                incorrectly_annotated4,
                "Incorrectly annotated validator function: The annotated type of argument 'y' mismatches the type "
                "in the DataSet: '<class 'str'>' != '<class 'int'>'",
                id="Wrong argument type",
            ),
            pytest.param(
                not_async,
                "The provided validator function has to be a coroutine (e.g. use async).",
                id="Function not async",
            ),
        ],
    )
    async def test_illegal_validator_functions(self, validator_func: ValidatorType, expected_error: str):
        validator_set = ValidatorSet[DataSetTest]()
        with pytest.raises(ValueError) as error:
            validator_set.register(validator_func)

        assert str(error.value) == expected_error

    async def test_timeout(self):
        validator_set = ValidatorSet[DataSetTest]()
        validator_set.register(check_x_expensive, timeout=timedelta(milliseconds=100))
        with pytest.raises(ExceptionGroup) as error_group:
            await validator_set.validate(dataset_instance)
        sub_exception_msgs = [str(exception) for exception in error_group.value.exceptions]
        assert len(sub_exception_msgs) == 1
        assert "Timeout (0.1s) during execution of validator 'check_x_expensive'" in sub_exception_msgs[0]
