import asyncio
import re
from datetime import timedelta
from typing import Any, Iterator, Optional

import pytest
from pydantic import BaseModel, Required

from bomf import ValidationManager
from bomf.model import Bo4eDataSet
from bomf.validation import Query, QueryMappedValidator, Validator, optional_field, param, required_field
from bomf.validation.core import ValidationError, ValidatorFunctionT
from bomf.validation.core.types import (
    AsyncValidatorFunction,
    MappedValidatorT,
    SyncValidatorFunction,
    ValidatorFunction,
)
from bomf.validation.path_map import PathMappedValidator


class Wrapper(BaseModel):
    x: str
    z: Optional[str] = Required


class DataSetTest(Bo4eDataSet):
    x: str
    y: int
    z: Wrapper
    a: int = -1


class SpecialDataSet(Bo4eDataSet):
    x: list[Wrapper]
    y: str


def iter_list(list_to_iter: list[Any]) -> Iterator[tuple[Any, str]]:
    return ((el, f"[{index}]") for index, el in enumerate(list_to_iter))


dataset_instance = DataSetTest(x="lo16", y=16, z=Wrapper.construct(x="Hello"))
special_dataset_instance = SpecialDataSet(
    x=[Wrapper.construct(x="Hello"), Wrapper.construct(x="World"), Wrapper.construct(x="!")],
    y="lul",
)
finishing_order: list[ValidatorFunction]


def check_multiple_registration(x: str):
    assert param("x").param_id in ("x", "z.x")
    if param("x").param_id == "x":
        assert x == "lo16"
    else:
        assert x == "Hello"
    finishing_order.append(check_multiple_registration)


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


def check_required_and_optional(zx: str, zz: str | None = None) -> None:
    assert zx == "Hello"
    assert zz is None


def check_required_and_optional_with_utility(z: Wrapper) -> None:
    assert required_field(z, "x", str) == "Hello"
    assert optional_field(z, "z", str) is None


def check_with_param_info(x: str, zz: str = "test"):
    x_param = param("x")
    assert x_param.name == "x"
    assert x_param.param_id == "x"
    assert x_param.provided
    assert x_param.value == x
    zz_param = param("zz")
    assert zz_param.name == "zz"
    assert zz_param.param_id == "z.z"
    assert not zz_param.provided
    assert zz_param.value == zz and zz == "test"


def check_special_data_set(x: str, y: str):
    x_param = param("x")
    assert re.match(r"x\[\d+\]\.x", x_param.param_id)
    assert x_param.provided
    finishing_order.append(check_special_data_set)


def check_special_data_set_optional(x: str, y: str = "Hello"):
    x_param = param("x")
    assert re.match(r"x\[\d+\]\.x", x_param.param_id)
    assert x_param.provided
    finishing_order.append(check_special_data_set_optional)


def check_with_param_info_fail(zz: str = "test"):
    unmapped_param = param("zz")


def unprovided_but_required(zz: str):
    pass


def check_fail(x: str) -> None:
    raise ValueError("I failed (on purpose! :O)")


def check_fail2(y: int) -> None:
    raise ValueError("I failed (on purpose! :O - again :OOO)")


def check_fail3(y: int) -> None:
    raise ValueError("This shouldn't be raised")


def check_different_fails(x: str):
    if x == "Hello":
        raise ValueError("Error 1")
    else:
        raise ValueError("Error 2")


def no_params():
    pass


def missing_annotation_y(x: str, y) -> None:
    pass


def unmapped_param_rofl(x: str, rofl: str) -> None:
    pass


def type_check_fail_y(x: str, y: str) -> int:
    return 0


validator_check_multiple_registration: Validator[DataSetTest, SyncValidatorFunction] = Validator(
    check_multiple_registration
)
validator_check_x_expensive: Validator[DataSetTest, AsyncValidatorFunction] = Validator(check_x_expensive)
validator_check_y_positive: Validator[DataSetTest, AsyncValidatorFunction] = Validator(check_y_positive)
validator_check_xy_ending: Validator[DataSetTest, AsyncValidatorFunction] = Validator(check_xy_ending)
validator_check_required_and_optional: Validator[DataSetTest, SyncValidatorFunction] = Validator(
    check_required_and_optional
)
validator_check_required_and_optional_with_utility: Validator[DataSetTest, SyncValidatorFunction] = Validator(
    check_required_and_optional_with_utility
)
validator_check_with_param_info: Validator[DataSetTest, SyncValidatorFunction] = Validator(check_with_param_info)
validator_check_with_param_info_fail: Validator[DataSetTest, SyncValidatorFunction] = Validator(
    check_with_param_info_fail
)
validator_check_special_data_set: Validator[DataSetTest, SyncValidatorFunction] = Validator(check_special_data_set)
validator_check_special_data_set_optional: Validator[DataSetTest, SyncValidatorFunction] = Validator(
    check_special_data_set_optional
)
validator_unprovided_but_required: Validator[DataSetTest, SyncValidatorFunction] = Validator(unprovided_but_required)
validator_check_fail: Validator[DataSetTest, SyncValidatorFunction] = Validator(check_fail)
validator_check_fail2: Validator[DataSetTest, SyncValidatorFunction] = Validator(check_fail2)
validator_check_fail3: Validator[DataSetTest, SyncValidatorFunction] = Validator(check_fail3)
validator_check_different_fails: Validator[DataSetTest, SyncValidatorFunction] = Validator(check_different_fails)
validator_type_check_fail_y: Validator[DataSetTest, SyncValidatorFunction] = Validator(
    type_check_fail_y  # type:ignore[arg-type]
)


class TestValidation:
    async def test_async_validation_and_result(self):
        """
        This test checks if the validation functions run concurrently by just ensuring that the expensive task
        (simulated with sleep) will finish always at last.
        """
        global finishing_order
        finishing_order = []
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(PathMappedValidator(validator_check_x_expensive, {"x": "x"}))
        validation_manager.register(PathMappedValidator(validator_check_y_positive, {"y": "y"}))
        validation_summary = await validation_manager.validate(dataset_instance)
        assert validation_summary.total == 1
        assert validation_summary.num_succeeds == 1
        assert len(validation_summary.succeeded_data_sets) == 1
        assert validation_summary.succeeded_data_sets[0] == dataset_instance
        assert validation_summary.num_fails == 0
        assert len(validation_summary.data_set_errors) == 0
        assert validation_summary.num_errors_total == 0
        assert len(validation_summary.num_errors_per_id) == 0
        assert finishing_order == [check_y_positive, check_x_expensive]

    async def test_depend_validation(self):
        """
        This test checks if the feature to define a dependent check works properly.
        This is achieved by setting up a validation function depending on an expensive task. The expensive task
        should finish first.
        """
        global finishing_order
        finishing_order = []
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(PathMappedValidator(validator_check_x_expensive, {"x": "x"}))
        validation_manager.register(
            PathMappedValidator(validator_check_multiple_registration, {"x": "x"}),
        )
        validation_manager.register(
            PathMappedValidator(validator_check_multiple_registration, {"x": "z.x"}),
        )
        validation_manager.register(
            PathMappedValidator(validator_check_xy_ending, {"x": "x", "y": "y"}),
            depends_on={
                PathMappedValidator(validator_check_x_expensive, {"x": "x"}),
                PathMappedValidator(validator_check_multiple_registration, {"x": "x"}),
                PathMappedValidator(validator_check_multiple_registration, {"x": "z.x"}),
            },
        )
        validation_summary = await validation_manager.validate(dataset_instance)
        assert validation_summary.num_errors_total == 0
        assert finishing_order == [
            check_multiple_registration,
            check_multiple_registration,
            check_x_expensive,
            check_xy_ending,
        ]

    async def test_depend_and_async_validation(self):
        """
        This test is a mix of the previous two and checks if the finishing order is as expected.
        """
        global finishing_order
        finishing_order = []
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(PathMappedValidator(validator_check_x_expensive, {"x": "x"}))
        validation_manager.register(PathMappedValidator(validator_check_y_positive, {"y": "y"}))
        validation_manager.register(
            PathMappedValidator(validator_check_xy_ending, {"x": "x", "y": "y"}),
            depends_on={
                PathMappedValidator(validator_check_x_expensive, {"x": "x"}),
                PathMappedValidator(validator_check_y_positive, {"y": "y"}),
            },
        )
        validation_summary = await validation_manager.validate(dataset_instance)
        assert validation_summary.num_errors_total == 0
        assert finishing_order == [check_y_positive, check_x_expensive, check_xy_ending]

    async def test_failing_validation(self):
        """
        Tests if a failing validation behaves as expected.
        """
        global finishing_order
        finishing_order = []
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(PathMappedValidator(validator_check_y_positive, {"y": "y"}))
        validation_manager.register(PathMappedValidator(validator_check_fail, {"x": "x"}))
        validation_manager.register(PathMappedValidator(validator_check_fail2, {"y": "y"}))
        validation_manager.register(
            PathMappedValidator(validator_check_fail3, {"y": "y"}),
            depends_on={PathMappedValidator(validator_check_fail, {"x": "x"})},
        )
        validation_summary = await validation_manager.validate(dataset_instance)

        assert validation_summary.num_errors_total == 3
        sub_exception_msgs = {str(exception) for exception in validation_summary.all_errors}
        assert any("I failed (on purpose! :O)" in sub_exception_msg for sub_exception_msg in sub_exception_msgs)
        assert any(
            "I failed (on purpose! :O - again :OOO)" in sub_exception_msg for sub_exception_msg in sub_exception_msgs
        )
        assert any(
            "Execution abandoned due to failing dependent validators" in sub_exception_msg
            for sub_exception_msg in sub_exception_msgs
        )

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
                "unmapped_param_rofl misses parameter(s) {'rofl'}",
                id="Unmapped parameter",
            ),
            pytest.param(no_params, {}, "The validator function must take at least one argument", id="No params"),
        ],
    )
    async def test_illegal_validator_functions(
        self, validator_func: ValidatorFunctionT, param_map: dict[str, str], expected_error: str
    ):
        validation_manager = ValidationManager[DataSetTest]()
        with pytest.raises(ValueError) as error:
            validation_manager.register(PathMappedValidator(Validator(validator_func), param_map))

        assert str(error.value) == expected_error

    async def test_illegal_dependency_registration(self):
        validation_manager = ValidationManager[DataSetTest]()
        with pytest.raises(ValueError) as exc:
            validation_manager.register(
                PathMappedValidator(validator_check_fail, {"x": "x"}),
                depends_on={PathMappedValidator(validator_check_multiple_registration, {"x": "x"})},
            )
        assert "The specified dependency is not registered: check_multiple_registration" == str(exc.value)

    @pytest.mark.parametrize(
        ["validator", "param_map", "expected_error"],
        [
            pytest.param(
                validator_type_check_fail_y,
                {"x": "x", "y": "y"},
                "type of y must be str; got int instead",
                id="Wrong parameter type",
            ),
        ],
    )
    async def test_type_error(
        self, validator: Validator[DataSetTest, ValidatorFunctionT], param_map: dict[str, str], expected_error: str
    ):
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(PathMappedValidator(validator, param_map))
        validation_summary = await validation_manager.validate(dataset_instance)

        assert validation_summary.num_errors_total == 1
        assert expected_error in str(validation_summary.all_errors[0])

    async def test_timeout(self):
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(
            PathMappedValidator(validator_check_x_expensive, {"x": "x"}),
            timeout=timedelta(milliseconds=100),
        )
        validation_summary = await validation_manager.validate(dataset_instance)
        assert validation_summary.num_errors_total == 1
        sub_exception_msgs = [str(exception) for exception in validation_summary.all_errors]
        assert "Timeout (0.1s) during execution" in sub_exception_msgs[0]
        assert "Validator function: check_x_expensive" in sub_exception_msgs[0]

    async def test_unprovided_but_required(self):
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(PathMappedValidator(validator_unprovided_but_required, {"zz": "z.z"}))
        validation_summary = await validation_manager.validate(dataset_instance)

        assert validation_summary.num_errors_total == 1
        assert "'z.z' does not exist" in str(validation_summary.all_errors[0])

    async def test_multiple_validator_registration(self):
        global finishing_order
        finishing_order = []
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(
            PathMappedValidator(validator_check_multiple_registration, {"x": "x"}),
        )
        validation_manager.register(
            PathMappedValidator(validator_check_multiple_registration, {"x": "z.x"}),
        )
        validation_summary = await validation_manager.validate(dataset_instance)
        assert validation_summary.num_errors_total == 0
        assert len(finishing_order) == 2

    async def test_required_and_optional(self):
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(
            PathMappedValidator(validator_check_required_and_optional, {"zx": "z.x", "zz": "z.z"}),
        )
        validation_summary = await validation_manager.validate(dataset_instance)
        assert validation_summary.num_errors_total == 0

    async def test_param_info(self):
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(
            PathMappedValidator(validator_check_with_param_info, {"x": "x", "zz": "z.z"}),
        )
        validation_summary = await validation_manager.validate(dataset_instance)
        assert validation_summary.num_errors_total == 0

    @staticmethod
    def wrapper_without_self_for_coverage():
        def more_call_stack():
            param("x")

        more_call_stack()

    async def test_param_info_fail(self):
        expected_error = "did not provide parameter information for parameter 'zz'"
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(
            PathMappedValidator(validator_check_with_param_info_fail, {}),
        )
        validation_summary = await validation_manager.validate(dataset_instance)
        assert any(expected_error in str(error) for error in validation_summary.all_errors)
        with pytest.raises(RuntimeError) as error:
            param("x")
        assert (
            "You can call this function only directly from inside a function "
            "which is executed by the validation framework" == str(error.value)
        )
        with pytest.raises(RuntimeError) as error:
            TestValidation.wrapper_without_self_for_coverage()
        assert (
            "You can call this function only directly from inside a function "
            "which is executed by the validation framework" == str(error.value)
        )

    async def test_error_ids(self):
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(PathMappedValidator(validator_check_fail, {"x": "x"}))
        validation_manager.register(PathMappedValidator(validator_check_fail2, {"y": "y"}))
        validation_manager.register(
            PathMappedValidator(validator_check_fail3, {"y": "y"}),
            depends_on={PathMappedValidator(validator_check_fail, {"x": "x"})},
        )
        validation_manager.register(PathMappedValidator(validator_check_different_fails, {"x": "x"}))
        validation_manager.register(PathMappedValidator(validator_check_different_fails, {"x": "z.x"}))
        validation_summary = await validation_manager.validate(dataset_instance)
        validation_summary2 = await validation_manager.validate(dataset_instance)
        # This is just to ensure that the ID generation for the errors is not completely random and has consistency

        sub_exceptions1: dict[MappedValidatorT, ValidationError] = {
            exception.mapped_validator: exception for exception in validation_summary.all_errors
        }
        sub_exceptions2: dict[MappedValidatorT, ValidationError] = {
            exception.mapped_validator: exception for exception in validation_summary2.all_errors
        }
        assert len(sub_exceptions1) == 5
        # This is a self-consistency check to ensure that there is no unwanted randomness in the program.
        assert {str(sub_exception1) for sub_exception1 in sub_exceptions1.values()} == {
            str(sub_exception2) for sub_exception2 in sub_exceptions2.values()
        }
        # Different errors in the same function should get different error IDs.
        assert (
            sub_exceptions1[PathMappedValidator(validator_check_different_fails, {"x": "x"})].error_id
            != sub_exceptions1[PathMappedValidator(validator_check_different_fails, {"x": "z.x"})].error_id
        )
        # This ensures that the ID is constant across python sessions - as long as the line number of the raising
        # exception in `check_fail` doesn't change.
        assert sub_exceptions1[PathMappedValidator(validator_check_fail, {"x": "x"})].error_id == 1746866

    async def test_utility_required_and_optional(self):
        validation_manager = ValidationManager[DataSetTest]()
        validation_manager.register(
            PathMappedValidator(validator_check_required_and_optional_with_utility, {"z": "z"}),
        )
        validation_summary = await validation_manager.validate(dataset_instance)
        assert validation_summary.num_errors_total == 0

    async def test_special_data_set(self):
        global finishing_order
        finishing_order = []

        validation_manager = ValidationManager[SpecialDataSet]()
        validation_manager.register(
            QueryMappedValidator(
                validator_check_special_data_set,
                {"x": Query().path("x").iter(iter_list).path("x"), "y": Query().path("y")},
            ),
        )
        validation_summary = await validation_manager.validate(special_dataset_instance)
        assert validation_summary.num_fails == 0
        assert len(finishing_order) == 3
        assert all(el == check_special_data_set for el in finishing_order)

    async def test_special_data_set_errors(self):
        global finishing_order
        finishing_order = []

        validation_manager = ValidationManager[SpecialDataSet]()
        validation_manager.register(
            QueryMappedValidator(
                validator_check_special_data_set,
                {"x": Query().path("x").iter(iter_list).path("x"), "y": Query().path("y")},
            ),
        )
        validation_manager.register(
            QueryMappedValidator(
                validator_check_special_data_set_optional,
                {"x": Query().path("x").iter(iter_list).path("x"), "y": Query().path("y")},
            ),
        )
        validation_summary = await validation_manager.validate(SpecialDataSet.construct(x=special_dataset_instance.x))
        assert validation_summary.num_errors_total == 1
        assert "y not provided" in str(validation_summary.all_errors[0])
        assert len(finishing_order) == 3
        assert all(el == check_special_data_set_optional for el in finishing_order)
