"""
Contains some useful utility functions to be used in validator functions.
"""
from typing import Any, Optional, TypeVar

from typeguard import check_type

AttrT = TypeVar("AttrT")


def optional_field(obj: Any, attribute_path: list[str], attribute_type: type[AttrT]) -> Optional[AttrT]:
    """
    Tries to query the `obj` with the provided `attribute_path`. If it is not existent, `None` will be returned.
    If the attribute is found, the type will be checked and TypeError will be raised if the type doesn't match the
    value.
    """
    try:
        return required_field(obj, attribute_path, attribute_type)
    except (AttributeError, TypeError):
        return None


def required_field(obj: Any, attribute_path: list[str], attribute_type: type[AttrT]) -> AttrT:
    """
    Tries to query the `obj` with the provided `attribute_path`. If it is not existent,
    an AttributeError will be raised.
    If the attribute is found, the type will be checked and TypeError will be raised if the type doesn't match the
    value.
    """
    current_obj: Any = obj
    for index, attr_name in enumerate(attribute_path):
        try:
            current_obj = getattr(current_obj, attr_name)
        except AttributeError as error:
            current_path = ".".join(attribute_path[0 : index + 1])
            raise AttributeError(f"'{current_path}' does not exist") from error
    check_type(".".join(attribute_path), current_obj, attribute_type)
    return current_obj
