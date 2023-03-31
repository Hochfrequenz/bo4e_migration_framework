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
    current_obj: Any = obj
    for attr_name in attribute_path:
        try:
            current_obj = getattr(current_obj, attr_name)
        except AttributeError:
            return None
    check_type(".".join(attribute_path), current_obj, attribute_type)
    return current_obj


def required_field(obj: Any, attribute_path: list[str], attribute_type: type[AttrT]) -> AttrT:
    """
    Tries to query the `obj` with the provided `attribute_path`. If it is not existent,
    an AttributeError will be raised.
    If the attribute is found, the type will be checked and TypeError will be raised if the type doesn't match the
    value.
    """
    result = optional_field(obj, attribute_path, attribute_type)
    if result is None:
        raise AttributeError(f"{'.'.join(attribute_path)} is required but not provided")
    return result
