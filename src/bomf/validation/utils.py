"""
Contains common functions for convenient usage inside validator functions.
"""
from typing import Any, Optional, TypeVar, cast

FieldT = TypeVar("FieldT")


def required_field(obj: Any, attribute_path: str, field_type: type[FieldT]) -> FieldT:
    """
    Extracts the value of the provided object by resolving the attribute path. The return type is specified as argument
    (to make mypy happy) and must match the type of the attribute (indicated by `attribute_path`).
    Raises a ValueError if the field could not be found or is None.
    Example usage:
    ```
    async def check_melo_id(messlokation: Messlokation) -> None:
        messlokations_id = required_field(messlokation, "messlokations_id", str)
        if not re.match(r"^DE\d{31}$", messlokations_id):
            raise ValueError("The Messlokations-ID has to start with 'DE' followed by 31 digits.")
    ```
    """
    assert len(attribute_path) > 0
    path_pointer = 0
    current_obj = obj
    while path_pointer < len(attribute_path):
        try:
            dot_index = attribute_path[path_pointer:-1].index(".") + path_pointer
        except ValueError:
            # No more dot in path string
            dot_index = len(attribute_path)
        attr_name = attribute_path[path_pointer:dot_index]
        if not hasattr(current_obj, attr_name) or getattr(current_obj, attr_name) is None:
            current_path = attribute_path[0:dot_index]
            raise ValueError(f"{attribute_path} is required. {current_path} not defined.")
        current_obj = getattr(current_obj, attr_name)
        path_pointer = dot_index + 1
    if type(current_obj) != field_type:
        # Normally this cannot occur. But it can happen if you use construct or supplied the wrong type in the
        # functions' argument.
        raise TypeError(f"Type mismatch: {attribute_path} is not {field_type} but {type(current_obj)}")
    return cast(FieldT, current_obj)


def optional_field(obj: Any, attribute_path: str, field_type: type[FieldT]) -> Optional[FieldT]:
    """
    Extracts the value of the provided object by resolving the attribute path. The return type is specified as argument
    (to make mypy happy) and must match the type of the attribute (indicated by `attribute_path`).
    Returns None if the field could not be found or is None.
    Example usage:
    ```
    async def check_melo_id(messlokation: Messlokation) -> None:
        messlokations_id = optional_field(messlokation, "messlokations_id", str)
        if messlokations_id is not None and not re.match(r"^DE\d{31}$", messlokations_id):
            raise ValueError("The Messlokations-ID has to start with 'DE' followed by 31 digits.")
    ```
    """
    try:
        return required_field(obj, attribute_path, field_type)
    except ValueError:
        return None
