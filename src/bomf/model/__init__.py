"""
general data models for migrations
"""
import enum
import uuid
from abc import ABC
from typing import TypeVar, Union

import attrs
from bo4e.bo.geschaeftsobjekt import Geschaeftsobjekt
from bo4e.com.com import COM
from pydantic import BaseModel, Field  # pylint: disable=no-name-in-module

_SpecificBusinessObject = TypeVar("_SpecificBusinessObject", bound=Geschaeftsobjekt)
"""
an arbitrary but fixed business object type
"""

_SpecificCom = TypeVar("_SpecificCom", bound=COM)
"""
an arbitrary but fixed COM type
"""

Bo4eTyp = Union[_SpecificBusinessObject, _SpecificCom]  # pylint: disable=invalid-name


# pylint:disable=too-few-public-methods
@attrs.define(kw_only=True, auto_attribs=True)
class BusinessObjectRelation:
    """
    A business object relation describes the relation between two business object.
    E.g. a relation could have the type "has_melo" where relation_part_a is a bo4e.bo.Vertrag
    and relation_part_b is a bo4e.bo.Messlokation. Some relations are already defined in BO4E itself (e.g MaLo/MeLo)
    or MeLo/Address.
    The idea is to not enforce too much of a structure to the downstream code but still push coders to think about
    necessary relation information.
    """

    relation_type: enum.Enum = attrs.field()
    """
    The relation type describes how two business objects relate to each other.
    This is not (only) about cardinality. It's about being able to model different relations between objects.
    Think about e.g. a business partner and an address: The relation could be:
    - the address is the residential address of the business partner
    - the address is the invoice address of the business partner
    - the address is the place where the business partner was born
    All these relation types are 1:1 relations between business partners and adresses, yet they all carry different
    meaning which we'd like to distinguish in our data.
    """
    relation_part_a: Bo4eTyp = attrs.field()
    """
    one Business Object or COM
    """

    relation_part_b: Bo4eTyp = attrs.field()
    """
    another Business Object or COM
    """


class Bo4eDataSet(BaseModel, ABC):
    """
    A BO4E data set is a collection of Business Objects that relate to each other.
    """

    id: uuid.UUID = Field(default_factory=uuid.uuid4)

    def get_id(self) -> str:
        """
        returns a unique id that only this dataset uses.
        Inheriting classes may overwrite this behaviour, if the dataset has "more natural" keys.
        By default, the id is a stringified UUID that is created in the dataset constructor.
        """
        try:
            return str(self.id)
        except AttributeError as attribute_error:
            if attribute_error.name == "uuid":
                raise ValueError(
                    f"You probably forgot to call super().__init__() in the constructor of {self.__class__}"
                ) from attribute_error
            raise

    def __hash__(self):
        return hash(self.get_id())
