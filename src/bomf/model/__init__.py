"""
general data models for migrations
"""
from typing import Any, Iterable, Optional, Protocol, Type, TypeVar

import attrs
import bo4e.bo.geschaeftsobjekt


# pylint:disable=too-few-public-methods
@attrs.define(kw_only=True, auto_attribs=True)
class BusinessObjectRelation:
    """
    a business object relation describes the relation between two business object
    """

    relation_type: str = attrs.field(validator=attrs.validators.instance_of(str))
    """
    the relation type describes how two business objects relate to each other
    """
    relation_part_a: Any = attrs.field()
    """
    one Business Object or COM
    """

    relation_part_b: Any = attrs.field()
    """
    another Business Object or COM
    """


SpecificBusinessObject = TypeVar("SpecificBusinessObject", bound=bo4e.bo.geschaeftsobjekt.Geschaeftsobjekt)
"""
an arbitrary but fixed business object type
"""


class Bo4eDataSet(Protocol):
    """
    A BO4E data set is a collection of Business Objects that relate to each other.
    This class just defines methods that any bo4e data set should implement (via structural subtyping) without forcing
    the data sets to inherit from a common base class.
    """

    def get_relations(self) -> Iterable[BusinessObjectRelation]:
        """
        returns all relations between the business objects
        """

    def get_business_object(
        self, bo_type: Type[SpecificBusinessObject], specification: Optional[str] = None
    ) -> SpecificBusinessObject:
        """
        Returns a business object of the provided type from the collection.
        If the type alone is not unique, you can provide an additional specification.
        """
