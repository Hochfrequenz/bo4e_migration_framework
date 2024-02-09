"""
general data models for migrations
"""

import uuid
from abc import ABC

from pydantic import BaseModel, Field  # pylint: disable=no-name-in-module


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
