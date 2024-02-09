"""
This module provides a class to hold configuration values for the `MigrationStrategy`.
"""

from datetime import datetime

from pydantic import AwareDatetime, BaseModel, ConfigDict, field_validator


class MigrationConfig(BaseModel):
    """
    The constants and settings uses during the migration are stored in instances of this class.
    It allows you to change the behaviour of the migration without changing the code.
    Per migration strategy exactly one instance of MigrationConfig must be provided.
    """

    model_config = ConfigDict(extra="forbid")

    migration_key_date: AwareDatetime
    """
    Global key date for the migration.
    This should be used if a class/method needs the migration date as a key date.
    """

    catch_all_mapping_errors: bool = False
    """
    If true a pokemon catcher around the mappers will prevent any mapping error
    from crashing the migration. This might be helpful if you're on a tight schedule and just "getting through" is ok.
    However this shouldn't be True by default as it basically hides any error in the logs... and we all know that error
    logs tend to be ignored faster than we expect ;) Use it responsibly.
    The actual implementation (and whether to consider this flag at all) is still in the hand of the mapper classes.
    """

    @field_validator("migration_key_date")
    @staticmethod
    def validate_datetime_tz_aware(value: datetime) -> datetime:
        """
        Ensure that the migration_key_date is timezone-aware to prevent any errors with comparing to this date.
        """
        if value.tzinfo is None:
            raise ValueError("datetime must be timezone-aware")
        return value
