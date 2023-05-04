"""
Source Data Provider Filters combine the features of a filter with the features of a Source Data Provider.
"""

from typing import Callable, Generic, Literal, Optional, overload

from bomf import KeyTyp, SourceDataProvider
from bomf.filter import Candidate, Filter
from bomf.provider import JsonFileSourceDataProvider, ListBasedSourceDataProvider

ASourceDataProvider = SourceDataProvider[Candidate, KeyTyp]


# pylint:disable=too-few-public-methods
class SourceDataProviderFilter(Generic[Candidate, KeyTyp]):
    """
    a filter that works on and returns a CandidateSourceDataProvider
    """

    def __init__(self, candidate_filter: Filter[Candidate]):
        """
        instantiate by providing a filter which can be applied on the data providers source data models
        """
        self._filter = candidate_filter

    @overload
    async def apply(
        self, source_data_provider: JsonFileSourceDataProvider[Candidate, KeyTyp]
    ) -> SourceDataProvider[Candidate, KeyTyp]:
        ...

    @overload
    async def apply(
        self, source_data_provider: ListBasedSourceDataProvider[Candidate, KeyTyp]
    ) -> SourceDataProvider[Candidate, KeyTyp]:
        ...

    @overload
    async def apply(self, source_data_provider: ASourceDataProvider) -> ASourceDataProvider:
        ...

    @overload
    async def apply(
        self, source_data_provider: SourceDataProvider[Candidate, KeyTyp]
    ) -> SourceDataProvider[Candidate, KeyTyp]:
        ...

    @overload
    async def apply(
        self, source_data_provider: JsonFileSourceDataProvider[Candidate, KeyTyp], key_selector: Literal[None]
    ) -> SourceDataProvider[Candidate, KeyTyp]:
        ...

    @overload
    async def apply(
        self, source_data_provider: ListBasedSourceDataProvider[Candidate, KeyTyp], key_selector: Literal[None]
    ) -> SourceDataProvider[Candidate, KeyTyp]:
        ...

    @overload
    async def apply(
        self, source_data_provider: SourceDataProvider[Candidate, KeyTyp], key_selector: Literal[None]
    ) -> SourceDataProvider[Candidate, KeyTyp]:
        ...

    async def apply(
        self,
        source_data_provider: SourceDataProvider[Candidate, KeyTyp],
        key_selector: Optional[Callable[[Candidate], KeyTyp]] = None,
    ) -> SourceDataProvider[Candidate, KeyTyp]:
        """
        Reads all the data from the given source_data_provider, applies the filtering, then returns a new source
        data provider that only contains those entries that passed the filter (its predicate).

        If the provided source_data_provider is a JsonFileSourceDataProvider, then you don't have to provide a
        key_selector (let it default to None).
        However, in general, you have to specify how the data can be indexed using a key_selector which is not None.
        If you provide both a JsonFileSourceDataProvider AND a key_selector, the explicit key_selector will be used.
        """
        survivors: list[Candidate] = await self._filter.apply(await source_data_provider.get_data())
        key_selector_to_be_used: Callable[[Candidate], KeyTyp]
        if key_selector is not None:
            key_selector_to_be_used = key_selector
        else:
            key_selector_to_be_used = source_data_provider.key_selector  # type:ignore[attr-defined]
            # if this raises an attribute error you have to
            # * either provide a source_data_provider which has a key_selector attribute
            # * or explicitly provide a key_selector as (non-None) argument
        filtered_data_provider_class = ListBasedSourceDataProvider(
            source_data_models=survivors, key_selector=key_selector_to_be_used
        )
        return filtered_data_provider_class
