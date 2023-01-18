"""
tests the chaining of multiple source data providers
"""
import dataclasses
from typing import Any, List, Optional

from bomf import IntermediateDataSet, KeyTyp, SourceDataProvider, SourceToBo4eDataSetMapper
from bomf.mapper import _ArbitraryButFixedAny


class _SourceContract:
    """
    on entity, e.g. a contract
    """

    pass


class _SourceCustomer:
    """
    another entity, e.g. customers
    """

    pass


class _CountractSourceDataProvider(SourceDataProvider[_SourceContract, Any]):
    def get_data(self) -> List[_SourceContract]:
        raise NotImplementedError("Not relevant for the test")

    def get_entry(self, key: Any) -> Optional[_SourceContract]:
        raise NotImplementedError("Not relevant for the test")


class _CustomerSourceProvider(SourceDataProvider[_SourceCustomer, Any]):
    def get_data(self) -> List[_SourceCustomer]:
        raise NotImplementedError("Not relevant for the test")

    def get_entry(self, key: Any) -> Optional[_SourceCustomer]:
        raise NotImplementedError("Not relevant for the test")


@dataclasses.dataclass
class _CustomerWithMultipleContracts:
    """
    combines a single customer (from the source) with multiple contracts from the source
    """

    # hier noch die quell objecte, wie sie ausm source system rauskommen, ungemapped, höchstens gefiltert
    # hier noch kein bo4e
    customer: _SourceCustomer
    contracts: List[_SourceContract]


class CombinedCustomerWithMultipleContractsDataProvider(SourceDataProvider[_CustomerWithMultipleContracts, Any]):
    """
    provide easy access to customers with multiple contracts
    """

    def __init__(self):
        """
        todo: diese konstruktor können wir übergeben, was immer wir wollen und er kann die verbindungen herstellen,
        zwischen den verschiedenen objekten.
        """

    def get_data(self) -> List[_CustomerWithMultipleContracts]:
        pass

    def get_entry(self, key: KeyTyp) -> Optional[_CustomerWithMultipleContracts]:
        pass


class DataSetWithCustomer:
    # hier der bo4e gp
    # und die bo4e adresse
    pass


class CustomerDataSetMapper(SourceToBo4eDataSetMapper[DataSetWithCustomer]):
    def create_data_set(self, source: _CustomerWithMultipleContracts) -> IntermediateDataSet:
        # der hier frisst jetzt mehrere beliebig zusammengewürftel objekte aus dem source system
        pass
