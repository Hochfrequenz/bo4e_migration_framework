# BO4E Migration Framework (bomf)

BOMF is the BO4E Migration Framework.
This repository contains the code of the Python package [bomf](https://pypi.org/project/bomf).

![Unittests status badge](https://github.com/Hochfrequenz/bo4e_migration_framework/workflows/Unittests/badge.svg)
![Coverage status badge](https://github.com/Hochfrequenz/bo4e_migration_framework/workflows/Coverage/badge.svg)
![Linting status badge](https://github.com/Hochfrequenz/bo4e_migration_framework/workflows/Linting/badge.svg)
![Black status badge](https://github.com/Hochfrequenz/bo4e_migration_framework/workflows/Black/badge.svg)
![PyPi Status Badge](https://img.shields.io/pypi/v/bomf)

## Rationale
bomf is a framework, that allows its users to migrate data
- from source systems (starting with the raw data extracts)
- into an intermediate, common BO4E based data layer.
- From there map data to individual target system data models
- and finally create records in target systems (aka "loading").

The framework
- encourages user to program consistent data processing pipelines from any source to any target system 
- enforces users to adapt to structured and consistent patterns
- and by doing so will lead to higher chances for maintainable and reusable code.

## Architeture / Overview
The overall setup for a migration from 1-n source systems (A, B, C...) to 1-m target systems (1,2, 3...) might look like this:

```mermaid
graph TD
    A[Source System A] -->|System A DB Dump| A2[Source A Data Model: A JSON Extract]
    B[Source System B] -->|System B CSV Export| B2[Source B Data Model: B CSV Files]
    A2 -->|SourceAToBo4eDataSetMapper| C{Intermediate BO4E Layer aka DataSets}
    B2 -->|SourceBToBo4eDataSetMapper| C
    C -->|validations| C
    C -->|Bo4eDataSetToTarget1Mapper| D1[Target 1 Data Model]
    C -->|Bo4eDataSetToTarget2Mapper| D2[Target 2 Data Model]
    C -->|Bo4eDataSetToTarget3Mapper| D3[Target 3 Data Model]
    D1 -->L1[Target 1 Loader]
    D2 -->L2[Target 2 Loader]
    D3 -->L3[Target 3 Loader]
    L1 -->M1[Target System 1]
    L2 -->M2[Target System 2]
    L3 -->M3[Target System 3]
```
The Intermediate BO4E Layer (that consists of different so called DataSets) is kind of a contract between the code that maps *from the source data model* and the code that maps *to the target data model*.

### Data Migration Flow
The migration of specific data from source to target is always the same:
```mermaid
graph TD
    A1{Source Data 1} -->|Export| B1(All source data 1 extracts)
    B1 -->C1[Filter on source data 1 model aka Pre-Select 1]
    A2{Source Data 2} -->|Export| B2(All source data 2 extracts)
    B2 -->C2[Filter on source data 2 model aka Pre-Select 2]
    C1 -->|do not match filter predicate| Z{discarded data}
    C1 -->|match filter criteria| M(Custom Logic: SourceDataSetToBo4EDataSetMapper) 
    C2 -->|do not match filter predicate| Z
    C2 -->|match filter criteria| M
    M -->|mapping| E(BO4E Data Sets)
    E -->F[Validation]
    F -->|obeys a validation rule|E
    F -->|violate any validation rule|Z
    F -->|passes all validations| G[BO4E to Target Mapper]
    G -->|mapping| H(target data model)
    H -->I[Target Loader]
    I -->|load target model|L1[Loader: 1. load to target]
    L1 -->|first: load to|T{Target System}
    L1 -->|then|L2[Loader: 2 optionally poll until target has processed data]
    L2 -->|second: poll until|T
    L2 -->|then|L3[Loader: 3 optionally verify the data have been processed correctly]
    L3 -->|finally: verify|T
    L3 -->|verification failed|Z
    L1 -->|loading failed|Z
    L3 -->|verification successful|Y[The End.]
    Z-->Z1[Monitoring and Logging]
    Z1-->Z2[Human Analyst]
    Z2 -.->|manually checks| T
    Z2 -.->|feedback: heuristically define new rules for|F
    Z2 -.->|feedback: heurisically define new filters for|C
```


## How to use this Repository on Your Machine (Development)

Please follow the [instructions in our Python Template Repository](https://github.com/Hochfrequenz/python_template_repository).
tl;dr: `tox`.

## Contribute

You are very welcome to contribute to this template repository by opening a pull request against the main branch.
