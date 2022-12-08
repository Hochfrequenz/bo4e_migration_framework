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
```mermaid
graph TD
    A[Source System A] -->|System A DB Dump| A2[Source A Data Model: A JSON Extract]
    B[Source System B] -->|System B CSV Export| B2[Source B Data Model: B CSV Files]
    A2 -->|SourceAToBo4eDataSetMapper| C{Intermediate BO4E Layer}
    B2 -->|SourceBToBo4eDataSetMapper| C
    C -->|validations| C
    C -->|Bo4eDataSetToTarget1Mapper| D1[Target 1 Data Model]
    C -->|Bo4eDataSetToTarget2Mapper| D2[Target 2 Data Model]
    C -->|Bo4eDataSetToTarget3Mapper| D3[Target 3 Data Model]
    D1 -->L1[Target 1 Loader]
    D2 -->L2[Target 2 Loader]
    D3 -->L3[Target 3 Loader]
```

## How to use this Repository on Your Machine (Development)

Please follow the [instructions in our Python Template Repository](https://github.com/Hochfrequenz/python_template_repository).
tl;dr: `tox`.

## Contribute

You are very welcome to contribute to this template repository by opening a pull request against the main branch.
