# Project Summary

## Overview

This repository is a small AIS indicator calculation project centered around a single Python script:

- `ais_indicator_pipeline.py`

The script reads AIS Excel data, cleans vessel track points, matches points into five built-in geographic regions, computes traffic indicators, detects potential conflict events, and writes CSV outputs.

## Current Repository Structure

- `ais_indicator_pipeline.py`: Main pipeline script. Contains CLI parsing, data cleaning, spatial matching, metrics aggregation, conflict detection, and CSV export.
- `2022-04-24.xlsx`: Source AIS Excel file currently present in the repository.
- `requirements.txt`: Minimal pip dependency list for running the script in a Python virtual environment.
- `cleaned_sample.csv`: Sample of cleaned AIS records written by the pipeline.
- `site_timeslice_metrics.csv`: Intermediate time-slice metrics by region and time bin.
- `site_indicator_raw.csv`: Final site-level raw indicator output.
- `conflict_events.csv`: Conflict event output. Currently almost empty for the available sample.
- `environment.yml`: Recommended Conda environment definition for this project.

## Core Data Flow

The pipeline in `ais_indicator_pipeline.py` follows this sequence:

1. Expand input file list from `--ais`.
2. Read AIS data from `.xlsx` in chunks using `openpyxl`.
3. Normalize column names and validate required fields.
4. Convert Unix timestamps to UTC datetimes.
5. Filter invalid MMSI, coordinates, speed, and course values.
6. Remove abnormal jump points based on inferred short-interval speed.
7. Bucket records into time bins.
8. Match cleaned points into five built-in polygon regions `R1` to `R5`.
9. Compute per-region and per-time-slice metrics:
   - `density_raw`
   - `complexity_raw`
10. Project coordinates and detect potential vessel conflict events.
11. Merge duplicate conflict events within a time window.
12. Export final CSV outputs.

## Built-in Regions

The script does not load region geometry from external files. Instead, it hardcodes five polygon regions in `get_5_regions()` and labels them:

- `R1`
- `R2`
- `R3`
- `R4`
- `R5`

These are created from DMS coordinates and converted into `shapely` polygons via `geopandas`.

## Output Files

The pipeline writes three main outputs:

- `cleaned_sample.csv`
  A sample of up to 2000 cleaned AIS records from the first valid chunk.
- `site_timeslice_metrics.csv`
  Time-slice metrics by `site_id` and `time_bin`.
- `site_indicator_raw.csv`
  Final site-level raw indicators:
  - `density_raw`: vessel traffic density
  - `complexity_raw`: traffic flow complexity
  - `conflict_freq_per_day`: regional conflict frequency
- `conflict_events.csv`
  Pairwise near-conflict events with DCPA and TCPA metrics.

## Key Script Conventions

The script expects these logical input columns after normalization:

- `timestamp`
- `mmsi`
- `lat`
- `lon`
- `speed`
- `course`

It also supports several aliases, for example:

- `time`, `ts`, `datetime` -> `timestamp`
- `longitude`, `lng` -> `lon`
- `latitude` -> `lat`
- `sog`, `speed_over_ground` -> `speed`
- `cog`, `course_over_ground` -> `course`

Only `.xlsx` input is supported at the moment.

## Important Runtime Note

There is a mismatch between the script default input and the file currently stored in the repository:

- Script default: `4-24.xlsx`
- Actual file: `2022-04-24.xlsx`

Because of this, the safest way to run the project is to pass the input file explicitly:

```powershell
python .\ais_indicator_pipeline.py --ais .\2022-04-24.xlsx
```

## Verified Runtime Fixes

The current script has already been adjusted to run correctly in the checked repository state:

- `load_sites()` now supports both `GeoSeries.union_all()` and the older `GeoSeries.unary_union` fallback.
- `detect_conflicts()` converts grouped `pandas.Series` objects to NumPy arrays before positional indexing, avoiding `KeyError` caused by preserved row labels.
- Final console output uses plain text and avoids emoji characters so it can print correctly in a default Windows `gbk` terminal.

## Environment Recommendation

Recommended runtime:

- Python `3.11`
- A Python virtual environment created with `venv` and installed from `requirements.txt`
- Conda environment created from `environment.yml` is still possible, but `venv` is the simplest verified setup for this repository on Windows

Suggested setup:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python .\ais_indicator_pipeline.py --ais .\2022-04-24.xlsx
```

Alternative Conda setup:

```powershell
conda env create -f environment.yml
conda activate ais-indicator
python .\ais_indicator_pipeline.py --ais .\2022-04-24.xlsx
```

## Current Observed Sample Output

From the CSV files currently present in the repository, only region `R4` appears in the final output sample:

- `density_raw = 0.002661653059799746`
- `complexity_raw = 0.0`
- `conflict_freq_per_day = 0.0`

This suggests the current sample output is minimal and may not reflect the full behavior of the pipeline on broader input data.

## Project Constraints And Operating Assumptions

- The repository is effectively a single-script data pipeline rather than a multi-module application.
- No test suite, package metadata, or README is currently present.
- No external region definition file is used; region geometry is embedded directly in code.
- The workflow is batch-oriented and file-based.
- The project depends on scientific and geospatial Python packages, but it can run successfully in either `venv + pip` or Conda.
