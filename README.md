# Play to Learn Bio Data Processing

All bio data from the field are processed for analysis here.

**NO PII IS EXPOSED IN THIS REPOSITORY**.

## Setup

In R, run

```r
renv::restore()
```

This repository only uses a handful of necessary packages, so this step is pretty quick.
Notably, this repo uses

- `tidytable`
- `stringi`
- `duckdb`

which handle most of the legwork.

## Running pipeline

To execute the pipeline, run

```r
# These environment variables need only be set once per R session
# Sys.setenv(BOX_ROOT = "C:/Users/youruser/Box") <-- This might be able to be detected automatically
Sys.setenv(SYNOLOGY_ROOT = "T:/Where/To/Save/Intermediate/Files")

# This can be run as many times as you'd like per R session
targets::tar_make()
```

in R.
