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
targets::tar_make()
```

in R.
