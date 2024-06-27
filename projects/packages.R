# References to supporting packages not directly used
languageserver::run

# Attach packages used in the entire pipeline here
library(targets)
library(tarchetypes)

if (interactive()) {
  library(duckdb)
}
