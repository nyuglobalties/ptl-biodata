#' Data processing prelude
#'
#' This script *needs* to be loaded at the top of each
#' project '_targets.R' file, along with a call to `library(targets)`
#'
#' For example:
#'
#' library(targets)
#' source(here::here("projects/prelude.R"))
#'
#' # ... extra code + targets pipeline below
NULL

# Just in case `source(here::here("projects/prelude.R"))` is being invoked interactively
library(targets)

## ------ COMMON ------
# Load common R functions
src_files <- list.files(
  here::here("R"),
  pattern = "\\.[rR]$",
  full.names = TRUE
)

for (f in src_files) {
  source(f)
}

# Attach common packages
source(here::here("projects/packages.R"))

# Assign common global environment variables
source(here::here("projects/environment.R"))

## ------ PROJECT-SPECIFIC ------
# Use this iff you're using targets' projects feature
# Check TAR_PROJECT
TAR_PROJECT <- Sys.getenv("TAR_PROJECT")

if (!identical(TAR_PROJECT, "")) {
  # Load project-specific R functions
  proj_r_dir <- here::here("R", TAR_PROJECT)

  if (dir.exists(proj_r_dir)) {
    proj_src_files <- list.files(
      proj_r_dir,
      pattern = "\\.[rR]$",
      full.names = TRUE
    )

    for (f in proj_src_files) {
      source(f)
    }
  }

  # Attach project-specific packages
  proj_packages <- here::here("projects", TAR_PROJECT, "packages.R")

  if (file.exists(proj_packages)) {
    source(proj_packages)
  }

  # Assign project-specific global environment variables
  proj_envs <- here::here("projects", TAR_PROJECT, "environment.R")

  if (file.exists(proj_envs)) {
    source(proj_envs)
  }

  ## ------ CLEANUP ------
  rm(src_files)

  rm(proj_r_dir)
  rm(proj_packages)
  rm(proj_envs)

  if (exists("proj_src_files")) {
    rm(proj_src_files)
  }
}
