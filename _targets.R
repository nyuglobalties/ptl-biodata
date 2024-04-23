# Setup ======
# targets needs this attachment so tar_option_set is in scope
library(targets)
library(crew)

source(here::here("projects/prelude.R"))

# Unload targets after creation. Mainly useful for freeing up space
# after rendering reports
tar_option_set(
  memory = "transient",
  garbage_collection = TRUE,
)

# If there are more than 2 logical cores (1 actual CPU),
# go ahead and use crew. This guard is mainly here to protect
# Github Actions, which seems to deadlock or something.
if (parallel::detectCores() > 2) {
  tar_option_set(
    controller = crew_controller_local(
      workers = max(parallel::detectCores() - 1, 1),
      local_log_directory = here::here("_logs")
    ),
  )
}

options(scipen = 999)

# Edit 'config/packages.R' to attach packages needed for your pipeline
# NB: If you use renv, add `package::function` references to enable
#     renv to track "Suggested" packages, which are not always picked
#     up with `renv::snapshot()`
source(here::here("projects/packages.R"))

# Edit 'config/environment.R' to define globals from environment vars
source(here::here("projects/environment.R"))

# Add tar_targets() to this list to define the pipeline
main_targets <- list()

# Only run these targets if Box is found
if (!is.null(box_root())) {
  box_targets <- rlang::list2(
    # 0. Extraction ------
    tar_target(
      box_bodyguard_root,
      "Bangladesh Study/Data/Firstbeat Bodyguard 3 data/Wave 1 data",
    ),
    tar_target(
      box_mirage_root,
      "Bangladesh Study/Data/Mirage data/Wave 1 data",
    ),
    tar_target(
      box_esense_root,
      "Bangladesh Study/Data/eSense data/Wave 1 data",
    ),
    tar_target(
      raw_box_ecg_files,
      list.files(
        box_path(box_bodyguard_root),
        recursive = TRUE,
        full.names = TRUE,
        pattern = "ecg\\.csv$"
      )
    ),
    tar_target(
      raw_box_mirage_files,
      list.files(
        box_path(box_mirage_root),
        recursive = TRUE,
        full.names = TRUE,
        pattern = "utcmarker\\.csv$",
        ignore.case = TRUE
      )
    ),
    tar_target(
      raw_box_esense_files,
      list.files(
        box_path(box_esense_root),
        recursive = TRUE,
        full.names = TRUE,
        pattern = "\\.csv$",
        ignore.case = TRUE
      )
    ),
    tar_target(
      raw_mirage_events,
      read_mirage_events(raw_box_mirage_files),
      pattern = map(raw_box_mirage_files),
      iteration = "list",
    ),

    # 1. Transformation ------
    tar_target(
      bodyguard_file_meta,
      bg_file_meta(raw_box_ecg_files),
      pattern = map(raw_box_ecg_files),
      iteration = "list"
    ),
    tar_target(
      esense_meta_all,
      esense_file_meta(raw_box_esense_files, esense_root = box_esense_root),
      pattern = map(raw_box_esense_files),
      iteration = "list"
    ),
    tar_target(
      esense_meta,
      esense_meta_all |>
        keep(is.data.frame) |>
        tidytable::bind_rows()
    ),

    # 2. Loading ------

    # 4. Reports ------
    # tar_render(
    #   report_bodyguard_data,
    #   proj_here("reports/bodyguard.Rmd")
    # ),
  ) # end rlang::list2()

  main_targets <- append(main_targets, box_targets)
}

main_targets
