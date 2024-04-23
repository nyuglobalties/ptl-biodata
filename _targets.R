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
      raw_box_bodyguard_files,
      list.files(
        box_path(box_bodyguard_root),
        recursive = TRUE,
        full.names = TRUE
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
      raw_mirage_events,
      read_mirage_events(raw_box_mirage_files),
      pattern = map(raw_box_mirage_files),
      iteration = "list",
    ),

    # 1. Transformation ------
    tar_target(
      ecg_meta_all,
      ecg_meta_dt(raw_box_bodyguard_files, bg_root = box_bodyguard_root),
    ),
    tar_target(ecg_paths, ecg_meta_all$path),
    tar_target(
      bodyguard_time_limits,
      bg_scan_file_limits(ecg_paths),
      pattern = map(ecg_paths),
      iteration = "list",
    ),
    tar_target(
      prepped_bodyguard_time_limits,
      bodyguard_time_limits |>
        lapply(bg_prepare_file_limits) |>
        purrr::keep(is.data.frame) |>
        tidytable::bind_rows() |>
        # Reject badly-formatted timestamps
        tidytable::filter(start >= "2023-01-01") |>
        tidytable::left_join(
          ecg_meta_all |>
            tidytable::select(path, locale) |>
            unique(),
          by = "path"
        ) |>
        tidytable::mutate(
          start_adj = lubridate::force_tzs(start, locale, tzone_out = "Asia/Dhaka"),
          .after = start,
        ) |>
        tidytable::mutate(
          end_adj = lubridate::force_tzs(end, locale, tzone_out = "Asia/Dhaka"),
          .after = end,
        ),
    ),
    tar_target(
      bg_long_meta,
      bg_generate_long_table(ecg_meta_all)
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
