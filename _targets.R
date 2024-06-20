# Setup ======
# targets needs this attachment so tar_option_set is in scope
library(targets)
library(crew)

source(here::here("projects/prelude.R"))
options(digits.secs = 3)

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
      ecg_sessions_meta,
      bg_ecg_session_meta(
        raw_box_ecg_files,
        bg_root = box_bodyguard_root,
        # TRUE by default! Do `Sys.setenv(F_RUN_ALL = "FALSE")` to run in test mode
        sample_size = if (!isTRUE(F_RUN_ALL)) 2000 else NULL
      ),
    ),
    tar_target(
      ecg_files_subset,
      bg_filter_recordings(
        raw_box_ecg_files,
        ecg_sessions_meta,
        run_all = isTRUE(F_RUN_ALL)
      )
    ),
    tar_target(
      ecg_recording_meta,
      bg_ecg_recording_meta(ecg_files_subset),
      pattern = map(ecg_files_subset),
      iteration = "list"
    ),
    tar_target(
      bucket_bg_dir,
      box_path("ptl_irrrd_bio", "bodyguard")
    ),
    tar_target(
      ecg_recordings_meta,
      bg_import_ecg_to_lake(
        ecg_recording_meta,
        root_dir = bucket_bg_dir
      ),
      pattern = map(ecg_recording_meta),
      iteration = "list"
    ),
    tar_target(
      ecg_recordings,
      ecg_recording_meta |>
        discard(is.null) |>
        tidytable::bind_rows() |>
        tidytable::left_join(
          ecg_recordings_meta |>
            discard(is.null) |>
            tidytable::bind_rows() |>
            tidytable::select(-path),
          by = "id"
        ) |>
        tidytable::arrange(id_session, sequence)
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
        tidytable::bind_rows() |>
        unique() |>
        tidytable::arrange(mirage_pid, start_time) |>
        tidytable::mutate(id = seq_len(.N))
    ),
    tar_target(
      raw_mirage_events,
      read_mirage_events(raw_box_mirage_files),
      pattern = map(raw_box_mirage_files),
      iteration = "list",
    ),

    # 1. Transformation ------
    tar_target(
      bucket_partitions,
      {
        dat <- ecg_recordings |>
          tidytable::filter(!is.na(year)) |>
          tidytable::select(year, month) |>
          unique()

        lapply2(dat$year, dat$month, \(yr, mon) {
          list(
            year = yr,
            month = mon,
            recordings = ecg_recordings |>
              tidytable::filter(year == yr, month == mon) |>
              tidytable::pull(id)
          )
        })
      }
    ),
    tar_target(
      ecg_session_windows,
      bg_ecg_session_windows(
        ecg_sessions_meta,
        bg_root = box_bodyguard_root
      ),
    ),
    tar_target(
      mirage_windows,
      raw_mirage_events |>
        mirage_prepare_events() |>
        mirage_process_windows() |>
        # NOTE: From 2023-03-12 to 2023-06-11, a 5-minute "play" window
        # was recorded prior to the actual baseline event. These must be excluded
        # as they create duplicate sessions
        tidytable::filter(
          !(
            event == "play" &
              start >= "2023-03-12" &
              start <= "2023-06-11"
          )
        )
    ),
    tar_target(
      mirage_sessions,
      create_mirage_sessions(mirage_windows)
    ),
    tar_target(
      combined_esense_mirage,
      combine_esense_mirage(esense_meta, mirage_windows)
    ),
    tar_target(
      separated_linked_ecg_recordings,
      link_ecg_to_mirage(
        partition = bucket_partitions,
        recordings = ecg_recordings,
        sessions = ecg_session_windows,
        mirage = mirage_windows,
        hive_dir = bucket_bg_dir,
        synology_subroot = "irrrd-data-intermediates"
      ),
      pattern = map(bucket_partitions),
      cue = tar_cue(
        command = FALSE,
        depend = TRUE,
        iteration = TRUE
      ),
      iteration = "list",
      # To prevent massive OOMs that crash your computer, only run on the main
      # process. This blocks progress for the rest of the pipeline, of course,
      # but it allows multithreading for DuckDB.
      deployment = "main"
    ),
    tar_target(
      linked_ecg_recordings,
      tidytable::bind_rows(separated_linked_ecg_recordings)
    ),
    tar_target(
      ecg_recording_limits,
      create_bg_file_limits(
        partition = bucket_partitions,
        hive_dir = bucket_bg_dir
      ),
      pattern = map(bucket_partitions),
      iteration = "list"
    ),

    # 2. Loading ------
    tar_target(
      excel_workbooks,
      workbook_for_partition(
        partition = bucket_partitions,
        linked_ecg_recordings = linked_ecg_recordings,
        ecg_meta = ecg_recordings,
        ecg_limits = ecg_recording_limits,
        mirage_sessions = mirage_sessions,
        mirage_windows = mirage_windows
      ),
      pattern = map(bucket_partitions),
      iteration = "list"
    ),
    tar_target(
      write_workbooks,
      write_workbook(
        excel_workbooks,
        output_directory = box_path("ptl_irrrd_bio", "bg-excel")
      ),
      pattern = map(excel_workbooks)
    ),

    # 4. Reports ------
    tar_render(
      report_linkage,
      here::here("reports/linkage-inspection.Rmd"),
      output_file = here::here("outputs/linkage-inspection.html"),
    ),
  ) # end rlang::list2()

  main_targets <- append(main_targets, box_targets)
}

main_targets
