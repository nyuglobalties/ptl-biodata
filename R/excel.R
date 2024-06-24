workbook_for_partition <- function(partition,
                                   rescat,
                                   linked_ecg_recordings,
                                   ecg_meta = NULL,
                                   ecg_limits = NULL,
                                   ecg_sessions = NULL,
                                   mirage_sessions = NULL,
                                   mirage_windows = NULL,
                                   esense = NULL,
                                   box_root = box_path()) {
  if (length(partition) == 1) {
    partition <- partition[[1]]
  }

  ecg_meta_rescat <- ecg_meta |>
    tidytable::left_join(
      ecg_sessions |>
        tidytable::select(id_session = id, respondent_cat),
      by = "id_session"
    ) |>
    tidytable::filter(
      respondent_cat == rescat,
      year == partition$year,
      month == partition$month
    )

  if (nrow(ecg_meta_rescat) < 1) {
    return(NULL)
  }

  m_sessions <- mirage_sessions |>
    tidytable::mutate(
      year = lubridate::year(date),
      month = lubridate::month(date),
    ) |>
    tidytable::filter(year == partition$year, month == partition$month) |>
    tidytable::left_join(
      mirage_windows |>
        tidytable::select(
          id_event = id,
          start_event = start,
          end_event = end
        ),
      by = "id_event"
    ) |>
    tidytable::select(
      id_session,
      mirage_pid,
      date,
      id_event,
      start_event,
      end_event,
      valid_event,
    ) |>
    tidytable::arrange(date, mirage_pid)

  linked_bg <- linked_ecg_recordings |>
    tidytable::mutate(
      year = lubridate::year(time_start),
      month = lubridate::month(time_start),
    ) |>
    tidytable::filter(
      year == partition$year,
      month == partition$month
    ) |>
    tidytable::left_join(
      ecg_meta |>
        tidytable::select(
          id_recording = id,
          path,
          id_device = device_id,
        ),
      by = "id_recording"
    ) |>
    tidytable::left_join(
      m_sessions |>
        tidytable::select(
          id_mirage = id_event,
          id_session,
          mirage_pid,
        ),
      by = "id_mirage"
    ) |>
    tidytable::mutate(path = gsub(box_root, "", path, fixed = TRUE)) |>
    tidytable::mutate(
      n_mirage_match = .N,
      .by = id_mirage
    ) |>
    tidytable::select(
      id_session,
      id_recording,
      id_mirage,
      id_device,
      mirage_pid,
      n_mirage_match,
      offset_start,
      time_start,
      offset_end,
      time_end,
      box_path = path,
    )

  linked_bg_rescat <- linked_bg |>
    tidytable::inner_join(
      ecg_meta_rescat |>
        tidytable::select(id_recording = id),
      by = "id_recording"
    )

  unlinked_bg <- ecg_meta_rescat |>
    tidytable::rename(id_recording = id) |>
    tidytable::filter(
      !is.na(year),
      !is.na(month),
      year == partition$year,
      month == partition$month
    ) |>
    tidytable::anti_join(
      linked_bg |>
        tidytable::select(id_recording),
      by = "id_recording"
    ) |>
    tidytable::mutate(path = gsub(box_root, "", path, fixed = TRUE)) |>
    tidytable::select(
      id_recording,
      id_device = device_id,
      box_path = path,
    ) |>
    tidytable::left_join(
      ecg_limits |>
        tidytable::bind_rows() |>
        tidytable::select(
          id_recording,
          time_start = t_min,
          time_end = t_max
        ),
      by = "id_recording"
    )

  list(
    "BG Uniquely Linked to Mirage" = linked_bg_rescat |>
      tidytable::filter(n_mirage_match == 1),
    "BG Multiply Linked to Mirage" = linked_bg_rescat |>
      tidytable::filter(n_mirage_match > 1) |>
      tidytable::arrange(id_mirage, offset_start),
    "BG Not Linked to Mirage" = unlinked_bg,
    "Unlinked Mirage (all rescats)" = m_sessions[!id_event %in% linked_bg$id_mirage],
    "Mirage" = m_sessions,
    meta = tidytable::tidytable(
      year = partition$year,
      month = partition$month,
      respondent_cat = rescat,
      pct_mirage_linked = if (nrow(m_sessions) > 0) {
        round(
          100 * (1 - (nrow(m_sessions[!id_event %in% linked_bg$id_mirage]) / nrow(m_sessions))),
          digits = 2
        )
      } else {
        NA_real_
      }
    )
  )
}

write_workbook <- function(workbook,
                           output_directory = NULL,
                           write_file = TRUE,
                           verbose = TRUE) {
  if (is.null(workbook)) {
    return(NULL)
  }

  if (length(workbook) == 1) {
    workbook <- workbook[[1]]
  }

  if (is.null(output_directory)) {
    stop0("Please specify where to save the Excel workbooks")
  }

  rescat <- workbook$meta$respondent_cat
  year <- workbook$meta$year
  month <- workbook$meta$month
  month <- stringi::stri_pad(str = month, pad = "0", width = 2)

  rescat_map <- as.character(seq_len(12))
  rescat_map[1] <- "1. Recruitment"
  rescat_map[4] <- "4. 3rd Trimester followup"
  rescat_map[6] <- "6. Birth follow up"
  rescat_map[7] <- "7. 6-month follow up"
  rescat_map[10] <- "10. 16-month follow up"

  path <- file.path(
    output_directory,
    rescat_map[rescat],
    glue::glue("bg_{year}_{month}.xlsx")
  )

  if (!dir.exists(dirname(path))) {
    if (verbose) message("Creating directory '", dirname(path), "'")

    dir.create(dirname(path), recursive = TRUE)
  }

  if (isTRUE(write_file)) {
    if (isTRUE(verbose)) message("Writing '", path, "'")
    openxlsx::write.xlsx(workbook, file = path)
  }

  path
}
