esense_file_meta <- function(path, esense_root) {
  lines <- readLines(path, n = 10L)

  device <- stringi::stri_split_fixed(lines[1], ";")[[1]][1]
  device <- stringi::stri_trim_left(stringi::stri_split_fixed(device, ":")[[1]][2])

  if (any(is.na(device))) {
    # Malformatted file -- skip
    return(NULL)
  }

  start_time <- stringi::stri_split_fixed(lines[4], ";")[[1]][2]
  start_time_bd <- stringi::stri_datetime_parse(
    start_time,
    stringi::stri_datetime_fstr("%d.%m.%y %T"),
    tz = "Asia/Dhaka"
  )
  start_time_al <- stringi::stri_datetime_parse(
    start_time,
    stringi::stri_datetime_fstr("%d.%m.%y %T"),
    tz = "Asia/Almaty" # This TZ was encountered in the Bodyguard data
  )
  start_time_ist <- stringi::stri_datetime_parse(
    start_time,
    stringi::stri_datetime_fstr("%d.%m.%y %T"),
    tz = "Asia/Calcutta"
  )

  esense_root_abs <- box_path(esense_root)
  explode_rel_path <- \(x) explode_path(gsub(esense_root_abs, "", x, fixed = TRUE))
  rescat <- (explode_rel_path(path)[1]) |>
    gsub("^([0-9_]+)\\..*", "\\1", x = _) |>
    gsub("_", "", x = _) |>
    as.integer()

  data.table::data.table(
    path = path,
    mirage_pid = gsub("\\.csv$", "", basename(path)),
    device = device,
    start_time_dhaka = start_time_bd,
    start_time_ist = start_time_ist,
    start_time_almaty = start_time_al
  ) |>
    tidytable::mutate(
      respondent_cat = rescat,
      .after = mirage_pid,
    )
}

esense_scan_length <- function(path) {
  buffer <- readLines(path, n = 28)
  buffer_view <- substr(buffer[28], 1, 6)

  if (!identical(buffer_view, "SECOND")) {
    # Malformatted?
    return(paste0(buffer, collapse = "\n"))
  }

  # These aren't giant files, so fread is fine
  secs <- data.table::fread(path, skip = 27, sep = ";", select = "SECOND")
  secs <- secs[["SECOND"]]

  if (!is.numeric(secs)) {
    secs <- chartr(",", ".", secs)
    secs <- as.numeric(secs)
  }

  data.table::data.table(
    path = path,
    length_secs = max(secs)
  )
}
