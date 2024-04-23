esense_file_meta <- function(path, esense_root) {
  lines <- readLines(path, n = 29L)

  device <- stringi::stri_split_fixed(lines[1], ";")[[1]][1]
  device <- stringi::stri_trim_left(stringi::stri_split_fixed(device, ":")[[1]][2])

  if (any(is.na(device))) {
    # Malformatted file -- skip but show what the content was
    return(lines)
  }

  esense_root_abs <- box_path(esense_root)
  explode_rel_path <- \(x) explode_path(gsub(esense_root_abs, "", x, fixed = TRUE))
  rescat <- (explode_rel_path(path)[1]) |>
    gsub("^([0-9_]+)\\..*", "\\1", x = _) |>
    gsub("_", "", x = _) |>
    as.integer()

  out <- data.table::data.table(
    path = path,
    device = device,
    mirage_pid = gsub("\\.csv$", "", basename(path)),
    rescat = rescat
  )

  esense_add_times_(out, lines)

  # lol look it's Go!
  ok <- esense_scan_length_(out, lines)
  if (is.null(ok)) {
    return(lines)
  }

  out
}

esense_add_times_ <- function(dat, lines) {
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

  dat[, start_time_bd := start_time_bd]
  dat[, start_time_al := start_time_al]
  dat[, start_time_ist := start_time_ist]

  invisible(dat)
}

esense_scan_length_ <- function(dat, lines) {
  # From observation, sometimes the data starts at 28 and other times 29
  idxes <- c(28, 29)
  idx <- 0
  ok <- FALSE

  while (!isTRUE(ok) && length(idxes) > 0) {
    idx <- idxes[1]

    line_slice <- substr(lines[idx], 1, 6)
    ok <- identical(line_slice, "SECOND")

    idxes <- idxes[-1]
  }

  if (!ok) {
    return(NULL)
  }

  # I love embedding SQL in R ... thank you DuckDB
  conn <- duckdb::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  res <- DBI::dbGetQuery(
    conn,
    glue::glue("
      WITH cte AS (
        SELECT CAST(regexp_replace(\"SECOND\", ',', '.') AS FLOAT) AS seconds
        FROM read_csv( '{dat$path}', delim = ';', 
          all_varchar = true,
          skip = {idx - 1}
        )
      )
      SELECT seconds FROM cte ORDER BY seconds DESC LIMIT 1;
    ")
  )[[1]]

  dat[, length_secs := res]
  invisible(dat)
}
