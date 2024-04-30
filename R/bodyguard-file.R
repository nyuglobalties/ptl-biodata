bg_file_meta <- function(path) {
  stopifnot(is.character(path) && length(path) == 1)

  out <- data.table::data.table(
    path = path
  )

  bg_parse_file_header_(out)
  bg_parse_file_sequence_(out)

  err <- bg_scan_file_limits_(out)
  if (!is.data.frame(err)) {
    return(NULL)
  }

  bg_prepare_file_limits_(out)

  out
}

bg_parse_file_header_ <- function(dat) {
  stopifnot(data.table::is.data.table(dat))

  meta <- readLines(dat$path, n = 15) |>
    keep(\(x) grepl("^\\# ", x))

  # It would be nice if we could refer to the exact row,
  # but idk if that's a guarantee
  sampling_rate <- meta |>
    grep("^\\# Sampling", x = _, value = TRUE) |>
    gsub("^\\# Sampling\\:[ 0-9.s]+\\((\\d+) Hz\\)$", "\\1", x = _) |>
    as_int()

  locale <- meta |>
    grep("^\\# Export timezone\\:", x = _, value = TRUE) |>
    gsub("^\\# Export timezone\\: ([A-Za-z_/]+)$", "\\1", x = _)

  device_id <- meta |>
    grep("^\\# Device\\:", x = _, value = TRUE) |>
    gsub("^\\# Device\\: ", "", x = _)

  dat[, sampling_rate := sampling_rate]
  dat[, locale := locale]
  dat[, device_id := device_id]

  invisible(dat)
}

bg_parse_file_sequence_ <- function(dat) {
  dat[
    grepl("_(\\d+)-ecg\\.csv", path),
    sequence := as.integer(gsub(".*(\\d+)-ecg\\.csv$", "\\1", path))
  ]
  dat[is.na(sequence), sequence := 1L]

  cols <- setdiff(names(dat), c("path", "sequence"))
  data.table::setcolorder(dat, c("path", "sequence", cols))

  invisible(dat)
}

#' Find the maximum time limits of a Bodyguard file
#'
#' Since these CSVs can be quite large, this function uses
#' DuckDB to scan only the relevant parts of the files.
#' Using data.table::fread(select) is too slow for the number
#' of files needed to be processed.
#'
#' @param dat data.table - Working data.table that has a "path" column
bg_scan_file_limits_ <- function(dat) {
  conn <- duckdb::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  possible_time_cols <- c("timestamp", "local_time")
  table_header <- tryCatch(
    DBI::dbGetQuery(
      conn,
      glue::glue("SELECT * FROM '{dat$path}' LIMIT 1")
    ),
    error = function(e) {
      e
    }
  )

  if (rlang::is_error(table_header)) {
    return(NULL)
  }

  cols <- names(table_header)

  if (!any(possible_time_cols %in% cols)) {
    stop0(
      "Unknown time column. Columns:\n",
      paste0(paste0("  - '", cols, "'"), collapse = "\n")
    )
  }

  time_col <- possible_time_cols[possible_time_cols %in% cols]

  start <- DBI::dbGetQuery(
    conn,
    glue::glue("SELECT {time_col} FROM '{dat$path}' ORDER BY 1 ASC LIMIT 1")
  )[[1]]

  end <- DBI::dbGetQuery(
    conn,
    glue::glue("SELECT {time_col} FROM '{dat$path}' ORDER BY 1 DESC LIMIT 1")
  )[[1]]

  dat[, c("start", "end") := .(start, end)]
  invisible(dat)
}

bg_prepare_file_limits_ <- function(dat) {
  dat[, time_mode := "local_time"]

  if (!"POSIXt" %in% class(dat$start)) {
    dat[, c("start", "end", "time_mode") := .(
      as.POSIXct(start, origin = "1970-01-01"),
      as.POSIXct(end, origin = "1970-01-01"),
      "unix_timestamp"
    )]
  }

  invisible(dat)
}
