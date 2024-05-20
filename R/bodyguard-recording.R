#' Read recording metadata
#'
#' @param path string - Path to CSV file
#' @return ?data.frame(
#'   id: string,
#'   path: string,
#'   sequence: integer(1),
#'   device_id: string,
#'   time_col: string,
#'   tz: string,
#'   sampling_rate: double(1),
#' )
bg_ecg_recording_meta <- function(path) {
  stopifnot(is.character(path) && length(path) == 1)

  out <- data.table::data.table(
    id = bg_recording_id(path),
    id_session = bg_dir_id(path),
    path = path
  )

  bg_parse_file_header_(out)
  bg_parse_file_sequence_(out)

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(conn))

  time_col <- bg_check_get_timecol(conn, path)
  if (is.null(time_col)) {
    # Invalid CSV format
    return(NULL)
  }

  out |>
    tidytable::mutate(time_col = time_col) |>
    tidytable::select(
      id, id_session, path, sequence,
      device_id, time_col, tz,
      sampling_rate
    )
}

#' Scan Bodyguard CSV into Hive-partitioned data lake
#'
#' This reads the CSVs, converts local times to UNIX Epoch milliseconds,
#' creates a column that calculates second offset from start of recording,
#' creates partitioning columns (year and month), and saves out as
#' as Hive-partitioned Parquet file.
#'
#' @param meta ?data.frame - Output of [bg_recording_meta()]
#' @param root_dir string - The root directory of the Hive partition
#' @param cache_root string ($projroot/_duckdb) - Where crew workers save out
#'        intermediate data if needed
#' @return data.frame(asset_id: string, path: string, hive_path: string) -
#'         Record of where the ingested data is located
bg_import_ecg_to_lake <- function(meta,
                                  root_dir,
                                  cache_root = here::here("_duckdb")) {
  if (is.null(meta)) {
    return(NULL)
  }

  assert_string(root_dir)
  assert_string(cache_root)

  cache_paths <- bg_check_paths(bg_get_worker_id(), cache_root = cache_root)

  # Does the file already exist in the bucket?
  known_data <- bg_find_data_for_path(meta$path, root_dir)
  if (!is.null(known_data)) {
    return(known_data)
  }

  conn <- bg_create_connection(cache_paths)
  on.exit(DBI::dbDisconnect(conn))

  bg_setup_tables_(conn, cache_paths)
  rows <- bg_load_into_ddb(conn = conn, meta = meta)

  if (rows < 1) {
    # Something wonky happened
    return(NULL)
  }

  bg_recording_meta_df(
    path = meta$path,
    hive_path = bg_write_parquet(conn, meta$id, root_dir)
  )
}

bg_find_data_for_path <- function(path, root_dir) {
  recording_id <- bg_recording_id(path)

  found <- FALSE
  known_files <- data.table::data.table(
    file = list.files(root_dir, recursive = TRUE, pattern = "\\.parq(uet)?$")
  )

  known_files[, id := gsub("\\.parq(uet)?$", "", basename(file))]
  found <- nrow(known_files[id == recording_id]) > 0

  if (!found) {
    return(NULL)
  }

  bg_recording_meta_df(path, known_files[id == recording_id, file])
}

bg_recording_meta_df <- function(path, hive_path) {
  if (is.null(hive_path)) {
    return(NULL)
  }

  data.frame(
    id = bg_recording_id(path),
    path = path,
    hive_path = hive_path,
    year = as.integer(gsub(".*year=(\\d+).*", "\\1", hive_path)),
    month = as.integer(gsub(".*month=(\\d+).*", "\\1", hive_path))
  )
}

bg_get_worker_id <- function() {
  if (interactive()) {
    return(0L)
  }

  worker_id <- Sys.getenv("CREW_WORKER", unset = "-1")
  if (worker_id == "-1") {
    stop0("Unsupported operation: can only run on workers to avoid deadlocks")
  }

  as.integer(worker_id)
}

bg_check_paths <- function(worker_id, cache_root = here::here("_duckdb")) {
  worker_id <- paste0("worker", worker_id)

  ddb_file <- file.path(cache_root, paste0(worker_id, ".duckdb"))

  if (!dir.exists(dirname(ddb_file))) {
    dir.create(dirname(ddb_file), recursive = TRUE)
  }

  list(
    duckdb = ddb_file
  )
}

bg_create_connection <- function(cache_paths = NULL, dbdir = NULL) {
  stopifnot(!is.null(cache_paths) || !is.null(dbdir))

  path <- cache_paths$duckdb %||% dbdir

  # Check on custom dbdir
  if (!dir.exists(dirname(path))) {
    dir.create(dirname(path), recursive = TRUE)
  }

  ddb_connect(
    dbdir = path,
    read_only = FALSE,
    extensions = "icu"
  )
}

bg_setup_tables_ <- function(conn, cache_paths) {
  DBI::dbExecute(conn, "CREATE OR REPLACE SEQUENCE id_sequence START 1;")

  invisible(conn)
}

bg_read_local_time_csv <- function(conn, meta) {
  id <- meta$id

  # Use a view to push down the operators directly to the CSV
  # and only use the worker file for spillover *if needed*
  rows <- DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TABLE bodyguard AS
      WITH cte AS (
        SELECT *
          , nextval('id_sequence') AS id
          , '{meta$tz}' AS tz
          , '{id}' AS id_recording
        FROM '{meta$path}'
      ), cte1 AS (
        SELECT * EXCLUDE local_time
          , timezone('UTC', timezone('{meta$tz}', local_time)) AS t
        FROM cte
      )
      SELECT id_recording
        , id, t
        , ecg, ecg_mV
      FROM cte1;
    ")
  )

  rows
}

bg_read_timestamped_csv <- function(conn, meta) {
  id <- meta$id

  rows <- DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TABLE bodyguard AS
      WITH cte AS (
        SELECT *
          , nextval('id_sequence') AS id
          , '{id}' AS id_recording
        FROM '{meta$path}'
      ), cte1 AS (
        SELECT *,
          date_part('year', to_timestamp(timestamp)) AS year
        FROM cte
      ), cte2 AS (
        SELECT * EXCLUDE (timestamp, year)
          , CASE WHEN year > 3000 THEN epoch_ms(timestamp::BIGINT)
              ELSE to_timestamp(timestamp)
            END AS t
        FROM cte1
      )
      SELECT id_recording
        , id, t
        , ecg, ecg_mV
      FROM cte2;
    ")
  )

  rows
}

bg_load_into_ddb <- function(conn, meta) {
  assert_string(meta$path)
  assert_string(meta$time_col)
  stopifnot(is.data.frame(meta))

  if (identical(meta$time_col, "local_time")) {
    rows <- bg_read_local_time_csv(conn, meta)
  } else {
    rows <- bg_read_timestamped_csv(conn, meta)
  }

  rows
}

bg_write_parquet <- function(conn, id, root_dir) {
  DBI::dbExecute(
    conn,
    glue::glue("
      CREATE TEMP VIEW tmp AS
      SELECT *
        , datesub('ms', (first(t) OVER (
          PARTITION BY id_recording
          ORDER BY t ASC
        )), t) / 1000 AS offset_secs
      FROM bodyguard;
    ")
  )

  res <- DBI::dbGetQuery(
    conn,
    "
    SELECT date_part('year', t) AS year
      , date_part('month', t) AS month
    FROM tmp
    LIMIT 1;
    "
  )

  year <- res$year
  month <- res$month

  # Timestamp sanity check -- 1970 and 1969 weirdness shows up
  if (year < 2000) {
    return(NULL)
  }

  hive_path <- file.path(
    root_dir,
    glue::glue("year={year}"),
    glue::glue("month={month}"),
    glue::glue("{id}.parquet")
  )

  if (!dir.exists(dirname(hive_path))) {
    dir.create(dirname(hive_path), recursive = TRUE)
  }

  DBI::dbExecute(
    conn,
    glue::glue("
      COPY (FROM tmp) TO '{hive_path}' (FORMAT 'parquet');
    ")
  )

  hive_path
}

bg_recording_id <- function(path) {
  digest::digest(path, algo = "xxhash64")
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

  tz <- meta |>
    grep("^\\# Export timezone\\:", x = _, value = TRUE) |>
    gsub("^\\# Export timezone\\: ([A-Za-z_/]+)$", "\\1", x = _)

  device_id <- meta |>
    grep("^\\# Device\\: BG", x = _, value = TRUE) |>
    gsub("^\\# Device\\: ", "", x = _) |>
    gsub(".*(*BG\\d{8}).*", "\\1", x = _)

  dat[, device_id := device_id]
  dat[, sampling_rate := sampling_rate]
  dat[, tz := tz]

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
  on.exit(duckdb::dbDisconnect(conn))

  check_obj <- bg_check_get_timecol(conn, dat$path)
  if (is.null(check_obj)) {
    return(NULL)
  }

  time_col <- check_obj$time_col

  # Retain table reference in memory so rescan isn't needed
  # The column should be cached.
  DBI::dbExecute(conn, "DROP TABLE IF EXISTS tmp;")
  DBI::dbExecute(
    conn,
    glue::glue("CREATE TABLE tmp AS FROM '{dat$path}';")
  )

  start <- DBI::dbGetQuery(
    conn,
    glue::glue("SELECT {time_col} FROM tmp ORDER BY 1 ASC LIMIT 1;")
  )[[1]]

  end <- DBI::dbGetQuery(
    conn,
    glue::glue("SELECT {time_col} FROM tmp ORDER BY 1 DESC LIMIT 1;")
  )[[1]]

  dat[, c("start", "end") := list(start, end)]
  invisible(dat)
}

#' Make sure the CSV has the expected format
bg_check_get_timecol <- function(conn, path) {
  possible_time_cols <- c("timestamp", "local_time")
  table_header <- tryCatch(
    DBI::dbGetQuery(
      conn,
      glue::glue("FROM '{path}' LIMIT 1;")
    ),
    error = function(e) {
      e
    }
  )

  if (rlang::is_error(table_header)) {
    return(NULL)
  }

  if (nrow(table_header) < 1) {
    return(NULL)
  }

  cols <- names(table_header)

  if (!any(possible_time_cols %in% cols)) {
    stop0(
      "Unknown time column. Columns:\n",
      paste0(paste0("  - '", cols, "'"), collapse = "\n")
    )
  }

  possible_time_cols[possible_time_cols %in% cols]
}
