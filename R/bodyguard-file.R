#' Scan Bodyguard CSV into Hive-partitioned data lake
#'
#' This reads the CSVs, converts local times to UNIX Epoch milliseconds,
#' creates a column that calculates second offset from start of recording,
#' creates partitioning columns (year and month), and saves out as
#' as Hive-partitioned Parquet file.
#'
#' @param path string - Path to CSV file
#' @param root_dir string - The root directory of the Hive partition
#' @param cache_root string ($projroot/_duckdb) - Where crew workers save out
#'        intermediate data if needed
#' @return data.frame(asset_id: string, path: string, hive_path: string) -
#'         Record of where the ingested data is located
bg_scan_into_lake_cache <- function(path,
                                    root_dir,
                                    cache_root = here::here("_duckdb")) {
  assert_string(root_dir)
  assert_string(cache_root)

  cache_paths <- bg_check_paths(bg_get_worker_id(), cache_root = cache_root)
  asset_id <- bg_asset_id(path)

  # Does the file already exist in the bucket?
  known_data <- bg_find_data_for_path(path, root_dir)
  if (!is.null(known_data)) {
    return(known_data)
  }

  conn <- bg_create_connection(cache_paths)
  on.exit(DBI::dbDisconnect(conn))

  bg_setup_tables_(conn, cache_paths)

  check_obj <- bg_check_file(conn, path)
  if (is.null(check_obj)) {
    return(NULL)
  }

  meta <- bg_file_meta(path)
  time_col <- check_obj$time_col

  rows <- bg_load_into_ddb(
    conn = conn, path = path,
    meta = meta, time_col = time_col
  )

  if (rows < 1) {
    # Something wonky happened
    return(NULL)
  }

  bg_asset_meta_df(
    path = path,
    hive_path = bg_write_parquet(conn, asset_id, root_dir)
  )
}

bg_find_data_for_path <- function(path, root_dir) {
  asset_id <- bg_asset_id(path)

  found <- FALSE
  known_files <- data.table::data.table(
    file = list.files(root_dir, recursive = TRUE, pattern = "\\.parq(uet)?$")
  )

  known_files[, id := gsub("\\.parq(uet)?$", "", basename(file))]
  found <- nrow(known_files[id == asset_id]) > 0

  if (!found) {
    return(NULL)
  }

  bg_asset_meta_df(path, known_files[id == asset_id, file])
}

bg_asset_meta_df <- function(path, hive_path) {
  if (is.null(hive_path)) {
    return(NULL)
  }

  data.frame(
    asset_id = bg_asset_id(path),
    path = path,
    hive_path = hive_path
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

bg_create_connection <- function(cache_paths) {
  ddb_connect(
    dbdir = cache_paths$duckdb,
    read_only = FALSE,
    extensions = "icu"
  )
}

bg_setup_tables_ <- function(conn, cache_paths) {
  DBI::dbExecute(conn, "CREATE OR REPLACE SEQUENCE id_sequence START 1;")

  invisible(conn)
}

bg_read_local_time_csv <- function(conn, path, meta) {
  asset_id <- bg_asset_id(path)

  rows <- DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TABLE bodyguard AS
      WITH cte AS (
        SELECT *
          , nextval('id_sequence') AS id
          , '{meta$tz}' AS tz
          , '{asset_id}' AS asset_id
        FROM '{path}'
      ), cte1 AS (
        SELECT * EXCLUDE local_time
          , epoch_ms(timezone('{meta$tz}', local_time)) AS t
        FROM cte
      )
      SELECT asset_id
        , id, t
        , ecg, ecg_mV
      FROM cte1;
    ")
  )

  rows
}

bg_read_timestamped_csv <- function(conn, path) {
  asset_id <- bg_asset_id(path)

  rows <- DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TABLE bodyguard AS
      WITH cte AS (
        SELECT *
          , nextval('id_sequence') AS id
          , '{asset_id}' AS asset_id
        FROM '{path}'
      ), cte1 AS (
        SELECT *,
          date_part('year', to_timestamp(timestamp)) AS year
        FROM cte
      ), cte2 AS (
        SELECT * EXCLUDE (timestamp, year)
          , CASE WHEN year > 3000 THEN epoch_ms(epoch_ms(timestamp::BIGINT))
              ELSE epoch_ms(to_timestamp(timestamp))
            END AS t
        FROM cte1
      )
      SELECT asset_id
        , id, t
        , ecg, ecg_mV
      FROM cte2;
    ")
  )

  rows
}

bg_load_into_ddb <- function(conn, path, meta, time_col) {
  assert_string(path)
  assert_string(time_col)
  stopifnot(is.data.frame(meta))

  if (identical(time_col, "local_time")) {
    rows <- bg_read_local_time_csv(conn, path, meta)
  } else {
    rows <- bg_read_timestamped_csv(conn, path)
  }

  rows
}

bg_write_parquet <- function(conn, asset_id, root_dir) {
  DBI::dbExecute(
    conn,
    glue::glue("
      CREATE TEMP VIEW tmp AS
      SELECT *
        , (t - (first(t) OVER (
          PARTITION BY asset_id
          ORDER BY t ASC
        ))) / 1000 AS offset_secs
      FROM bodyguard;
    ")
  )

  res <- DBI::dbGetQuery(
    conn,
    "
    SELECT date_part('year', ts) AS year
      , date_part('month', ts) AS month
    FROM (SELECT to_timestamp(t / 1000) AS ts FROM tmp)
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
    glue::glue("{asset_id}.parquet")
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

bg_file_meta <- function(path) {
  stopifnot(is.character(path) && length(path) == 1)

  out <- data.table::data.table(
    path = path
  )

  bg_parse_file_header_(out)
  bg_parse_file_sequence_(out)

  out
}

bg_asset_id <- function(path) {
  digest::digest(path, algo = "xxhash64")
}

bg_cache_file_from_id <- function(cache_id, cache_root) {
  stopifnot(is.character(cache_root) && length(cache_root) == 1)

  cache_dirname <- substr(cache_id, 1, 2)
  cache_basename <- paste0(
    substr(cache_id, 3, nchar(cache_id)),
    ".duckdb"
  )

  file.path(cache_root, cache_dirname, cache_basename)
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

  dat[, sampling_rate := sampling_rate]
  dat[, tz := tz]
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
  on.exit(duckdb::dbDisconnect(conn))

  check_obj <- bg_check_file(conn, dat$path)
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

bg_check_file <- function(conn, path) {
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

  cols <- names(table_header)

  if (!any(possible_time_cols %in% cols)) {
    stop0(
      "Unknown time column. Columns:\n",
      paste0(paste0("  - '", cols, "'"), collapse = "\n")
    )
  }

  time_col <- possible_time_cols[possible_time_cols %in% cols]

  list(
    table_header = table_header,
    time_col = time_col
  )
}
