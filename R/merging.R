combine_esense_mirage <- function(esense, mirage) {
  conn <- duckdb::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(duckdb::dbDisconnect(conn))

  duckdb::duckdb_register(conn, "esense", esense)
  duckdb::duckdb_register(conn, "mirage", mirage)

  res <- DBI::dbGetQuery(conn, "
    WITH
    e AS (
      SELECT *, date_trunc('day', start_time) AS start_date
      FROM esense
    ),
    m AS (
      SELECT *, date_trunc('day', start) AS start_date
      FROM mirage
    ),
    cte AS (
      SELECT
        e.mirage_pid AS mirage_pid,
        e.start_time AS start_esense,
        e.end_time AS end_esense,
        m.start AS start_mirage,
        m.end AS end_mirage,
        e.start_date AS date,
        e.id AS row_esense,
        m.id AS row_mirage
      FROM e
      LEFT JOIN m ON (
        e.mirage_pid = m.mirage_pid AND
        e.start_date = m.start_date
      )
    ),
    cte2 AS (
      SELECT DISTINCT
        *,
        @(datediff('minute', start_mirage, start_esense)) AS diff_start,
        @(datediff('minute', end_mirage, end_esense)) AS diff_end
      FROM cte
    ),
    cte3 AS (
      SELECT
        *,
        diff_start <= 2 AND diff_end <= 2 AS window_2,
        diff_start <= 5 AND diff_end <= 5 AS window_5,
        diff_start <= 10 AND diff_end <= 10 AS window_10,
      FROM cte2
    )
    SELECT * EXCLUDE (window_2, window_5, window_10)
    FROM cte3
    WHERE (
      window_2 OR window_5 OR window_10
    )
    ORDER BY mirage_pid, date;
  ")

  res
}

link_ecg_to_mirage <- function(partition,
                               recordings,
                               sessions,
                               mirage,
                               hive_dir,
                               cache_root = here::here("_duckdb"),
                               synology_subroot = NULL) {
  if (length(partition) == 1) {
    partition <- partition[[1]]
  }

  stopifnot(is.numeric(partition$year) && is.numeric(partition$month))
  assert_string(synology_subroot)

  yr <- partition$year
  mon <- partition$month

  db_dir <- file.path(hive_dir, glue::glue("year={yr}"), glue::glue("month={mon}"))
  db_path <- synology_path(
    synology_subroot,
    "bodyguard",
    glue::glue("year={yr}"),
    glue::glue("month={mon}"),
    "db.duckdb"
  )

  conn <- bg_create_connection(dbdir = db_path)
  on.exit(DBI::dbDisconnect(conn))

  mirage_corrected <- mirage |>
    tidytable::mutate(
      tidytable::across(
        .cols = tidyselect::all_of(c("start", "end")),
        .fns = \(x) lubridate::with_tz(x, "Asia/Dhaka")
      )
    ) |>
    tidytable::mutate(
      year = lubridate::year(start),
      month = lubridate::month(start),
    )

  if (!is.null(yr)) {
    mirage_corrected <- mirage_corrected |>
      tidytable::filter(year == yr, month == mon)

    sessions <- sessions |>
      tidytable::filter(year == yr, month == mon)

    recordings <- recordings |>
      tidytable::filter(year == yr, month == mon)
  }

  duckdb::duckdb_register(conn, "mirage_raw", mirage_corrected)
  duckdb::duckdb_register(conn, "bg_sessions", sessions)
  duckdb::duckdb_register(conn, "bg_recordings", recordings)

  hive_parquet_glob <- file.path(db_dir, "*.parquet")

  DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TABLE bg AS
      FROM read_parquet('{hive_parquet_glob}', hive_partitioning=true);
    ")
  )

  DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TEMP VIEW mirage AS
      SELECT * EXCLUDE (\"start\", \"end\")
        , timezone('UTC', start) AS start
        , timezone('UTC', \"end\") AS \"end\"
      FROM mirage_raw;
    ")
  )

  DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TABLE bg_limits AS
      SELECT id_recording
        , min(t) AS t_min
        , max(t) AS t_max
      FROM bg GROUP BY ALL;
    ")
  )

  DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TEMP VIEW bg_mirage_link_raw AS
      SELECT b.id_recording AS id_recording
        , m.id AS id_mirage
        , m.mirage_pid AS id_participant
      FROM bg_limits b, mirage m
      WHERE m.start >= b.t_min
        AND m.end <= b.t_max;
    ")
  )

  DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TABLE links AS
      WITH cte AS (
        SELECT * EXCLUDE (id)
        FROM bg_mirage_link_raw
        LEFT JOIN (SELECT id, id_session FROM bg_recordings) bgr
          ON id_recording=bgr.id
      ), cte2 AS (
        FROM cte
        LEFT JOIN (
          SELECT id_session
            , id_win
            , participant
          FROM bg_sessions
        ) bgs
          ON bgs.id_session=cte.id_session
      )
      SELECT id_recording
        , id_mirage
        , id_session
        , id_win
        , id_participant
      FROM cte2
      WHERE id_participant=participant;
    ")
  )

  DBI::dbExecute(
    conn,
    glue::glue("
      CREATE OR REPLACE TEMP VIEW tmp AS
      WITH cte AS (
        SELECT id_recording
          , id_mirage
        FROM links
      )
      SELECT cte.id_recording AS id_recording
        , cte.id_mirage AS id_mirage
        , m.start AS start_mirage
        , m.end AS end_mirage
      FROM cte
      LEFT JOIN mirage m
      ON cte.id_mirage = m.id;
    ")
  )

  recs <- DBI::dbGetQuery(conn, "SELECT DISTINCT id_recording FROM tmp;")[[1]]

  DBI::dbExecute(
    conn,
    "CREATE OR REPLACE TABLE bg_mirage_times (
      id_recording VARCHAR,
      id_mirage INTEGER,
      \"time\" TIMESTAMPTZ,
      \"offset\" DOUBLE,
      attribute VARCHAR
    );"
  )

  for (rec in recs) {
    DBI::dbExecute(
      conn,
      glue::glue("
        INSERT INTO bg_mirage_times BY NAME (
          WITH bg_sub AS (
            SELECT id_recording
              , id
              , t
              , offset_secs
            FROM bg
            WHERE id_recording = '{rec}'
          )
          SELECT tmp.id_recording
            , tmp.id_mirage
            , bgs.t AS \"time\"
            , bgs.offset_secs AS \"offset\"
            , 'start' AS attribute
          FROM tmp
          ASOF JOIN bg_sub bgs
            ON tmp.id_recording = bgs.id_recording
            AND tmp.start_mirage >= bgs.t
          UNION BY NAME
          SELECT tmp.id_recording
            , tmp.id_mirage
            , bgs.t AS \"time\"
            , bgs.offset_secs AS offset
            , 'end' AS attribute
          FROM tmp
          ASOF JOIN bg_sub bgs
            ON tmp.id_recording = bgs.id_recording
            AND tmp.end_mirage <= bgs.t
          ORDER BY id_mirage, time ASC
        );
      ")
    )
  }

  res <- DBI::dbGetQuery(conn, "FROM bg_mirage_times;") |>
    tidytable::pivot_wider(
      id_cols = c("id_recording", "id_mirage"),
      names_from = "attribute",
      values_from = c("time", "offset")
    )

  res
}

create_bg_file_limits <- function(partition, hive_dir) {
  if (length(partition) == 1) {
    partition <- partition[[1]]
  }

  stopifnot(is.numeric(partition$year) && is.numeric(partition$month))

  year <- partition$year
  month <- partition$month

  db_dir <- file.path(hive_dir, glue::glue("year={year}"), glue::glue("month={month}"))
  hive_parquet_glob <- file.path(db_dir, "*.parquet")

  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(conn))

  DBI::dbGetQuery(
    conn,
    glue::glue("
      SELECT id_recording
        , min(t) AS t_min
        , max(t) AS t_max
      FROM read_parquet('{hive_parquet_glob}', hive_partitioning=true)
      GROUP BY ALL;
    ")
  )
}
