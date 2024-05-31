read_mirage_events <- function(path) {
  path |>
    data.table::fread(integer64 = "character") |>
    tidytable::select(
      id,
      mirage_pid = participantID,
      lang,
      event = marker,
      timecode_utc = utc_app,
    )
}

mirage_prepare_events <- function(raw_events) {
  raw_events |>
    tidytable::bind_rows() |>
    unique() |>
    tidytable::filter(grepl("\\d{5,6}$", mirage_pid)) |>
    tidytable::mutate(mirage_pid = gsub("PID", "", mirage_pid)) |>
    tidytable::arrange(timecode_utc)
}

mirage_process_windows <- function(events) {
  events |>
    tidytable::group_by(mirage_pid) |>
    tidytable::nest() |>
    tidytable::mutate(
      data = lapply(data, mirage_process_participant_windows)
    ) |>
    tidytable::unnest() |>
    tidytable::ungroup() |>
    tidytable::mutate(
      start = lubridate::force_tz(lubridate::as_datetime(as.numeric(start) / 1000), "UTC"),
      end = lubridate::force_tz(lubridate::as_datetime(as.numeric(end) / 1000), "UTC"),
      length = difftime(end, start, units = "secs"),
    ) |>
    # Only include windows that *could* include baseline events (>30s)
    tidytable::filter(as.numeric(length) >= 30) |>
    unique() |>
    tidytable::arrange(mirage_pid, start) |>
    tidytable::mutate(id = seq_len(.N))
}

mirage_process_participant_windows <- function(part_events) {
  part_events <- part_events |>
    tidytable::filter(grepl("_(start|stop)$", event))

  event_stacks <- map(val_type = "kv_stack")
  windows <- vector("list", nrow(part_events))
  idx <- 1

  for (i in seq_len(nrow(part_events))) {
    ev <- part_events[["event"]][i]
    ev_time <- part_events[["timecode_utc"]][i]
    ev_root <- gsub("_(start|stop)$", "", ev)
    is_stop <- grepl("stop$", ev)

    handle_start <- function(stacks, key, val) {
      if (!has(stacks, key)) {
        st <- kv_stack(type = vector(typeof(val), 0))
        push(st, key, val)
        put(stacks, key, st)
      } else {
        push(getval(stacks, key), key, val)
      }

      invisible(stacks)
    }

    handle_stop <- function(stacks, key, val) {
      if (!has(stacks, key) || is_empty(getval(stacks, key))) {
        stop0("Stop marker given for event that hasn't started: ", key)
      }

      # Note: `getval` returns a pointer because the elements
      # are kv_stack objects. See type-map.R and type-stack.R
      # for implementation details.
      el <- pop(getval(stacks, key))

      data.frame(
        event = el$key,
        start = el$val,
        end = val
      )
    }

    if (isTRUE(is_stop)) {
      window <- handle_stop(event_stacks, ev_root, ev_time)
      windows[[idx]] <- window
      idx <- idx + 1
    } else {
      handle_start(event_stacks, ev_root, ev_time)
    }
  }

  windows_no_null <- discard(windows, is.null)

  if (length(windows_no_null) < 1) {
    return(NULL)
  }

  tidytable::bind_rows(windows_no_null)
}

create_mirage_sessions <- function(mirage_windows) {
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(conn))

  duckdb::duckdb_register(conn, "mirage", mirage_windows)

  DBI::dbExecute(conn, "CREATE OR REPLACE TEMP SEQUENCE idx START 1;")
  DBI::dbGetQuery(conn, "
    WITH m AS (
      SELECT *
        , date_trunc('day', start) AS date
      FROM mirage
    ), raw_pairs AS (
      SELECT a.id AS id_a
      , b.id AS id_b
      FROM (SELECT id FROM m) a
      , (SELECT id FROM m) b
    ), adorned_pairs AS (
      SELECT id_a, id_b
      , a.start AS start_a
      , a.date AS date_a
      , a.mirage_pid AS mpid_a
      , b.start AS start_b
      , b.date AS date_b
      , b.mirage_pid AS mpid_b
      FROM raw_pairs
      LEFT JOIN (SELECT id, start, date, mirage_pid FROM m) a
        ON a.id = raw_pairs.id_a
      LEFT JOIN (SELECT id, start, date, mirage_pid FROM m) b
        ON b.id = raw_pairs.id_b
    ), raw_diffs AS (
      SELECT mpid_a AS mirage_pid
        , date_a AS date
        , id_a
        , id_b
        , start_a
        , start_b
        , date_diff('ms', start_a, start_b) / 1000 AS dt
      FROM adorned_pairs
      WHERE date_a = date_b AND mpid_a = mpid_b
    ), cleaned_diffs AS (
      SELECT * REPLACE (
        CASE WHEN id_a = id_b AND start_a = start_b THEN NULL ELSE dt END AS dt
      ) 
      FROM raw_diffs
    ), cte AS (
      SELECT *
        , mad(dt) OVER (
          PARTITION BY mirage_pid, date, id_a
        ) AS mad_dt
      FROM cleaned_diffs c
      LEFT JOIN (
        SELECT mirage_pid, date, nextval('idx') AS id_session
        FROM cleaned_diffs
        GROUP BY 1, 2
      ) s
        USING (mirage_pid, date)
      ORDER BY mirage_pid, date, id_a
    ), cte2 AS (
      SELECT *
      FROM (
        SELECT mirage_pid
          , date
          , COLUMNS('^id_')
          , COLUMNS('^start_')
          , dt
          , mad_dt
          , min(mad_dt) OVER (PARTITION BY id_session) AS min_mad_dt
        FROM cte
      ) 
      WHERE mad_dt = min_mad_dt OR min_mad_dt IS NULL
    ), cte3 AS (
      SELECT *
        , min(id_a) OVER (PARTITION BY id_session) AS id_event_root
      FROM cte2
    )
    SELECT id_session
      , mirage_pid
      , date
      , id_b AS id_event
      , ifnull(abs(dt) < (60 * 60), true) AS valid_event
    FROM cte3
    WHERE id_a = id_event_root;
  ")
}
