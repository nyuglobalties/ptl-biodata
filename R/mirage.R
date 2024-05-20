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
