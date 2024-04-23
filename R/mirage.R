read_mirage_events <- function(path) {
  path |>
    data.table::fread() |>
    tidytable::select(
      id,
      mirage_pid = participantID,
      lang,
      event = marker,
      timecode_utc = utc_app,
    )
}
