ecg_meta_dt <- function(paths, bg_root) {
  bg_root_abs <- box_path(bg_root)
  explode_rel_path <- \(x) explode_path(gsub(bg_root_abs, "", x, fixed = TRUE))

  out <- data.table::data.table(
    path = paths
  ) |>
    tidytable::filter(grepl("ecg\\.csv$", path)) |>
    tidytable::mutate(
      size = vdapply(path, \(x) file.info(x)$size),
      exploded_rel_path = lapply(path, explode_rel_path),
      respondent_cat = viapply(exploded_rel_path, \(ex) {
        ex[1] |>
          gsub("^([0-9_]+)\\..*", "\\1", x = _) |>
          gsub("_", "", x = _) |>
          as.integer()
      }),
      csv_meta = lapply(path, parse_ecg_file_meta),
      sampling_rate = viapply(csv_meta, \(x) x$sampling_rate),
      locale = vcapply(csv_meta, \(x) x$locale),
      device_id = vcapply(csv_meta, \(x) x$device_id),
    ) |>
    tidytable::mutate(
      size_ok = size > 1e3 & size < 3e7,
      .after = size
    ) |>
    bg_parse_date_and_participants() |>
    bg_parse_file_sequence()

  out
}

parse_ecg_file_meta <- function(f) {
  stopifnot(is.character(f))

  meta <- readLines(f, n = 15) |>
    purrr::keep(\(x) grepl("^\\# ", x))

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

  list(
    sampling_rate = sampling_rate,
    locale = locale,
    device_id = device_id
  )
}

bg_parse_file_sequence <- function(dat) {
  dat |>
    tidytable::mutate(
      sequence = tidytable::if_else(
        grepl("_(\\d+)-ecg\\.csv$", path),
        as.integer(gsub(".*(\\d+)-ecg\\.csv$", "\\1", path)),
        1L
      ),
      .after = path,
    )
}

bg_parse_date_and_participants <- function(dat) {
  dat |>
    tidytable::mutate(
      exp_path_2 = vcapply(exploded_rel_path, \(x) x[2]),
      exp_path_3 = vcapply(exploded_rel_path, \(x) x[3]),
      date = tidytable::case_when(
        grepl("^\\d{8}$", exp_path_2) ~ lubridate::ymd(exp_path_2),
        grepl("^\\d{8}$", exp_path_3) ~ lubridate::ymd(exp_path_3),
        TRUE ~ as.Date(NA_real_)
      ),
      # There can be multiple participants per file group allegedly
      # so we need to capture all (mother, child, plus optional twin)
      mirage_pid_1 = tidytable::case_when(
        grepl("^\\d{8}$", exp_path_2) & grepl("^\\d{5,6}$", exp_path_3) ~ exp_path_3,
        TRUE ~ NA_character_,
      ),
      participant_type_1 = tidytable::case_when(
        exp_path_2 %in% c("Birth_follow_up_child", "Child") ~ "child",
        exp_path_2 %in% c("Birth_follow_up_mother", "Mother") ~ "mother",
        TRUE ~ NA_character_
      ),
      .after = path,
    ) |>
    bg_prep_window_names()
}

bg_prep_window_names <- function(dat) {
  final_pattern_1 <- "^\\d{5,6}[CMB]_\\d{1,2};\\d{2}[AP]M\\-\\d{1,2};\\d{2}[AP]M"
  final_pattern_2 <- "^\\d{5,6}[CMB]"

  dat[, .processed := FALSE]

  # Using in-place operations because of grouped aggregations
  dat[, .zone := grepl("[Mm]\\s+,?\\d{5,6}", exp_path_3)]

  # ZONE 1 ------
  dat[.zone == TRUE, exp_path_3 := toupper(exp_path_3)]

  # Assuming no marker is the mother -- doesn't really
  # matter, but it helps with the overall processing code
  dat[
    .zone == TRUE & grepl("\\d{5,6}_", exp_path_3),
    exp_path_3 := gsub("(\\d{5,6})_", "\\1M_", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("[CM] _", exp_path_3),
    exp_path_3 := gsub("([CM]) _", "\\1_", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("MC_", exp_path_3),
    exp_path_3 := gsub("MC", "B", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("[CM]_ ", exp_path_3),
    exp_path_3 := gsub("([CM])_ ", "\\1_", exp_path_3)
  ]

  dat[
    .zone == TRUE,
    exp_path_3 := gsub("([M])\\s+,?(\\d{5,6})", "\\1\t\\2", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("\\t\\d{5}M.*[ (]CHILD[)]?$", exp_path_3),
    exp_path_3 := gsub("\\t(\\d{5})M(.*)[ (]CHILD[)]?$", "\t\\1C\\2", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("\\s+MOTHER\\s*$", exp_path_3),
    exp_path_3 := gsub("\\s+MOTHER\\s*$", "", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("[CM]\\d{1,2}\\'", exp_path_3),
    exp_path_3 := gsub("([CM])(\\d{1,2})\\'", "\\1_\\2;", exp_path_3)
  ]

  # Handle missing meridian markers
  dat[
    .zone == TRUE & grepl("\\d{1,2}\\;\\d{2}\\-\\d{1,2}\\;\\d{2}[AP]M", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2}\\;\\d{2})\\-(\\d{1,2}\\;\\d{2})([AP]M)",
      "\\1\\3-\\2\\3",
      exp_path_3
    )
  ]

  dat[
    .zone == TRUE & grepl("\\d{1,2}\\;\\d{2}[AP]M\\-\\d{1,2}\\;\\d{2}", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2}\\;\\d{2})([AP]M)\\-(\\d{1,2}\\;\\d{2})",
      "\\1\\2-\\3\\2",
      exp_path_3
    )
  ]

  dat[
    .zone == TRUE & grepl("PMPM?", exp_path_3),
    exp_path_3 := gsub("(PMPM?)", "PM", exp_path_3)
  ]
  dat[
    .zone == TRUE & grepl("AMAM?", exp_path_3),
    exp_path_3 := gsub("(AMAM?)", "AM", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("\\d{1,2};\\d{2}[AP]M;\\d{1,2};\\d{2}[AP]M", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2};\\d{2}[AP]M);(\\d{1,2};\\d{2}[AP]M)",
      "\\1-\\2",
      exp_path_3
    )
  ]

  dat[
    .zone == TRUE & grepl("\\d{1,2};\\d{2}[AP]M\\-?\\d{1,2}(?:[.;]|-)?\\d{2}[AP]M", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2};\\d{2}[AP]M)\\-?(\\d{1,2})(?:[.;]|-)?(\\d{2}[AP]M)",
      "\\1-\\2;\\3",
      exp_path_3
    )
  ]

  dat[
    .zone == TRUE & grepl("\\-\\d{4}[AP]M", exp_path_3),
    exp_path_3 := gsub("\\-(\\d{2})(\\d{2}[AP]M)", "-\\1;\\2", exp_path_3)
  ]

  dat[.zone == TRUE, exp_path_3 := gsub("'", ";", exp_path_3)]

  # Gaps in the time slices themselves
  dat[
    .zone == TRUE & grepl("_\\d{1,2};\\d{2}[AP]M \\-", exp_path_3),
    exp_path_3 := gsub(
      "(_\\d{1,2};\\d{2}[AP]M) \\-",
      "\\1-",
      exp_path_3
    )
  ]

  dat[
    .zone == TRUE & grepl("\\d{1,2};\\d{2} [AP]M", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2};\\d{2}) ([AP]M)",
      "\\1\\2",
      exp_path_3
    )
  ]

  dat[
    .zone == TRUE & grepl(";[AP]M", exp_path_3),
    exp_path_3 := gsub(
      ";([AP]M)",
      "\\1",
      exp_path_3
    )
  ]

  # Ad-hoc changes
  dat[
    .zone == TRUE & grepl("11;031AM", exp_path_3),
    exp_path_3 := gsub("11;031AM", "11;31AM", exp_path_3)
  ]
  dat[
    # For this one, I'm imputing 9 to cover the maximum range
    .zone == TRUE & grepl("11;4AM", exp_path_3),
    exp_path_3 := gsub("11;4AM", "11;49AM", exp_path_3)
  ]
  dat[
    .zone == TRUE & grepl("110;4AM", exp_path_3),
    exp_path_3 := gsub("110;4AM", "11;04AM", exp_path_3)
  ]
  dat[
    .zone == TRUE & grepl("012;", exp_path_3),
    exp_path_3 := gsub("012", "12", exp_path_3)
  ]

  dat[.zone == TRUE, .processed := TRUE]
  dat[, .zone := NULL]

  # ZONE 2 ------
  dat[, .zone := !grepl("\\t", exp_path_3) & grepl("\\d{2}\\s+\\d{5,6}[CM]?", exp_path_3)]

  dat[.zone == TRUE, exp_path_3 := toupper(exp_path_3)]

  # Precaution -- while all of these entries only have spaces
  # in the middle, I'd rather not accidentally mess this up
  # in case this format shows up in the future.
  dat[
    .zone == TRUE & grepl("\\s{2,}", exp_path_3),
    exp_path_3 := gsub("\\s{2,}", "\t", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("\\d{1,2}\\,\\d{2}", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2})\\,(\\d{2})",
      "\\1;\\2",
      exp_path_3
    )
  ]

  dat[
    .zone == TRUE & grepl("\\d{1,2};\\d{2}[AP]M-\\d{1,2};\\d{2}\\t", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2};\\d{2})([AP]M)-(\\d{1,2};\\d{2})\\t",
      "\\1\\2-\\3\\2\t",
      exp_path_3
    )
  ]
  dat[
    .zone == TRUE & grepl("\\d{1,2};\\d{2}[AP]M-\\d{1,2};\\d{2}$", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2};\\d{2})([AP]M)-(\\d{1,2};\\d{2})$",
      "\\1\\2-\\3\\2",
      exp_path_3
    )
  ]

  # Impute missing participant code to be mother for now
  dat[
    .zone == TRUE & grepl("\\d{5,6}_", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{5,6})_",
      "\\1M_",
      exp_path_3
    )
  ]

  # Operations were during the day
  dat[
    .zone == TRUE & grepl("1[01];\\d{2}(\\-|\\t)", exp_path_3),
    exp_path_3 := gsub("(1[01];\\d{2})(\\-|\\t)", "\\1AM\\2", exp_path_3)
  ]
  dat[
    .zone == TRUE & grepl("1[01];\\d{2}$", exp_path_3),
    exp_path_3 := gsub("(1[01];\\d{2})$", "\\1AM", exp_path_3)
  ]
  dat[
    .zone == TRUE & grepl("12;\\d{2}(\\-|\\t)", exp_path_3),
    exp_path_3 := gsub("(12;\\d{2})(\\-|\\t)", "\\1PM\\2", exp_path_3)
  ]
  dat[
    .zone == TRUE & grepl("12;\\d{2}$", exp_path_3),
    exp_path_3 := gsub("(12;\\d{2})$", "\\1PM", exp_path_3)
  ]

  dat[
    .zone == TRUE & grepl("\\d{1,2};\\d{2}[AP]M\\d{1,2};\\d{2}", exp_path_3),
    exp_path_3 := gsub(
      "(\\d{1,2};\\d{2}[AP]M)(\\d{1,2};\\d{2}[AP]M)",
      "\\1-\\2",
      exp_path_3
    )
  ]

  dat[.zone == TRUE, .processed := TRUE]
  dat[, .zone := NULL]

  # ZONE 3 ------
  dat[, .zone := grepl("^\\d{5,6}([_ ]?[CM](HILD|OTHER)?)?$", toupper(exp_path_3))]

  dat[
    .zone == TRUE & grepl("^\\d{5,6}$", exp_path_3),
    exp_path_3 := paste0(exp_path_3, "M")
  ]

  dat[
    .zone == TRUE & grepl("^\\d{5,6}[_ ][CM](HILD|OTHER)?$", toupper(exp_path_3)),
    exp_path_3 := gsub("^(\\d{5,6})[_ ]([CM]).*", "\\1\\2", exp_path_3)
  ]

  dat[.zone == TRUE, .processed := TRUE]
  dat[, .zone := NULL]

  # ZONE 4 ------
  dat[, .zone := grepl("^\\d{8}$", exp_path_3)]
  dat[, ep3_date := .zone]
  dat[.zone == TRUE, .processed := TRUE]
  dat[, .zone := NULL]

  # ZONE 5 ------
  dat[, .zone := grepl("PIDgt", exp_path_3) | grepl("SL_0", exp_path_3)]

  dat[
    .zone == TRUE & grepl(" [&$]? ", exp_path_3),
    exp_path_3 := gsub(
      " [&$]? ",
      "\t",
      exp_path_3
    )
  ]

  dat[
    .zone == TRUE & grepl("_ ?[A-Z][a-z]+", exp_path_3),
    exp_path_3 := gsub(
      "_ ?[A-Z][a-z]+",
      "",
      exp_path_3
    )
  ]

  dat[.zone == TRUE, .processed := TRUE]
  dat[, .zone := NULL]

  # ZONE 6 ------

  dat[, .zone := grepl("^\\d{5,6}\\,\\d{5,6}$", exp_path_3)]

  dat[.zone == TRUE, exp_path_3 := chartr(",", "\t", exp_path_3)]
  dat[.zone == TRUE, exp_path_3 := gsub("(\\d{5,6})", "\\1M", exp_path_3)]

  dat[.zone == TRUE, .processed := TRUE]
  dat[, .zone := NULL]

  # ZONE 7 ------
  # Ad-hoc changes

  dat[, .zone := .processed == FALSE & grepl("_Birth [Ff]ollowup", exp_path_3)]
  dat[
    .zone == TRUE,
    exp_path_3 := gsub("_Birth [Ff]ollowup", "M", exp_path_3)
  ]
  dat[.zone == TRUE, .processed := TRUE]

  dat[, .zone := .processed == FALSE & grepl("\\)\\s{3,}", exp_path_3)]
  dat[
    .zone == TRUE,
    exp_path_3 := gsub("\\s{3,}", "\t", exp_path_3)
  ]
  dat[
    .zone == TRUE,
    exp_path_3 := gsub(
      "\\([A-Za-z0-9 ]+\\)\t",
      "\t",
      exp_path_3
    )
  ]
  dat[
    .zone == TRUE,
    exp_path_3 := gsub(
      "\\([A-Za-z0-9 ]+\\)$",
      "",
      exp_path_3
    )
  ]
  dat[
    .zone == TRUE,
    exp_path_3 := gsub(
      "(\\d{5})_",
      "\\1M_",
      exp_path_3
    )
  ]

  # IDK why this is at this step, but this is the only time this got applied
  # so some regex magic above must have introduced these errors. However, I
  # do not have the time to fully diagnose.
  dat[
    grepl("\\d{1,2};\\d{2}[AP]M_\\d{1,2};\\d{2}[AP]M", toupper(exp_path_3)),
    exp_path_3 := gsub(
      ";(\\d{2}[AP]M)_(\\d{1,2});",
      ";\\1-\\2;",
      toupper(exp_path_3)
    )
  ]

  dat[.processed == FALSE, exp_path_3 := toupper(exp_path_3)]
  dat[, c(".processed", ".zone") := NULL]

  # Final adjustments: all data collection happened during the day, so fix
  # cases of "11PM" and "12AM".

  dat[
    grepl("12;\\d{2}AM", exp_path_3),
    exp_path_3 := gsub(
      "12;(\\d{2})AM",
      "12;\\1PM",
      exp_path_3
    )
  ]

  dat[
    grepl("11;\\d{2}PM", exp_path_3),
    exp_path_3 := gsub(
      "11;(\\d{2})PM",
      "11;\\1AM",
      exp_path_3
    )
  ]

  dat
}

#' Find the maximum time limits of a Bodyguard file
#'
#' Since these CSVs can be quite large, this function uses
#' DuckDB to scan only the relevant parts of the files.
#' Using data.table::fread(select) is too slow for the number
#' of files needed to be processed.
#'
#' @param path path - Path to a CSV to scan
bg_scan_file_limits <- function(path) {
  conn <- duckdb::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  possible_time_cols <- c("timestamp", "local_time")
  table_header <- tryCatch(
    DBI::dbGetQuery(
      conn,
      glue::glue("SELECT * FROM '{path}' LIMIT 1")
    ),
    error = function(e) {
      e
    }
  )

  if (rlang::is_error(table_header)) {
    return(list(
      res = list(NULL),
      err = table_header
    ))
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
    glue::glue("SELECT {time_col} FROM '{path}' ORDER BY 1 ASC LIMIT 1")
  )[[1]]

  end <- DBI::dbGetQuery(
    conn,
    glue::glue("SELECT {time_col} FROM '{path}' ORDER BY 1 DESC LIMIT 1")
  )[[1]]

  list(
    res = data.table::data.table(
      path = path,
      start = start,
      end = end
    ),
    err = list(NULL)
  )
}

bg_prepare_file_limits <- function(limits) {
  if (!is.data.frame(limits$res)) {
    return(NULL)
  }

  dat <- limits$res
  dat <- dat |>
    tidytable::mutate(mode = "local_time")

  if (!"POSIXt" %in% class(dat$start)) {
    dat <- dat |>
      tidytable::mutate(
        start = as.POSIXct(start, origin = "1970-01-01"),
        end = as.POSIXct(end, origin = "1970-01-01"),
        mode = "unix_timestamp",
      )
  }

  dat
}

bg_generate_long_table <- function(part1) {
  part1 |>
    tidytable::select(path, exp_path_3, respondent_cat) |>
    tidytable::summarise(
      component = unlist(strsplit(exp_path_3, "\\t")),
      respondent_cat = unique(respondent_cat),
      .by = path
    ) |>
    tidytable::mutate(component = chartr(";", ":", component)) |>
    tidytable::mutate(
      date = tidytable::if_else(
        grepl("^\\d{8}$", component),
        component,
        NA_character_
      ),
      mirage_id = tidytable::case_when(
        grepl("^PID", component) | grepl("^SL_", component) ~ component,
        grepl("^\\d{5}[MCB]$", component) ~ strtrim(component, 5),
        grepl("^\\d{6}[MCB]$", component) ~ strtrim(component, 6),
        grepl("^\\d{5}[MCB]_", component) ~ strtrim(component, 5),
        grepl("^\\d{6}[MCB]_", component) ~ strtrim(component, 6),
        TRUE ~ NA_character_
      ),
      participant = tidytable::case_when(
        grepl("^\\d{5,6}C", component) ~ "child",
        grepl("^\\d{5,6}M", component) ~ "mother",
        grepl("^\\d{5,6}B", component) ~ "both",
        TRUE ~ NA_character_
      ),
      window = tidytable::case_when(
        grepl("^\\d{5,6}\\w_\\d{2}:", component) ~ gsub(
          ".*(\\d{2}:\\d{2}[AP]M-\\d{1,2}:\\d{2}[AP]M).*",
          "\\1",
          component
        ),
        grepl("^\\d{5,6}\\w_\\d{1}:", component) ~ gsub(
          ".*(\\d{1}:\\d{2}[AP]M-\\d{1,2}:\\d{2}[AP]M).*",
          "\\1",
          component
        ),
        TRUE ~ NA_character_
      ),
      start = tidytable::if_else(
        !is.na(window),
        vcapply(window, \(s) unlist(strsplit(s, "\\-"))[1]),
        NA_character_
      ),
      end = tidytable::if_else(
        !is.na(window),
        vcapply(window, \(s) unlist(strsplit(s, "\\-"))[2]),
        NA_character_
      ),
    )
}
