bg_ecg_session_meta <- function(bg_files, bg_root, sample_size = NULL, seed = 0xABBA) {
  dir_paths <- dirname(bg_files)
  dir_ids <- bg_dir_id(bg_files)

  bg_root_abs <- box_path(bg_root)
  explode_rel_path <- \(x) explode_path(gsub(bg_root_abs, "", x, fixed = TRUE))
  exploded_rel_path <- lapply(dir_paths, explode_rel_path)
  respondent_cat <- viapply(exploded_rel_path, \(ex) {
    ex[1] |>
      gsub("^([0-9_]+)\\..*", "\\1", x = _) |>
      gsub("_", "", x = _) |>
      as.integer()
  })

  out <- tidytable::tidytable(
    id = dir_ids,
    path = dir_paths,
    respondent_cat = respondent_cat
  )

  if (!is.null(sample_size)) {
    rows <- withr::with_seed(seed = seed, {
      sample(seq_len(nrow(out)), sample_size)
    })

    out <- out[rows]
  }

  unique(out)
}

bg_ecg_session_windows <- function(sessions, bg_root) {
  dir_ids <- sessions$id
  dir_paths <- sessions$path
  bg_root_abs <- box_path(bg_root)

  explode_rel_path <- \(x) explode_path(gsub(bg_root_abs, "", x, fixed = TRUE))
  exploded_rel_path <- lapply(dir_paths, explode_rel_path)

  dat <- tidytable::tidytable(
    id_session = dir_ids,
    exploded_rel_path = exploded_rel_path
  ) |>
    tidytable::unnest(exploded_rel_path) |>
    tidytable::select(id_session, value = exploded_rel_path) |>
    tidytable::mutate(
      attribute = tidytable::case_when(
        grepl("^\\d{8}$", value) ~ "date_str",
        grepl("^\\d{5,6}[MC]?$", toupper(value)) ~ "participant_str",
        grepl("^[0-9_]+\\. ", value) ~ "respondent_cat",
        value %in% c("Birth_follow_up_mother", "Mother") ~ "participant_type",
        value %in% c("Birth_follow_up_child", "Child") ~ "participant_type",
        TRUE ~ NA_character_
      ),
      .before = value
    ) |>
    tidytable::mutate(id = 1:.N) |>
    tidytable::relocate(id)

  known_attrs <- tidytable::filter(dat, !is.na(attribute))

  unknown_attrs <- dat |>
    tidytable::filter(is.na(attribute)) |>
    tidytable::mutate(
      value = toupper(value),
      to_split_1 = grepl("\\d{1,2} ?([AP]M)?\\s+,?\\d{5,6}[CM]?[CM]? ?_", value),
      to_split_2 = grepl("\\d{2} ?([AP]M)?\\([A-Z0-9 ]+\\)\\s+\\d{5,6}[CM]?[CM]?_", value),
      to_split_3 = grepl("^\\d{5},\\d{5}$", value),
      to_split_4 = grepl("^PID[A-Z0-9]+  PID[A-Z0-9]+$", value),
      to_split_5 = grepl("\\d{1,2} ?([AP]M)?\\s+,?\\d{5,6}[CM]?[CM]? ?-", value),
      to_split = to_split_1 | to_split_2 | to_split_3 | to_split_4 | to_split_5,
    ) |>
    tidytable::mutate(
      value = tidytable::if_else(
        to_split_1,
        gsub(
          "(\\d{1,2}) ?([AP]M)?\\s+,?(\\d{5,6}[CM]?[CM]?) ?(_)",
          "\\1\\2\t\\3\\4",
          value
        ),
        value
      ),
      # Annoying parenthetical statements that *hopefully* don't
      # require writing a parser
      value = tidytable::if_else(
        to_split_2,
        gsub(
          "(\\d{2}) ?([AP]M)?\\(([A-Z0-9 ]+)\\)\\s+(\\d{5,6}[CM]?[CM]?_)",
          "\\1\\2(\\3)\t\\4",
          value
        ),
        value
      ),
      value = tidytable::if_else(
        to_split_3,
        chartr(",", "\t", value),
        value
      ),
      value = tidytable::if_else(
        to_split_4, gsub("\\s+", "\t", value), value
      ),
      value = tidytable::if_else(
        to_split_5,
        gsub(
          "(\\d{1,2}) ?([AP]M)?\\s+,?(\\d{5,6}[CM]?[CM]?) ?(-)",
          "\\1\\2\t\\3_",
          value
        ),
        value
      ),
      to_split_1 = NULL,
      to_split_2 = NULL,
      to_split_3 = NULL,
      to_split_4 = NULL,
      to_split_5 = NULL,
    )

  bad_unknown_attrs <- !is.na(unknown_attrs$value) &
    grepl("\\s{2,}", unknown_attrs$value) &
    !grepl("\\t", unknown_attrs$value)

  if (any(bad_unknown_attrs)) {
    if (interactive()) {
      browser()
    } else {
      stop0("Unhandled multi-session label!")
    }
  }

  windows <- unknown_attrs |>
    tidytable::filter(to_split == TRUE) |>
    tidytable::select(id, value) |>
    tidytable::summarize(
      value = strsplit(value, "\\t"),
      .by = id
    ) |>
    tidytable::transmute(
      data = lapply(value, \(w_strs) data.frame(value = w_strs, id_win = seq_along(w_strs))),
      .by = id
    ) |>
    tidytable::unnest(data) |>
    tidytable::bind_rows(
      unknown_attrs |>
        tidytable::filter(to_split == FALSE) |>
        tidytable::select(id, value) |>
        tidytable::mutate(id_win = 1L)
    ) |>
    bg_fix_window_strs() |>
    bg_parse_window_attrs()

  combined <- unknown_attrs |>
    tidytable::select(id, id_session) |>
    tidytable::right_join(windows, by = "id") |>
    tidytable::bind_rows(known_attrs) |>
    tidytable::mutate(
      id_win = tidytable::replace_na(id_win, 1),
      priority = tidytable::replace_na(priority, 1000),
    ) |>
    tidytable::arrange(id_session, id_win, priority) |>
    bg_window_repair_rescat() |>
    bg_window_repair_datestr() |>
    bg_window_repair_participant_str() |>
    bg_window_repair_participant_type() |>
    # Keep lowest priority per attribute
    # Window strings have lower priority to address
    # finer granularity in detail.
    tidytable::mutate(
      value = do.call(tidytable::coalesce, as.list(value)),
      priority = seq_len(.N),
      .by = c("id_session", "id_win", "attribute")
    ) |>
    tidytable::filter(priority == 1) |>
    tidytable::select(-id, -priority)

  out <- combined |>
    tidytable::pivot_wider(
      names_from = "attribute",
      id_cols = c("id_session", "id_win")
    ) |>
    tidytable::arrange(id_session, id_win) |>
    tidytable::mutate(
      respondent_cat = do.call(tidytable::coalesce, as.list(respondent_cat)),
      year = do.call(tidytable::coalesce, as.list(year)),
      month = do.call(tidytable::coalesce, as.list(month)),
      day = do.call(tidytable::coalesce, as.list(day)),
      .by = id_session
    ) |>
    tidytable::mutate(
      respondent_cat = as.integer(respondent_cat),
      year = as.integer(year),
      month = as.integer(month),
      day = as.integer(day),
    ) |>
    tidytable::filter(!is.na(participant)) |>
    tidytable::mutate(id = seq_len(.N))

  out
}

bg_filter_recordings <- function(recordings, sessions, run_all = FALSE) {
  out <- recordings
  dir_ids <- bg_dir_id(out)

  if (!isTRUE(run_all)) {
    out <- out[dir_ids %in% sessions$id]
  }

  out
}

bg_dir_id <- function(path) {
  if (length(path) > 1) {
    return(vcapply(path, bg_dir_id))
  }

  digest::digest(dirname(path), algo = "xxhash64")
}

bg_fix_window_strs <- function(windows) {
  window_pattern <- "^1?\\d{5}[CMB][CM]?(_\\d{1,2};\\d{2}[AP]M\\-\\d{1,2};\\d{2}[AP]M(\\([A-Z0-9 ]+\\))?)?$"

  out <- windows |>
    tidytable::mutate(
      value = chartr(".", ";", value),
      value = chartr(",", ";", value),
      value = chartr("'", ";", value),
      value = tidytable::case_when(
        is.na(value) ~ value,
        grepl("\\d{1,2}'\\d{2}[AP]M", value) ~ chartr("'", ";", value),
        grepl("^\\d{5,6}_", value) ~ gsub("^(\\d{5,6})_", "\\1M_", value),
        grepl("\\d{2}[AP]M;\\d{1,2};\\d{2}[AP]M", value) ~ gsub(
          "(\\d{2}[AP]M);(\\d{1,2};\\d{2}[AP]M)", "\\1-\\2", value
        ),
        grepl("^\\d{5}$", value) ~ paste0(value, "M"),
        grepl("^SL_", value) ~ NA_character_,
        grepl("^PIDGT", value) ~ NA_character_,
        TRUE ~ value
      ),
      # Delimiter fixes
      value = tidytable::case_when(
        grepl("-\\d{4}[AP]M", value) ~ gsub("(-\\d{2})(\\d{2}[AP]M)", "\\1;\\2", value),
        grepl("_\\d{1,2};\\d{2}[AP]M;\\d{1,2};\\d{2}[AP]M$", value) ~ gsub(
          "(_\\d{1,2};\\d{2}[AP]M);(\\d{1,2};\\d{2}[AP]M)$", "\\1-\\2", value
        ),
        grepl("\\d{2};[AP]M", value) ~ gsub(
          "(\\d{2});([AP]M)", "\\1\\2", value
        ),
        grepl("[AP]M_\\d{1,2};", value) ~ gsub(
          "([AP]M)_(\\d{1,2};)", "\\1-\\2", value
        ),
        grepl("-\\d{1,2}-\\d{2}[AP]M", value) ~ gsub("(-\\d{1,2})-(\\d{2}[AP]M)", "\\1;\\2", value),
        grepl("^\\d{5,6}[CM]\\d{1,2};\\d{2}[AP]M", value) ~ gsub(
          "^(\\d{5,6}[MC])(\\d{1,2};\\d{2}[AP]M)", "\\1_\\2", value
        ),
        grepl("\\d{1,2};\\d{2}[AP]M\\d{1,2};\\d{2}", value) ~ gsub(
          "(\\d{1,2};\\d{2}[AP]M)(\\d{1,2};\\d{2})", "\\1-\\2", value
        ),
        TRUE ~ value
      ),
      # Meridian fixes
      value = tidytable::case_when(
        grepl("_0?\\d;\\d{2}-0?\\d;\\d{2}PM", value) ~ gsub(
          "_(0?\\d;\\d{2})(-0?\\d;\\d{2}PM)", "_\\1PM\\2", value
        ),
        grepl("_0?\\d;\\d{2}PM-0?\\d;\\d{2}\\s*$", value) ~ gsub(
          "_(0?\\d;\\d{2}PM)(-0?\\d;\\d{2})", "_\\1\\2PM", value
        ),
        grepl("_1[01];\\d{2}-1[01];\\d{2}AM", value) ~ gsub(
          "_(1[01];\\d{2})(-1[01];\\d{2}AM)", "_\\1AM\\2", value
        ),
        grepl("_1[01];\\d{2}AM-1[01];\\d{2}\\s*$", value) ~ gsub(
          "_(1[01];\\d{2}AM)(-1[01];\\d{2})", "_\\1\\2AM", value
        ),
        TRUE ~ value
      ),
      # Range fixes
      value = tidytable::case_when(
        grepl("_\\d{1,2};\\d{2}[AP]M\\d{1,2};\\d{2}[AP]M", value) ~ gsub(
          "_(\\d{1,2};\\d{2}[AP]M)(\\d{1,2};\\d{2}[AP]M)", "_\\1-\\2", value
        ),
        grepl("_\\d{1,2};\\d{2}-[AP]M\\d{1,2};\\d{2}[AP]M", value) ~ gsub(
          "_(\\d{1,2};\\d{2})-([AP]M)(\\d{1,2};\\d{2}[AP]M)", "_\\1\\2-\\3", value
        ),
        TRUE ~ value
      ),
      # Naked ranges
      value = tidytable::case_when(
        grepl("1[01];\\d{2}-1[01];\\d{2}$", value) ~ gsub(
          "(1[01];\\d{2})(-1[01];\\d{2})", "\\1AM\\2AM", value
        ),
        grepl("12;\\d{2}-12;\\d{2}$", value) ~ gsub(
          "(12;\\d{2})(-12;\\d{2})", "\\1PM\\2PM", value
        ),
        TRUE ~ value
      ),
      # Midday
      value = tidytable::case_when(
        grepl("12;\\d{2}PM-(12|0?1);\\d{2}$", value) ~ gsub(
          "(12;\\d{2}PM-(?:12|0?1);\\d{2})$", "\\1PM", value
        ),
        grepl("12;\\d{2}-(12|0?1);\\d{2}PM\\s*$", value) ~ gsub(
          "(12;\\d{2})(-(?:12|0?1);\\d{2}PM)\\s*$", "\\1PM\\2", value
        ),
        grepl("11;\\d{2}AM-11;\\d{2}$", value) ~ gsub(
          "(11;\\d{2}AM-11;\\d{2})$", "\\1AM", value
        ),
        grepl("11;\\d{2}-11;\\d{2}AM$", value) ~ gsub(
          "(11;\\d{2})(-11;\\d{2}AM)$", "\\1AM\\2", value
        ),
        grepl("11;\\d{2}AM-12;\\d{2}$", value) ~ gsub(
          "(11;\\d{2}AM-12;\\d{2})$", "\\1PM", value
        ),
        grepl("11;\\d{2}-12;\\d{2}PM$", value) ~ gsub(
          "(11;\\d{2})(-12;\\d{2}PM)$", "\\1AM\\2", value
        ),
        TRUE ~ value
      ),
      # Errant spaces
      value = tidytable::case_when(
        grepl("^\\d{5,6}[MC] _", value) ~ gsub("^(\\d{5,6}[MC]) _", "\\1_", value),
        grepl("^\\d{5,6}[MC]_ ", value) ~ gsub("^(\\d{5,6}[MC])_ ", "\\1_", value),
        grepl("[-_]\\d{1,2};\\d{2} [AP]M", value) ~ gsub(
          "([-_]\\d{1,2};\\d{2}) ([AP]M)", "\\1\\2", value
        ),
        grepl("[AP]M -\\d{2};\\d{2}", value) ~ gsub(
          "([AP]M) (-\\d{2};\\d{2})", "\\1\\2", value
        ),
        TRUE ~ value
      ),
      # Ad-hoc
      value = tidytable::case_when(
        grepl("^\\d{5,6}M_.*\\s+CHILD$", value) ~ gsub(
          "^(\\d{5,6})M([^ ]*)\\s+CHILD$", "\\1C\\2", value
        ),
        grepl("^\\d{5}M_C(HILD)?$", value) ~ gsub("^(\\d{5})M_C(HILD)?", "\\1C", value),
        grepl("^\\d{5}M_M$", value) ~ substr(value, 1, 6),
        grepl("^\\d{5}M_BIRTH FOLLOWUP$", value) ~ substr(value, 1, 6),
        grepl("PMP", value) ~ gsub("PMP", "PM", value),
        # No 6-digit PIDs start with 2-9
        grepl("^[2-9]\\d{3}00[CM]", value) ~ gsub("^([2-9]\\d{3})0(0[CM])", "\\1\\2", value),
        grepl("-012", value) ~ gsub("-012", "-12", value),
        grepl(";031", value) ~ gsub(";031", ";31", value),
        grepl("110;4AM", value) ~ gsub("110;4AM", "11;04AM", value),
        # Estimating these windows based on maximum possible width but hopefully
        # Mirage will narrow it down
        grepl("03;0PM", value) ~ gsub("03;0PM", "03;09PM", value),
        grepl("11;4AM", value) ~ gsub("11;4AM", "11;49PM", value),
        grepl("^\\d{5}M_.*\\(CHILD\\)$", value) ~ gsub("^(\\d{5})M(_.*)\\(CHILD\\)", "\\1C\\2", value),
        grepl("-110;", value) ~ gsub("-110;", "-11;0", value),
        grepl("_114;", value) ~ gsub("_114;", "_11;4", value),
        grepl("11;\\d{2}PAM", value) ~ gsub("11;(\\d{2})PAM", "11;\\1AM", value),
        grepl("\\s+TWIN BABY$", value) ~ gsub("\\s+TWIN BABY$", "", value),
        grepl("\\(TWIN BABY\\)$", value) ~ gsub("\\(TWIN BABY\\)$", "", value),
        grepl("\\(PREAGNANT WOMAN\\)$", value) ~ gsub("\\(PREAGNANT WOMAN\\)$", "", value),
        grepl("^\\d{5} MOTHER", value) ~ paste0(substr(value, 1, 5), "M"),
        grepl(" MOTHER$", value) ~ gsub("\\s+MOTHER$", "", value), # Cleanup
        TRUE ~ value
      ),
      # Repeats for corner cases
      value = tidytable::case_when(
        grepl("12;\\d{2}PM-12;\\d{2}$", value) ~ gsub(
          "(12;\\d{2}PM-12;\\d{2})$", "\\1PM", value
        ),
        grepl("12;\\d{2}-12;\\d{2}PM\\s*$", value) ~ gsub(
          "(12;\\d{2})(-12;\\d{2}PM)\\s*$", "\\1PM\\2", value
        ),
        grepl("_0?\\d;\\d{2}-0?\\d;\\d{2}PM", value) ~ gsub(
          "_(0?\\d;\\d{2})(-0?\\d;\\d{2}PM)", "_\\1PM\\2", value
        ),
        grepl("_0?\\d;\\d{2}PM-0?\\d;\\d{2}\\s*$", value) ~ gsub(
          "_(0?\\d;\\d{2}PM)(-0?\\d;\\d{2})", "_\\1\\2PM", value
        ),
        TRUE ~ value
      ),
    )

  if (any(!is.na(out$value) & !grepl(window_pattern, out$value))) {
    if (interactive()) {
      browser()
    } else {
      stop0("Unhandled broken window pattern!")
    }
  }

  out
}

bg_parse_window_attrs <- function(windows) {
  initial <- windows |>
    tidytable::mutate(
      participant = gsub("^(\\d{5,6}).*$", "\\1", value),
      participant_type = gsub("^\\d{5,6}([MC]).*$", "\\1", value),
      start = tidytable::if_else(
        grepl("_\\d{1,2};\\d{2}[AP]M-\\d{1,2};\\d{2}[AP]M", value),
        chartr(";", ":", gsub("^.*_(\\d{1,2};\\d{2}[AP]M)-.*$", "\\1", value)),
        NA_character_
      ),
      end = tidytable::if_else(
        grepl("_\\d{1,2};\\d{2}[AP]M-\\d{1,2};\\d{2}[AP]M", value),
        chartr(";", ":", gsub("^.*-(\\d{1,2};\\d{2}[AP]M)(?:\\(.*\\))?$", "\\1", value)),
        NA_character_
      ),
      respondent_cat = tidytable::case_when(
        grepl("\\(6 MONTH.*$", value) ~ 7L,
        grepl("\\(28 DAYS.*$", value) ~ 6L,
        grepl("\\(3RD", value) ~ 4L,
        TRUE ~ NA_integer_,
      )
    )

  if (any(!is.na(initial$value) & grepl("\\(.*$", initial$value) & is.na(initial$respondent_cat))) {
    if (interactive()) {
      browser()
    } else {
      stop0("Unhandled custom respondent_cat parenthetical statement!")
    }
  }

  if (any(!is.na(initial$start) & !grepl("\\d{1,2}:\\d{2}[AP]M", initial$start))) {
    if (interactive()) {
      browser()
    } else {
      stop0("Incorrectly formatted window start timestamp")
    }
  }

  if (any(!is.na(initial$end) & !grepl("\\d{1,2}:\\d{2}[AP]M", initial$end))) {
    if (interactive()) {
      browser()
    } else {
      stop0("Incorrectly formatted window end timestamp")
    }
  }

  cols <- setdiff(names(initial), c("id", "id_win", "value"))

  out <- initial |>
    tidytable::select(-value) |>
    tidytable::pivot_longer(
      cols = tidyselect::all_of(cols),
      names_to = "attribute",
      values_to = "value"
    ) |>
    tidytable::filter(!(attribute == "respondent_cat" & is.na(value))) |>
    tidytable::mutate(
      priority = tidytable::if_else(
        attribute == "respondent_cat",
        0,
        100
      )
    )

  out
}

bg_window_repair_rescat <- function(windows) {
  rescats <- windows |>
    tidytable::filter(attribute == "respondent_cat") |>
    tidytable::mutate(
      value = tidytable::if_else(
        grepl("^[0-9_]\\..*$", value),
        value |>
          gsub("^([0-9_])\\..*", "\\1", x = _) |>
          gsub("_", "", x = _),
        value
      )
    )

  windows |>
    tidytable::filter(attribute != "respondent_cat") |>
    tidytable::bind_rows(rescats)
}

bg_window_repair_datestr <- function(windows) {
  date_strs <- windows |>
    tidytable::filter(attribute == "date_str") |>
    tidytable::mutate(
      date = lubridate::ymd(value),
      year = lubridate::year(date),
      month = lubridate::month(date),
      day = lubridate::day(date),
      date = NULL,
    ) |>
    tidytable::select(id, id_session, id_win, priority, year, month, day)

  cols <- setdiff(names(date_strs), c("id", "id_session", "id_win", "priority"))

  attr_table <- date_strs |>
    tidytable::pivot_longer(
      cols = tidyselect::all_of(cols),
      names_to = "attribute",
      values_to = "value"
    )

  windows |>
    tidytable::filter(attribute != "date_str") |>
    tidytable::bind_rows(attr_table)
}

bg_window_repair_participant_str <- function(windows) { # nolint
  part_strs <- windows |>
    tidytable::filter(attribute == "participant_str") |>
    tidytable::mutate(
      participant = gsub("^(\\d{5,6}).*$", "\\1", value),
      participant_type_match = grepl("[CM][CM]?$", value),
      participant_type = tidytable::if_else(
        participant_type_match,
        gsub("^.*([CM][CM]?)$", "\\1", value),
        "ignore"
      ),
      participant_type = tidytable::if_else(
        participant_type %in% c("MC", "CM"),
        "B",
        participant_type
      ),
      participant_type_match = NULL,
    ) |>
    tidytable::select(
      id, id_session, id_win,
      priority, participant, participant_type
    )

  cols <- setdiff(names(part_strs), c("id", "id_session", "id_win", "priority"))

  windows |>
    tidytable::filter(attribute != "participant_str") |>
    tidytable::bind_rows(
      part_strs |>
        tidytable::pivot_longer(
          cols = tidyselect::all_of(cols),
          names_to = "attribute",
          values_to = "value"
        ) |>
        tidytable::filter(value != "ignore")
    )
}

bg_window_repair_participant_type <- function(windows) { # nolint
  windows |>
    tidytable::filter(attribute != "participant_type") |>
    tidytable::bind_rows(
      windows |>
        tidytable::filter(attribute == "participant_type") |>
        tidytable::mutate(
          value = tidytable::case_when(
            is.na(value) ~ value,
            grepl("child", tolower(value)) ~ "C",
            grepl("mother", tolower(value)) ~ "M",
            TRUE ~ value
          )
        )
    )
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
