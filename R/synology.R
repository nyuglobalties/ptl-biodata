synology_path <- function(..., .root = SYNOLOGY_ROOT) {
  if (is.null(.root) || !dir.exists(SYNOLOGY_ROOT)) {
    stop0("Define where Synology is located with `Sys.setenv(SYNOLOGY_ROOT = '<path>')`")
  }

  file.path(.root, ...)
}

is_synology_available <- function(...) {
  tryCatch(
    is.character(synology_path(...)),
    error = function(err) {
      FALSE
    }
  )
}

prefer_synology <- function(syn_obj, box_obj, .avail = is_synology_available()) {
  if (isTRUE(.avail)) {
    syn_obj
  } else {
    box_obj
  }
}

smb_list_files <- function(path, dump_file = NULL, ...) {
  assert_string(path)

  smb_list_files_(path = path, dump_file = dump_file, ...)
}

smb_list_files_ <- function(path,
                            pattern = NULL,
                            all_files = TRUE,
                            full_names = TRUE,
                            recursive = FALSE,
                            max_depth = NULL,
                            dump_file = NULL) {
  message(path)

  # Because list.files' handling of dirs with SMB is weird, but
  # fstat works just fine
  is_dir <- function(x) {
    isTRUE(file.info(x)[["isdir"]])
  }

  if (!is_dir(path)) {
    return(path)
  }

  tmp_all_files <- list.files(path, pattern = pattern, all.files = all_files, full.names = full_names)
  dir_files <- discard(tmp_all_files, is_dir)
  dir_dirs <- keep(tmp_all_files, is_dir)
  dir_dirs <- dir_dirs[!basename(dir_dirs) %in% c(".", "..")]

  if (!isTRUE(recursive)) {
    return(c(dir_dirs, dir_files))
  } else {
    out <- dir_files

    if (!is.null(dump_file)) {
      tmpf <- tempfile()
      writeLines(out, tmpf)

      if (!file.exists(dump_file)) {
        file.rename(tmpf, dump_file)
      } else {
        file.append(dump_file, tmpf)
      }
    }

    if (length(dir_dirs) > 0) {
      for (dd in dir_dirs) {
        if (is.null(max_depth) || max_depth > 0) {
          out <- c(
            out,
            smb_list_files_(
              dd,
              pattern = pattern,
              all_files = all_files,
              full_names = full_names,
              recursive = recursive,
              max_depth = if (!is.null(max_depth)) max_depth - 1 else max_depth,
              dump_file = dump_file
            )
          )
        }
      }
    }

    out
  }
}
