box_root <- function(root = NULL) {
  root <- root %||% find_box_path()

  if (!is.null(root) && !dir.exists(root)) {
    # In case the dir is a symlink?
    root <- NULL
  }

  root
}

box_path <- function(..., .box_root = NULL) {
  box_true_root <- box_root(root = .box_root)

  if (is.null(box_true_root)) {
    stop0("Cannot find Box root. Please define it with .box_root")
  }

  file.path(box_true_root, ...)
}

find_box_path <- function() {
  if (identical(.Platform$OS.type, "windows")) {
    home <- normalizePath(file.path("~", ".."), winslash = "/")
  } else {
    home <- normalizePath("~")
  }

  box_drive <- file.path(home, "Box")
  box_sync <- file.path(home, "Box Sync")
  box_drive2 <- file.path(home, "Library", "CloudStorage", "Box-Box")

  possible_paths <- c(box_drive, box_sync, box_drive2)
  existing_paths <- dir.exists(possible_paths)
  path <- NULL

  if (any(existing_paths)) {
    path <- possible_paths[existing_paths]

    # Box should prevent multiple install locations, but
    # in case there are conflicts, return the first. In
    # other words, this prefers Box Drive (symlink), then Box Sync,
    # then Box Drive (cloud storage).
    if (length(path) > 1) {
      path <- path[1]
    }
  }

  path
}
