synology_path <- function(..., .root = SYNOLOGY_ROOT) {
  if (is.null(.root) || !dir.exists(SYNOLOGY_ROOT)) {
    stop0("Define where Synology is located with `Sys.setenv(SYNOLOGY_ROOT = '<path>')`")
  }

  file.path(.root, ...)
}
