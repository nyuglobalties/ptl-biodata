tar_file_hash <- function(tarsym, file, ...) {
  tarname <- as.character(substitute(tarsym))

  targets::tar_target_raw(
    tarname,
    bquote(
      list(
        file = .(substitute(file)),
        hash = digest::digest(file = .(substitute(file)))
      )
    ),
    ...
  )
}
