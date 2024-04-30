map <- function(val_type = list()) {
  env <- new.env()

  if (!is.character(val_type)) {
    env$.val_type <- class(val_type)
  } else {
    env$.val_type <- val_type
  }

  structure(
    env,
    class = "map"
  )
}

.map_assert_key_type <- function(m, key) {
  stopifnot(is.character(key) && length(key) == 1)
}

.map_assert_val_type <- function(m, val) {
  stopifnot(
    identical(class(val), m$.val_type)
  )
}

put <- function(m, key, val) {
  .map_assert_key_type(m, key)
  .map_assert_val_type(m, val)

  m[[key]] <- val

  invisible(m)
}

has <- function(m, key) {
  .map_assert_key_type(m, key)

  exists(key, envir = m)
}

getval <- function(m, key) {
  .map_assert_key_type(m, key)

  if (!has(m, key)) {
    stop0("<map> does not have '", key, "'")
  }

  get(key, envir = m)
}

getval_opt <- function(m, key) {
  .map_assert_key_type(m, key)

  if (!has(m, key)) {
    return(NULL)
  }

  get(key, envir = m)
}
