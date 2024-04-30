is_empty <- function(ty, ...) {
  UseMethod("is_empty", ty)
}

is_empty.default <- function(ty, ...) {
  length(ty) == 0
}
