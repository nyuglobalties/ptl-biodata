with_transaction <- function(conn, f, ...) {
  DBI::dbExecute(conn, "BEGIN TRANSACTION;")

  res <- tryCatch(f(conn, ...), error = function(e) {
    DBI::dbExecute(conn, "ROLLBACK;")
    e
  })

  if (rlang::is_error(res)) {
    stop0(res)
  }

  DBI::dbExecute(conn, "COMMIT;")
  res
}

ddb_check_extension <- function(conn, extension) {
  res <- DBI::dbGetQuery(
    conn,
    glue::glue("
      SELECT extension_name, loaded
      FROM duckdb_extensions()
      WHERE installed;
    ")
  )

  if (!extension %in% res$extension_name) {
    DBI::dbExecute(conn, glue::glue("INSTALL {extension};"))
  }

  if (nrow(res[res$loaded & res$extension_name == extension, ]) < 1) {
    DBI::dbExecute(conn, glue::glue("LOAD {extension};"))
  }

  invisible(conn)
}

ddb_connect <- function(dbdir = ":memory:",
                        read_only = TRUE,
                        extensions = NULL) {
  conn <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = dbdir,
    read_only = read_only
  )

  if (!is.null(extensions)) {
    for (ext in extensions) {
      ddb_check_extension(conn, ext)
    }
  }

  conn
}

ddb_check_icu <- function(conn) {
  ddb_check_extension(conn, "icu")
}

ddb_check_sqlite <- function(conn) {
  ddb_check_extension(conn, "sqlite")
}
